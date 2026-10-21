use reed_solomon_erasure::galois_8::ReedSolomon;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

const DATAGRAM_SIZE: usize = 1200;
const HEADER_SIZE: usize = 15;
const LENGTH_SIZE: usize = 2;
pub const KCP_MTU: usize = DATAGRAM_SIZE - HEADER_SIZE - LENGTH_SIZE;

const DATA_SHARDS: usize = 10;
const MAX_PARITY_SHARDS: usize = 5;
const MAGIC: [u8; 4] = *b"FEC2";
const PUSH: u8 = 81;
const ACK: u8 = 82;
const GROUP_TTL: Duration = Duration::from_secs(30);
const MAX_GROUPS: usize = 64;
const RAW_DUPLICATES: usize = 16;
const LOSS_SAMPLE: Duration = Duration::from_secs(1);
const MIN_SAMPLE_PACKETS: u64 = 64;
const MIN_LOSS_RATE: f64 = 0.005;
const TARGET_GROUP_FAILURE: f64 = 0.005;

struct Shard<'a> {
    conv: u32,
    group: u32,
    index: usize,
    data_shards: usize,
    parity_shards: usize,
    payload: &'a [u8],
}

fn shard(packet: &[u8]) -> Option<Shard<'_>> {
    if !(HEADER_SIZE + 1..=DATAGRAM_SIZE).contains(&packet.len()) || packet[4..8] != MAGIC {
        return None;
    }
    let index = packet[12] as usize;
    let data_shards = packet[13] as usize;
    let parity_shards = packet[14] as usize;
    if data_shards != DATA_SHARDS
        || !(1..=MAX_PARITY_SHARDS).contains(&parity_shards)
        || index >= data_shards + parity_shards
    {
        return None;
    }
    Some(Shard {
        conv: u32::from_le_bytes(packet[0..4].try_into().unwrap()),
        group: u32::from_le_bytes(packet[8..12].try_into().unwrap()),
        index,
        data_shards,
        parity_shards,
        payload: &packet[HEADER_SIZE..],
    })
}

fn data(shard: &[u8]) -> Option<&[u8]> {
    if shard.len() < LENGTH_SIZE {
        return None;
    }
    let length = u16::from_le_bytes(shard[..LENGTH_SIZE].try_into().unwrap()) as usize;
    if !(kcp::KCP_OVERHEAD as usize..=KCP_MTU).contains(&length) || LENGTH_SIZE + length > shard.len() {
        return None;
    }
    Some(&shard[LENGTH_SIZE..LENGTH_SIZE + length])
}

pub fn session_packet(packet: &[u8]) -> Option<(u32, bool)> {
    if packet.len() > DATAGRAM_SIZE {
        return None;
    }
    if packet.get(4..8) == Some(MAGIC.as_slice()) {
        let shard = shard(packet)?;
        let initial = shard.index < shard.data_shards
            && data(shard.payload).is_some_and(|packet| packet[4] == PUSH);
        return Some((shard.conv, initial));
    }
    if !(kcp::KCP_OVERHEAD as usize..=KCP_MTU).contains(&packet.len()) {
        return None;
    }
    Some((u32::from_le_bytes(packet[..4].try_into().unwrap()), packet[4] == PUSH))
}

pub fn ack_rtt(packet: &[u8], current: u32) -> Option<u32> {
    let mut offset = 0;
    let mut sample = None;
    while packet.len().saturating_sub(offset) >= kcp::KCP_OVERHEAD as usize {
        let command = packet[offset + 4];
        let timestamp = u32::from_le_bytes(packet[offset + 8..offset + 12].try_into().unwrap());
        let length = u32::from_le_bytes(packet[offset + 20..offset + 24].try_into().unwrap()) as usize;
        let next = offset.checked_add(kcp::KCP_OVERHEAD as usize + length)?;
        if next > packet.len() {
            return None;
        }
        if command == ACK {
            let elapsed = current.wrapping_sub(timestamp);
            if elapsed <= 60_000 {
                sample = Some(sample.map_or(elapsed, |previous: u32| previous.min(elapsed)));
            }
        }
        offset = next;
    }
    (offset == packet.len()).then_some(sample).flatten()
}

pub struct Encoder {
    conv: u32,
    group: u32,
    shards: Vec<Vec<u8>>,
    raw_duplicates: usize,
    acknowledged: Arc<AtomicBool>,
    profile: Arc<AtomicU8>,
    active_parity: usize,
    newest_sn: Option<u32>,
    sampling: bool,
    sample_started: Instant,
    sample_new: u64,
    sample_retransmitted: u64,
    codes: HashMap<usize, ReedSolomon>,
}

impl Encoder {
    pub fn new(conv: u32, acknowledged: Arc<AtomicBool>, profile: Arc<AtomicU8>) -> Self {
        Self {
            conv,
            group: 0,
            shards: Vec::with_capacity(DATA_SHARDS + MAX_PARITY_SHARDS),
            raw_duplicates: RAW_DUPLICATES,
            acknowledged,
            profile,
            active_parity: 0,
            newest_sn: None,
            sampling: false,
            sample_started: Instant::now(),
            sample_new: 0,
            sample_retransmitted: 0,
            codes: HashMap::new(),
        }
    }

    pub fn send(
        &mut self,
        packet: &[u8],
        mut output: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        if packet.len() > KCP_MTU {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "KCP datagram exceeds FEC MTU"));
        }

        let now = Instant::now();
        let ready = self.acknowledged.load(Ordering::Relaxed);
        if ready && !self.sampling {
            self.sampling = true;
            self.sample_started = now;
            self.sample_new = 0;
            self.sample_retransmitted = 0;
        }
        let mut offset = 0;
        while packet.len().saturating_sub(offset) >= kcp::KCP_OVERHEAD as usize {
            let command = packet[offset + 4];
            let length = u32::from_le_bytes(packet[offset + 20..offset + 24].try_into().unwrap()) as usize;
            let next = offset.checked_add(kcp::KCP_OVERHEAD as usize + length)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid KCP datagram"))?;
            if next > packet.len() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid KCP datagram"));
            }
            if command == PUSH {
                let sn = u32::from_le_bytes(packet[offset + 12..offset + 16].try_into().unwrap());
                let new = self.newest_sn.is_none_or(|previous| sn.wrapping_sub(previous) as i32 > 0);
                if new {
                    self.newest_sn = Some(sn);
                    if self.sampling {
                        self.sample_new += 1;
                    }
                } else if self.sampling {
                    self.sample_retransmitted += 1;
                }
            }
            offset = next;
        }
        if offset != packet.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid KCP datagram"));
        }

        if self.sampling
            && self.sample_started.elapsed() >= LOSS_SAMPLE
            && self.sample_new >= MIN_SAMPLE_PACKETS
        {
            let total = self.sample_new + self.sample_retransmitted;
            let loss = self.sample_retransmitted as f64 / total as f64;
            let current = self.profile.load(Ordering::Relaxed) as usize;
            let mut desired = 0;
            if loss >= MIN_LOSS_RATE {
                desired = MAX_PARITY_SHARDS;
                for parity in 1..=MAX_PARITY_SHARDS {
                    let total_shards = DATA_SHARDS + parity;
                    let mut probability = (1.0 - loss).powi(total_shards as i32);
                    let mut recoverable = probability;
                    for missing in 1..=parity {
                        probability *= (total_shards - missing + 1) as f64 / missing as f64
                            * loss / (1.0 - loss);
                        recoverable += probability;
                    }
                    if 1.0 - recoverable <= TARGET_GROUP_FAILURE {
                        desired = parity;
                        break;
                    }
                }
                if current > 0 && desired <= current {
                    desired = (current + 1).min(MAX_PARITY_SHARDS);
                }
            }
            if desired > current {
                self.profile.store(desired as u8, Ordering::Relaxed);
            }
            self.sample_started = now;
            self.sample_new = 0;
            self.sample_retransmitted = 0;
        }

        if self.shards.is_empty() {
            self.active_parity = self.profile.load(Ordering::Relaxed) as usize;
        }
        if self.active_parity == 0 {
            output(packet)?;
            if self.raw_duplicates > 0 {
                self.raw_duplicates -= 1;
                output(packet)?;
            }
            return Ok(());
        }

        let mut coded = Vec::with_capacity(LENGTH_SIZE + packet.len());
        coded.extend_from_slice(&(packet.len() as u16).to_le_bytes());
        coded.extend_from_slice(packet);
        let mut datagram = Vec::with_capacity(HEADER_SIZE + coded.len());
        datagram.extend_from_slice(&self.conv.to_le_bytes());
        datagram.extend_from_slice(&MAGIC);
        datagram.extend_from_slice(&self.group.to_le_bytes());
        datagram.push(self.shards.len() as u8);
        datagram.push(DATA_SHARDS as u8);
        datagram.push(self.active_parity as u8);
        datagram.extend_from_slice(&coded);
        output(&datagram)?;
        self.shards.push(coded);
        if self.shards.len() < DATA_SHARDS {
            return Ok(());
        }

        let length = self.shards.iter().map(Vec::len).max().unwrap();
        for shard in &mut self.shards {
            shard.resize(length, 0);
        }
        let total_shards = DATA_SHARDS + self.active_parity;
        self.shards.resize_with(total_shards, || vec![0; length]);
        if !self.codes.contains_key(&self.active_parity) {
            let code = ReedSolomon::new(DATA_SHARDS, self.active_parity)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("creating FEC encoder: {error:?}")))?;
            self.codes.insert(self.active_parity, code);
        }
        self.codes[&self.active_parity].encode(&mut self.shards)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("encoding FEC group: {error:?}")))?;
        for index in DATA_SHARDS..total_shards {
            datagram.clear();
            datagram.extend_from_slice(&self.conv.to_le_bytes());
            datagram.extend_from_slice(&MAGIC);
            datagram.extend_from_slice(&self.group.to_le_bytes());
            datagram.push(index as u8);
            datagram.push(DATA_SHARDS as u8);
            datagram.push(self.active_parity as u8);
            datagram.extend_from_slice(&self.shards[index]);
            output(&datagram)?;
        }
        self.shards.clear();
        self.group = self.group.wrapping_add(1);
        Ok(())
    }
}

struct Group {
    parity_shards: usize,
    shards: Vec<Option<Vec<u8>>>,
    delivered: Vec<bool>,
    started: Instant,
}

pub struct Decoder {
    conv: u32,
    groups: HashMap<u32, Group>,
    completed: VecDeque<(u32, Instant)>,
    codes: HashMap<usize, ReedSolomon>,
}

impl Decoder {
    pub fn new(conv: u32) -> Self {
        Self {
            conv,
            groups: HashMap::new(),
            completed: VecDeque::new(),
            codes: HashMap::new(),
        }
    }

    pub fn receive(
        &mut self,
        packet: &[u8],
        mut deliver: impl FnMut(&[u8]) -> Result<(), String>,
    ) -> Result<bool, String> {
        if packet.len() > DATAGRAM_SIZE {
            return Err("oversized FEC datagram".to_owned());
        }
        if packet.get(4..8) != Some(MAGIC.as_slice()) {
            if !(kcp::KCP_OVERHEAD as usize..=KCP_MTU).contains(&packet.len()) {
                return Ok(false);
            }
            deliver(packet)?;
            return Ok(true);
        }
        let shard = shard(packet).ok_or_else(|| "invalid FEC datagram".to_owned())?;
        if shard.conv != self.conv || shard.payload.is_empty() {
            return Err("invalid FEC conversation".to_owned());
        }
        let now = Instant::now();
        self.groups.retain(|_, group| now.duration_since(group.started) < GROUP_TTL);
        while self.completed.front().is_some_and(|(_, when)| now.duration_since(*when) >= GROUP_TTL) {
            self.completed.pop_front();
        }
        if self.completed.iter().any(|(group, _)| *group == shard.group) {
            return Ok(true);
        }
        if !self.groups.contains_key(&shard.group) && self.groups.len() >= MAX_GROUPS {
            if let Some(oldest) = self.groups.iter().min_by_key(|(_, group)| group.started).map(|(group, _)| *group) {
                self.groups.remove(&oldest);
            }
        }
        let total_shards = shard.data_shards + shard.parity_shards;
        let mut group = self.groups.remove(&shard.group).unwrap_or_else(|| Group {
            parity_shards: shard.parity_shards,
            shards: (0..total_shards).map(|_| None).collect(),
            delivered: vec![false; shard.data_shards],
            started: now,
        });
        if group.parity_shards != shard.parity_shards || group.shards.len() != total_shards {
            return Err("inconsistent FEC group".to_owned());
        }
        if group.shards[shard.index].is_none() {
            if shard.index < shard.data_shards {
                let packet = data(shard.payload).ok_or_else(|| "invalid FEC data shard".to_owned())?;
                deliver(packet)?;
                group.delivered[shard.index] = true;
            }
            group.shards[shard.index] = Some(shard.payload.to_vec());
        }
        let present = group.shards.iter().filter(|shard| shard.is_some()).count();
        if present >= shard.data_shards && group.delivered.iter().any(|delivered| !delivered) {
            let length = group.shards.iter().filter_map(|shard| shard.as_ref().map(Vec::len)).max().unwrap();
            for shard in group.shards.iter_mut().flatten() {
                shard.resize(length, 0);
            }
            if !self.codes.contains_key(&group.parity_shards) {
                let code = ReedSolomon::new(DATA_SHARDS, group.parity_shards)
                    .map_err(|error| format!("creating FEC decoder: {error:?}"))?;
                self.codes.insert(group.parity_shards, code);
            }
            self.codes[&group.parity_shards].reconstruct_data(&mut group.shards)
                .map_err(|error| format!("reconstructing FEC group: {error:?}"))?;
            for index in 0..shard.data_shards {
                if !group.delivered[index] {
                    let packet = data(group.shards[index].as_deref().unwrap())
                        .ok_or_else(|| "invalid reconstructed FEC shard".to_owned())?;
                    deliver(packet)?;
                    group.delivered[index] = true;
                }
            }
        }
        if group.delivered.iter().all(|delivered| *delivered) {
            self.completed.push_back((shard.group, now));
            while self.completed.len() > MAX_GROUPS {
                self.completed.pop_front();
            }
        } else {
            self.groups.insert(shard.group, group);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovers_dynamic_fec_profiles() {
        let conv = 0x12345678;
        let acknowledged = Arc::new(AtomicBool::new(true));
        let profile = Arc::new(AtomicU8::new(2));
        let mut encoder = Encoder::new(conv, acknowledged, profile.clone());
        let mut encoded = Vec::new();
        let mut original = Vec::new();
        for group in 0..2 {
            if group == 1 {
                profile.store(5, Ordering::Relaxed);
            }
            for index in 0..DATA_SHARDS {
                let mut packet = vec![0; kcp::KCP_OVERHEAD as usize + group * 3 + index * 7];
                packet[..4].copy_from_slice(&conv.to_le_bytes());
                packet[4] = PUSH;
                packet[12..16].copy_from_slice(&((group * DATA_SHARDS + index) as u32).to_le_bytes());
                let length = (packet.len() - kcp::KCP_OVERHEAD as usize) as u32;
                packet[20..24].copy_from_slice(&length.to_le_bytes());
                original.push(packet.clone());
                encoder.send(&packet, |datagram| {
                    encoded.push(datagram.to_vec());
                    Ok(())
                }).unwrap();
            }
        }
        encoded.retain(|packet| {
            let group = u32::from_le_bytes(packet[8..12].try_into().unwrap());
            let index = packet[12] as usize;
            index >= if group == 0 { 2 } else { 5 }
        });
        encoded.reverse();
        let mut decoder = Decoder::new(conv);
        let mut decoded = Vec::new();
        for packet in &encoded {
            decoder.receive(packet, |packet| {
                decoded.push(packet.to_vec());
                Ok(())
            }).unwrap();
        }
        decoded.sort();
        original.sort();
        assert_eq!(decoded, original);
    }

    #[test]
    fn chooses_heavy_fec_from_measured_retransmissions() {
        let conv = 7;
        let acknowledged = Arc::new(AtomicBool::new(true));
        let profile = Arc::new(AtomicU8::new(0));
        let mut encoder = Encoder::new(conv, acknowledged, profile.clone());
        for sn in 0u32..64 {
            let mut packet = vec![0; kcp::KCP_OVERHEAD as usize];
            packet[..4].copy_from_slice(&conv.to_le_bytes());
            packet[4] = PUSH;
            packet[12..16].copy_from_slice(&sn.to_le_bytes());
            encoder.send(&packet, |_| Ok(())).unwrap();
        }
        for sn in 0u32..7 {
            let mut packet = vec![0; kcp::KCP_OVERHEAD as usize];
            packet[..4].copy_from_slice(&conv.to_le_bytes());
            packet[4] = PUSH;
            packet[12..16].copy_from_slice(&sn.to_le_bytes());
            encoder.send(&packet, |_| Ok(())).unwrap();
        }
        encoder.sample_started = Instant::now() - LOSS_SAMPLE;
        let mut packet = vec![0; kcp::KCP_OVERHEAD as usize];
        packet[..4].copy_from_slice(&conv.to_le_bytes());
        packet[4] = PUSH;
        packet[12..16].copy_from_slice(&64u32.to_le_bytes());
        encoder.send(&packet, |_| Ok(())).unwrap();
        assert_eq!(profile.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn ignores_startup_retransmissions_and_keeps_clean_link_raw() {
        let conv = 9;
        let acknowledged = Arc::new(AtomicBool::new(false));
        let profile = Arc::new(AtomicU8::new(0));
        let mut encoder = Encoder::new(conv, acknowledged.clone(), profile.clone());
        for sn in 0u32..64 {
            let mut packet = vec![0; kcp::KCP_OVERHEAD as usize];
            packet[..4].copy_from_slice(&conv.to_le_bytes());
            packet[4] = PUSH;
            packet[12..16].copy_from_slice(&sn.to_le_bytes());
            encoder.send(&packet, |_| Ok(())).unwrap();
            encoder.send(&packet, |_| Ok(())).unwrap();
        }
        acknowledged.store(true, Ordering::Relaxed);
        for sn in 64u32..128 {
            let mut packet = vec![0; kcp::KCP_OVERHEAD as usize];
            packet[..4].copy_from_slice(&conv.to_le_bytes());
            packet[4] = PUSH;
            packet[12..16].copy_from_slice(&sn.to_le_bytes());
            encoder.send(&packet, |_| Ok(())).unwrap();
        }
        encoder.sample_started = Instant::now() - LOSS_SAMPLE;
        let mut packet = vec![0; kcp::KCP_OVERHEAD as usize];
        packet[..4].copy_from_slice(&conv.to_le_bytes());
        packet[4] = PUSH;
        packet[12..16].copy_from_slice(&128u32.to_le_bytes());
        encoder.send(&packet, |_| Ok(())).unwrap();
        assert_eq!(profile.load(Ordering::Relaxed), 0);
    }
}
