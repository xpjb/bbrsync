mod fec;

use super::{Connection, Incoming, Transport};
use anyhow::{Context, Result, anyhow};
use kcp::{Error as KcpError, Kcp};
use socket2::SockRef;
use std::collections::HashMap;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const WINDOW: u16 = 1024;
const FEC_WINDOW: u16 = 512;
const FEC_MIN_RTO: u32 = 1000;
const UPDATE: Duration = Duration::from_millis(10);
const POLL: Duration = Duration::from_millis(1);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE: Duration = Duration::from_secs(10 * 60);
const DRAIN: Duration = Duration::from_secs(10);
const CLOSE_SETTLE: Duration = Duration::from_millis(20);
const CLOSE_LINGER: Duration = Duration::from_secs(3);
const READ_CAPACITY: usize = 2 << 20;
const WRITE_SIZE: usize = 64 << 10;
const COMMAND_CAPACITY: usize = 32;
const DATAGRAM_CAPACITY: usize = 2048;
const PACKETS_PER_PASS: usize = 4096;
const SOCKET_BUFFER: usize = 4 << 20;
const MAX_PENDING: usize = WINDOW as usize * 2;
const MAX_SESSIONS: usize = 8;
const CLOSED_TTL: Duration = Duration::from_secs(60);

type SessionKey = (SocketAddr, u32);

pub struct KcpTransport;

pub static KCP: KcpTransport = KcpTransport;

struct ReadState {
    bytes: Vec<u8>,
    offset: usize,
    closed: bool,
    error: Option<String>,
}

struct KcpReader {
    state: Arc<(Mutex<ReadState>, Condvar)>,
}

impl Read for KcpReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let (lock, ready) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            let available = state.bytes.len().saturating_sub(state.offset);
            if available > 0 {
                let count = available.min(output.len());
                output[..count].copy_from_slice(&state.bytes[state.offset..state.offset + count]);
                state.offset += count;
                if state.offset == state.bytes.len() {
                    state.bytes.clear();
                    state.offset = 0;
                }
                return Ok(count);
            }
            if let Some(error) = &state.error {
                return Err(io::Error::new(ErrorKind::ConnectionAborted, error.clone()));
            }
            if state.closed {
                return Ok(0);
            }
            state = ready.wait(state).unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

enum Command {
    Write(Vec<u8>),
    Flush(SyncSender<std::result::Result<(), String>>),
}

struct KcpWriter {
    commands: Option<SyncSender<Command>>,
    error: Arc<Mutex<Option<String>>>,
    drained: Arc<(Mutex<bool>, Condvar)>,
}

impl KcpWriter {
    fn failure(&self) -> Option<io::Error> {
        self.error.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_ref()
            .map(|error| io::Error::new(ErrorKind::ConnectionAborted, error.clone()))
    }
}

impl Write for KcpWriter {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.is_empty() {
            return Ok(0);
        }
        if let Some(error) = self.failure() {
            return Err(error);
        }
        let count = input.len().min(WRITE_SIZE);
        self.commands.as_ref().ok_or_else(|| io::Error::new(ErrorKind::BrokenPipe, "KCP connection closed"))?
            .send(Command::Write(input[..count].to_vec()))
            .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "KCP connection closed"))?;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(error) = self.failure() {
            return Err(error);
        }
        let (reply, answer) = mpsc::sync_channel(1);
        self.commands.as_ref().ok_or_else(|| io::Error::new(ErrorKind::BrokenPipe, "KCP connection closed"))?
            .send(Command::Flush(reply))
            .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "KCP connection closed"))?;
        match answer.recv() {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(io::Error::new(ErrorKind::ConnectionAborted, error)),
            Err(_) => Err(self.failure().unwrap_or_else(|| {
                io::Error::new(ErrorKind::BrokenPipe, "KCP connection closed")
            })),
        }
    }
}

impl Drop for KcpWriter {
    fn drop(&mut self) {
        self.commands.take();
        let (lock, drained) = &*self.drained;
        let state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = drained.wait_timeout_while(state, DRAIN, |drained| !*drained);
    }
}

enum UdpEndpoint {
    Connected(Arc<UdpSocket>),
    Peer(Arc<UdpSocket>, SocketAddr),
}

struct UdpOutput {
    endpoint: UdpEndpoint,
    encoder: fec::Encoder,
}

impl Write for UdpOutput {
    fn write(&mut self, packet: &[u8]) -> io::Result<usize> {
        let endpoint = &self.endpoint;
        self.encoder.send(packet, |datagram| {
            let sent = match endpoint {
                UdpEndpoint::Connected(socket) => socket.send(datagram)?,
                UdpEndpoint::Peer(socket, peer) => socket.send_to(datagram, *peer)?,
            };
            if sent != datagram.len() {
                return Err(io::Error::new(ErrorKind::WriteZero, "partial UDP datagram"));
            }
            Ok(())
        })?;
        Ok(packet.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum NetworkInput {
    Connected(Arc<UdpSocket>),
    Routed(Receiver<Vec<u8>>),
}

fn configure_socket(socket: &UdpSocket) -> io::Result<()> {
    let socket = SockRef::from(socket);
    socket.set_recv_buffer_size(SOCKET_BUFFER)?;
    socket.set_send_buffer_size(SOCKET_BUFFER)
}

fn receive_packet(
    kcp: &mut Kcp<UdpOutput>,
    decoder: &mut fec::Decoder,
    packet: &[u8],
    current: u32,
    acknowledged: &AtomicBool,
) -> std::result::Result<bool, String> {
    decoder.receive(packet, |packet| {
        let ack = fec::ack_rtt(packet, current).is_some();
        kcp.input(packet).map_err(|error| format!("invalid KCP datagram: {error}"))?;
        if ack {
            acknowledged.store(true, Ordering::Relaxed);
        }
        Ok(())
    })
}

fn spawn_connection(
    conv: u32,
    endpoint: UdpEndpoint,
    input: NetworkInput,
    close_notice: Option<(SessionKey, mpsc::Sender<SessionKey>)>,
) -> Result<Connection> {
    let acknowledged = Arc::new(AtomicBool::new(false));
    let fec_profile = Arc::new(AtomicU8::new(0));
    let output = UdpOutput {
        endpoint,
        encoder: fec::Encoder::new(conv, acknowledged.clone(), fec_profile.clone()),
    };
    let mut decoder = fec::Decoder::new(conv);
    let mut kcp = Kcp::new_stream(conv, output);
    kcp.set_mtu(fec::KCP_MTU)?;
    kcp.set_nodelay(true, UPDATE.as_millis() as i32, 2, true);
    kcp.set_wndsize(WINDOW, WINDOW);

    let (command_tx, command_rx) = mpsc::sync_channel(COMMAND_CAPACITY);
    let read_state = Arc::new((Mutex::new(ReadState {
        bytes: Vec::new(),
        offset: 0,
        closed: false,
        error: None,
    }), Condvar::new()));
    let write_error = Arc::new(Mutex::new(None));
    let drained = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_read = read_state.clone();
    let worker_error = write_error.clone();
    let worker_drained = drained.clone();

    thread::Builder::new().name(format!("bbrsync-kcp-{conv:08x}")).spawn(move || {
        let started = Instant::now();
        let mut last_packet = Instant::now();
        let mut received_packet = false;
        let mut closing = None;
        let mut drained_at = None;
        let mut packet = vec![0u8; 65536];
        let mut failure = None;
        let mut applied_fec_profile = 0;

        'worker: loop {
            let tick = Instant::now();
            let mut processed = 0;
            match &input {
                NetworkInput::Connected(socket) => while processed < PACKETS_PER_PASS {
                    let count = match socket.recv(&mut packet) {
                        Ok(count) => count,
                        Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => break,
                        Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                        Err(error) => {
                            failure = Some(format!("receiving KCP datagram: {error}"));
                            break 'worker;
                        }
                    };
                    processed += 1;
                    match receive_packet(
                        &mut kcp,
                        &mut decoder,
                        &packet[..count],
                        started.elapsed().as_millis() as u32,
                        &acknowledged,
                    ) {
                        Ok(true) => {
                            last_packet = Instant::now();
                            received_packet = true;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            failure = Some(error);
                            break 'worker;
                        }
                    }
                },
                NetworkInput::Routed(packets) => while processed < PACKETS_PER_PASS {
                    let received = match packets.try_recv() {
                        Ok(packet) => packet,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            failure = Some("KCP listener closed".to_owned());
                            break 'worker;
                        }
                    };
                    processed += 1;
                    match receive_packet(
                        &mut kcp,
                        &mut decoder,
                        &received,
                        started.elapsed().as_millis() as u32,
                        &acknowledged,
                    ) {
                        Ok(true) => {
                            last_packet = Instant::now();
                            received_packet = true;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            failure = Some(error);
                            break 'worker;
                        }
                    }
                },
            }

            while kcp.wait_snd() < MAX_PENDING {
                match command_rx.try_recv() {
                    Ok(Command::Write(bytes)) => {
                        if let Err(error) = kcp.send(&bytes) {
                            failure = Some(format!("queueing KCP data: {error}"));
                            break 'worker;
                        }
                    }
                    Ok(Command::Flush(reply)) => {
                        let current = started.elapsed().as_millis() as u32;
                        let result = kcp.update(current).and_then(|_| kcp.flush())
                            .map_err(|error| format!("flushing KCP data: {error}"));
                        let failed = result.as_ref().err().cloned();
                        let _ = reply.send(result);
                        if let Some(error) = failed {
                            failure = Some(error);
                            break 'worker;
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        closing.get_or_insert_with(Instant::now);
                        break;
                    }
                }
            }

            let selected_fec_profile = fec_profile.load(Ordering::Relaxed);
            if selected_fec_profile != applied_fec_profile {
                if selected_fec_profile >= 4 {
                    kcp.set_rx_minrto(FEC_MIN_RTO);
                    kcp.set_fast_resend(0);
                    kcp.set_wndsize(FEC_WINDOW, FEC_WINDOW);
                } else {
                    kcp.set_fast_resend(2);
                    kcp.set_wndsize(WINDOW, WINDOW);
                }
                applied_fec_profile = selected_fec_profile;
            }

            if let Err(error) = kcp.update(started.elapsed().as_millis() as u32) {
                failure = Some(format!("updating KCP state: {error}"));
                break;
            }

            loop {
                let capacity = {
                    let state = worker_read.0.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                    READ_CAPACITY.saturating_sub(state.bytes.len().saturating_sub(state.offset))
                };
                let size = match kcp.peeksize() {
                    Ok(size) if size <= capacity => size,
                    Ok(_) => break,
                    Err(KcpError::RecvQueueEmpty | KcpError::ExpectingFragment) => break,
                    Err(error) => {
                        failure = Some(format!("reading KCP stream: {error}"));
                        break 'worker;
                    }
                };
                let mut bytes = vec![0u8; size];
                if let Err(error) = kcp.recv(&mut bytes) {
                    failure = Some(format!("reading KCP stream: {error}"));
                    break 'worker;
                }
                let (lock, ready) = &*worker_read;
                let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                if state.offset > READ_CAPACITY / 2 {
                    let offset = state.offset;
                    state.bytes.drain(..offset);
                    state.offset = 0;
                }
                state.bytes.extend_from_slice(&bytes);
                ready.notify_all();
            }

            if kcp.is_dead_link() {
                failure = Some("KCP peer stopped acknowledging data".to_owned());
                break;
            }
            if let Some(since) = closing {
                if kcp.wait_snd() == 0 {
                    if drained_at.is_none() {
                        drained_at = Some(Instant::now());
                    }
                    if drained_at.as_ref().is_some_and(|at| at.elapsed() >= CLOSE_SETTLE) {
                        let (lock, ready) = &*worker_drained;
                        let mut drained = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                        if !*drained {
                            *drained = true;
                            ready.notify_all();
                        }
                    }
                    if drained_at.as_ref().is_some_and(|at| at.elapsed() >= CLOSE_LINGER) {
                        break;
                    }
                }
                if since.elapsed() >= DRAIN {
                    break;
                }
            } else if !received_packet && last_packet.elapsed() >= CONNECT_TIMEOUT {
                failure = Some("KCP endpoint did not answer".to_owned());
                break;
            } else if received_packet && last_packet.elapsed() >= IDLE {
                failure = Some("KCP session timed out".to_owned());
                break;
            }
            if processed < PACKETS_PER_PASS {
                let cadence = if drained_at.is_some() { UPDATE } else { POLL };
                if let Some(delay) = cadence.checked_sub(tick.elapsed()) {
                    thread::sleep(delay);
                }
            }
        }

        if let Some(error) = &failure {
            *worker_error.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.clone());
        }
        let (lock, ready) = &*worker_read;
        let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.error = failure;
        state.closed = true;
        ready.notify_all();
        drop(state);
        if let Some((key, closed)) = close_notice {
            let _ = closed.send(key);
        }
        let (lock, ready) = &*worker_drained;
        *lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
        ready.notify_all();
    }).context("starting KCP session worker")?;

    Ok(Connection {
        reader: Box::new(KcpReader { state: read_state }),
        writer: Box::new(KcpWriter {
            commands: Some(command_tx),
            error: write_error,
            drained,
        }),
    })
}

struct KcpIncoming {
    accepted: Receiver<Result<Connection>>,
    shutdown: Arc<AtomicBool>,
}

impl Iterator for KcpIncoming {
    type Item = Result<Connection>;

    fn next(&mut self) -> Option<Self::Item> {
        self.accepted.recv().ok()
    }
}

impl Drop for KcpIncoming {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

impl Transport for KcpTransport {
    fn connect(&self, address: &str) -> Result<Connection> {
        let addresses: Vec<_> = address.to_socket_addrs()
            .with_context(|| format!("resolving KCP endpoint {address}"))?
            .collect();
        let peer = addresses.iter().copied().find(SocketAddr::is_ipv4)
            .or_else(|| addresses.first().copied())
            .ok_or_else(|| anyhow!("KCP endpoint {address} resolved to no addresses"))?;
        let socket = UdpSocket::bind(if peer.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" })
            .context("binding KCP client socket")?;
        configure_socket(&socket).context("configuring KCP client socket")?;
        socket.connect(peer).with_context(|| format!("connecting KCP to {peer}"))?;
        socket.set_nonblocking(true)?;
        let socket = Arc::new(socket);
        let mut id = [0u8; 4];
        getrandom::fill(&mut id).context("generating KCP conversation ID")?;
        let conv = u32::from_le_bytes(id).max(1);
        spawn_connection(
            conv,
            UdpEndpoint::Connected(socket.clone()),
            NetworkInput::Connected(socket),
            None,
        )
    }

    fn incoming(&self, address: &str) -> Result<Incoming> {
        let socket = UdpSocket::bind(address)
            .with_context(|| format!("binding KCP {address}"))?;
        configure_socket(&socket).context("configuring KCP listener socket")?;
        socket.set_read_timeout(Some(POLL))?;
        let socket = Arc::new(socket);
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(MAX_SESSIONS);
        let (closed_tx, closed_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = shutdown.clone();
        thread::Builder::new().name("bbrsync-kcp-listener".to_owned()).spawn(move || {
            let mut sessions: HashMap<SessionKey, SyncSender<Vec<u8>>> = HashMap::new();
            let mut closed: HashMap<SessionKey, Instant> = HashMap::new();
            let mut packet = vec![0u8; 65536];
            while !worker_shutdown.load(Ordering::Acquire) {
                while let Ok(key) = closed_rx.try_recv() {
                    sessions.remove(&key);
                    closed.insert(key, Instant::now());
                }
                closed.retain(|_, when| when.elapsed() < CLOSED_TTL);
                let (count, peer) = match socket.recv_from(&mut packet) {
                    Ok(received) => received,
                    Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::ConnectionReset) => continue,
                    Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                    Err(error) => {
                        if accepted_tx.send(Err(error).context("receiving KCP datagram")).is_err() {
                            break;
                        }
                        continue;
                    }
                };
                let Some((conv, initial)) = fec::session_packet(&packet[..count]) else {
                    continue;
                };
                let key = (peer, conv);
                if let Some(session) = sessions.get(&key) {
                    match session.try_send(packet[..count].to_vec()) {
                        Ok(()) | Err(TrySendError::Full(_)) => {}
                        Err(TrySendError::Disconnected(_)) => {
                            sessions.remove(&key);
                            closed.insert(key, Instant::now());
                        }
                    }
                    continue;
                }
                if !initial || closed.contains_key(&key) || sessions.len() >= MAX_SESSIONS {
                    continue;
                }
                let (packet_tx, packet_rx) = mpsc::sync_channel(DATAGRAM_CAPACITY);
                if packet_tx.try_send(packet[..count].to_vec()).is_err() {
                    continue;
                }
                let connection = match spawn_connection(
                    conv,
                    UdpEndpoint::Peer(socket.clone(), peer),
                    NetworkInput::Routed(packet_rx),
                    Some((key, closed_tx.clone())),
                ) {
                    Ok(connection) => connection,
                    Err(error) => {
                        if accepted_tx.send(Err(error)).is_err() {
                            break;
                        }
                        continue;
                    }
                };
                sessions.insert(key, packet_tx);
                match accepted_tx.try_send(Ok(connection)) {
                    Ok(()) => {}
                    Err(TrySendError::Full(connection)) => {
                        drop(connection);
                        sessions.remove(&key);
                        closed.insert(key, Instant::now());
                    }
                    Err(TrySendError::Disconnected(_)) => break,
                }
            }
        }).context("starting KCP listener")?;
        Ok(Box::new(KcpIncoming { accepted: accepted_rx, shutdown }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_allows_a_final_ack_update() {
        let probe = UdpSocket::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap().to_string();
        drop(probe);
        let mut incoming = KCP.incoming(&address).unwrap();
        let server = thread::spawn(move || {
            let Connection { mut reader, mut writer } = incoming.next().unwrap().unwrap();
            let mut request = [0u8; 1];
            reader.read_exact(&mut request).unwrap();
            assert_eq!(request, [1]);
            writer.write_all(&[2]).unwrap();
            writer.flush().unwrap();
        });

        let Connection { mut reader, mut writer } = KCP.connect(&address).unwrap();
        writer.write_all(&[1]).unwrap();
        writer.flush().unwrap();
        let mut response = [0u8; 1];
        reader.read_exact(&mut response).unwrap();
        assert_eq!(response, [2]);
        let started = Instant::now();
        drop(writer);
        assert!(started.elapsed() >= CLOSE_SETTLE);
        drop(reader);
        server.join().unwrap();
    }
}
