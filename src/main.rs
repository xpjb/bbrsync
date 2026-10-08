//! bbrsync: one-way, content-defined-chunk sync over a trusted TCP connection.
//! Wire exchange:
//! receiver Files -> sender Diffs -> receiver Needs -> sender Chunks/Done -> receiver Ok.

use anyhow::{bail, Context, Result};
use bincode::Options as _;
use fastcdc::v2020::StreamCDC;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::time::UNIX_EPOCH;

const VERSION: u32 = 1;
const MIN_CHUNK: usize = 4096;
const AVG_CHUNK: usize = 16384;
const MAX_CHUNK: usize = 65536;
const TMP_SUFFIX: &str = ".bbrsync-tmp";
const IGNORE: &str = ".bbrsyncignore";
const DEFAULT_PATTERNS: [&str; 7] =
    [".git", ".hg", ".svn", ".DS_Store", "Thumbs.db", "*.swp", "*~"];

// -------------------------------------------------------------------- data

type Hash = [u8; 32];
type Manifest = BTreeMap<String, Hash>;

#[derive(Serialize, Deserialize, Clone)]
struct ChunkRef {
    hash: Hash,
    start: u64,
    len: u64,
}

#[derive(Serialize, Deserialize)]
struct FileRef {
    path: String,
    content: Hash,
    chunks: Vec<ChunkRef>,
}

#[derive(Serialize, Deserialize)]
struct FileNeed {
    path: String,
    send: Vec<bool>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Entry {
    size: u64,
    mtime: i64,
    content: Hash,
    chunks: Vec<ChunkRef>,
}

type Cache = BTreeMap<String, Entry>;

struct Local {
    root: PathBuf,
    entries: Cache,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct Opts {
    delete: bool,
    patterns: Vec<String>,
    dry: bool,
    full: bool,
}

#[derive(Serialize, Deserialize)]
struct Plan {
    refs: Vec<FileRef>,
    deletes: Vec<String>,
    total: u64,
}

#[derive(Serialize, Deserialize, Default, Clone, Copy, Debug)]
struct Stats {
    files: u64,
    deleted: u64,
    received: u64,
    assembled: u64,
}

#[derive(Serialize, Deserialize)]
enum Msg {
    Hello { version: u32, token: String, path: String, pull: bool, opts: Opts },
    Ready { version: u32 },
    Files(Manifest),
    Diffs(Plan),
    Needs(Vec<FileNeed>),
    Chunk(Vec<u8>),
    Done,
    Ok(Stats),
    Error(String),
}

struct Pat {
    negate: bool,
    dir_only: bool,
    anchored: bool,
    glob: String,
}

struct Patterns {
    lines: Vec<Pat>,
}

// ----------------------------------------------- paths and temporary files

fn is_internal(name: &str) -> bool {
    name.starts_with(".bbrsync") || name.ends_with(TMP_SUFFIX)
}

fn is_link(md: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        md.file_attributes() & 0x400 != 0 // includes junctions, not just symlinks
    }
    #[cfg(not(windows))]
    {
        md.file_type().is_symlink()
    }
}

/// Resolve existing ancestors and validate the root without creating it.
fn resolve_target(base: Option<&Path>, dest: &str) -> Result<PathBuf> {
    if dest.is_empty() {
        bail!("no destination path given");
    }
    #[cfg(unix)]
    if dest.contains('\\') || dest.contains(':') {
        bail!("{dest:?} looks like a Windows path, not a path on this daemon");
    }
    let dest = Path::new(dest);
    let joined = if dest.is_absolute() {
        dest.to_path_buf()
    } else {
        base.context("destination must be absolute when the daemon has no --root")?.join(dest)
    };
    let mut normal = PathBuf::new();
    for c in joined.components() {
        match c {
            Component::ParentDir => {
                normal.pop();
            }
            Component::CurDir => {}
            other => normal.push(other.as_os_str()),
        }
    }
    let mut ancestor = normal.as_path();
    let resolved = loop {
        match ancestor.canonicalize() {
            Ok(p) => break p.join(normal.strip_prefix(ancestor)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                ancestor = ancestor.parent().context("no existing destination ancestor")?;
            }
            Err(e) => return Err(e.into()),
        }
    };
    if let Some(base) = base {
        if !resolved.starts_with(base) {
            bail!("{} is outside the served base {}", resolved.display(), base.display());
        }
    }
    if resolved.parent().is_none() {
        bail!("refusing to use the filesystem root as a destination");
    }
    #[cfg(unix)]
    for forbidden in
        ["/bin", "/boot", "/dev", "/etc", "/lib", "/proc", "/root", "/sbin", "/sys", "/usr"]
    {
        if resolved.starts_with(forbidden) {
            bail!("refusing to use {forbidden} as a destination");
        }
    }
    Ok(resolved)
}

/// Reject traversal, internal names and existing links. The local filesystem
/// is trusted; this is not a sandbox against concurrent hostile changes.
fn safe_join(root: &Path, rel: &str) -> Result<PathBuf> {
    if rel.is_empty() || rel.contains('\\') || rel.contains(':') {
        bail!("unsafe path {rel:?}");
    }
    let mut p = root.to_path_buf();
    for c in Path::new(rel).components() {
        match c {
            Component::Normal(x) if !is_internal(&x.to_string_lossy()) => p.push(x),
            _ => bail!("unsafe path {rel:?}"),
        }
        match fs::symlink_metadata(&p) {
            Ok(md) if is_link(&md) => bail!("refusing to follow a link at {}", p.display()),
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(p)
}

/// Set modes only on directories we create; leave existing directories alone.
fn make_dirs(root: &Path, dest: &Path) -> Result<()> {
    let rel = dest.strip_prefix(root)?;
    let Some(parent) = rel.parent() else { return Ok(()) };
    let mut cur = root.to_path_buf();
    for c in parent.components() {
        cur.push(c);
        match fs::create_dir(&cur) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&cur, fs::Permissions::from_mode(0o755))?;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e).with_context(|| format!("creating {}", cur.display())),
        }
    }
    Ok(())
}

fn temp_file(parent: &Path) -> Result<tempfile::NamedTempFile> {
    Ok(tempfile::Builder::new().prefix(".bbrsync-").suffix(TMP_SUFFIX).tempfile_in(parent)?)
}

// ------------------------------------------- hashing, caching and scanning

fn hash_of(b: &[u8]) -> Hash {
    *blake3::hash(b).as_bytes()
}

fn describe(path: &Path) -> Result<(Hash, Vec<ChunkRef>)> {
    let file = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut chunks = Vec::new();
    let mut content = blake3::Hasher::new();
    for c in StreamCDC::new(file, MIN_CHUNK, AVG_CHUNK, MAX_CHUNK) {
        let c = c?;
        content.update(&c.data);
        chunks.push(ChunkRef { hash: hash_of(&c.data), start: c.offset, len: c.data.len() as u64 });
    }
    Ok((*content.finalize().as_bytes(), chunks))
}

fn mtime_ns(md: &fs::Metadata) -> i64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn default_cache_dir() -> Result<PathBuf> {
    #[cfg(windows)]
    {
        let base = std::env::var_os("LOCALAPPDATA")
            .context("LOCALAPPDATA is not set; use --cache-dir")?;
        Ok(PathBuf::from(base).join("bbrsync"))
    }
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME").context("HOME is not set; use --cache-dir")?;
        Ok(PathBuf::from(home).join("Library/Caches/bbrsync"))
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Some(base) = std::env::var_os("XDG_CACHE_HOME").filter(|base| !base.is_empty()) {
            let base = PathBuf::from(base);
            if base.is_absolute() {
                return Ok(base.join("bbrsync"));
            }
        }
        let home = std::env::var_os("HOME").context("HOME is not set; use --cache-dir")?;
        Ok(PathBuf::from(home).join(".cache/bbrsync"))
    }
}

fn cache_path(cache_dir: &Path, root: &Path) -> PathBuf {
    let root = root.components().collect::<PathBuf>();
    let mut id = blake3::Hasher::new();
    id.update(b"bbrsync-cache-v1\0");
    id.update(root.as_os_str().to_string_lossy().as_bytes());
    cache_dir.join("trees").join(format!("{}.bin", id.finalize().to_hex()))
}

fn load_cache(path: &Path) -> Cache {
    File::open(path)
        .ok()
        .and_then(|f| {
            let len = f.metadata().ok()?.len();
            bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(len)
                .deserialize_from(BufReader::new(f))
                .ok()
        })
        .unwrap_or_default()
}

fn save_cache(path: &Path, cache: &Cache) -> Result<()> {
    let parent = path.parent().context("cache path has no parent")?;
    fs::create_dir_all(parent)?;
    let mut tmp = temp_file(parent)?;
    let mut writer = BufWriter::new(tmp.as_file_mut());
    bincode::serialize_into(&mut writer, cache)?;
    writer.flush()?;
    drop(writer);
    tmp.as_file().sync_all()?;
    tmp.persist(path)?;
    Ok(())
}

/// `*` and `?` match within a path component; `**` can cross `/`.
fn glob_match(pat: &str, text: &str) -> bool {
    fn go(p: &[u8], t: &[u8]) -> bool {
        match p.first() {
            None => t.is_empty(),
            Some(b'*') => {
                let double = p.get(1) == Some(&b'*');
                let rest = if double && p.get(2) == Some(&b'/') { &p[3..] } else { &p[1..] };
                if double {
                    (0..=t.len()).any(|i| go(rest, &t[i..]))
                } else {
                    let mut i = 0;
                    loop {
                        if go(rest, &t[i..]) {
                            return true;
                        }
                        if i >= t.len() || t[i] == b'/' {
                            return false;
                        }
                        i += 1;
                    }
                }
            }
            Some(b'?') => !t.is_empty() && t[0] != b'/' && go(&p[1..], &t[1..]),
            Some(c) => t.first() == Some(c) && go(&p[1..], &t[1..]),
        }
    }
    go(pat.as_bytes(), text.as_bytes())
}

impl Patterns {
    fn parse(lines: &[String]) -> Self {
        let mut out = Vec::new();
        for raw in lines {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let negate = line.starts_with('!');
            let rest = line.strip_prefix('!').unwrap_or(line);
            let dir_only = rest.ends_with('/');
            let rest = rest.strip_suffix('/').unwrap_or(rest);
            let anchored = rest.starts_with('/');
            let glob = rest.strip_prefix('/').unwrap_or(rest);
            if !glob.is_empty() {
                out.push(Pat { negate, dir_only, anchored, glob: glob.to_string() });
            }
        }
        Self { lines: out }
    }

    /// Search backwards: the last matching pattern wins.
    fn ignored(&self, rel: &str, is_dir: bool) -> bool {
        for p in self.lines.iter().rev() {
            if p.dir_only && !is_dir {
                continue;
            }
            let hit = if p.anchored || p.glob.contains('/') {
                glob_match(&p.glob, rel)
            } else {
                rel.split('/').any(|c| glob_match(&p.glob, c))
            };
            if hit {
                return !p.negate;
            }
        }
        false
    }
}

fn load_patterns(src: &Path, extra: &[String]) -> Result<Vec<String>> {
    let mut lines: Vec<String> = DEFAULT_PATTERNS.into_iter().map(str::to_owned).collect();
    match fs::read_to_string(src.join(IGNORE)) {
        Ok(text) => lines.extend(text.lines().map(|l| l.to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("reading {}", src.join(IGNORE).display())),
    }
    lines.extend(extra.iter().cloned());
    Ok(lines)
}

impl Local {
    /// Missing destinations are empty until commit. Missing/unreadable sources
    /// are errors, never an empty manifest that could authorize deletion.
    fn scan(root: &Path, cache_dir: &Path, opts: &Opts, receiving: bool) -> Result<Self> {
        match fs::metadata(root) {
            Ok(md) if md.is_dir() => {}
            Ok(_) => bail!("{} is not a directory", root.display()),
            Err(e) if receiving && e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self { root: root.to_path_buf(), entries: Cache::new() });
            }
            Err(e) => return Err(e).with_context(|| format!("reading {}", root.display())),
        }
        let cache = load_cache(&cache_path(cache_dir, root));
        let pats = Patterns::parse(&opts.patterns);
        let mut entries = Cache::new();
        let mut stack = vec![(root.to_path_buf(), String::new())];
        while let Some((dir, prefix)) = stack.pop() {
            for e in fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))? {
                let e = e?;
                let name = e
                    .file_name()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("non-UTF-8 filename in {}", dir.display()))?;
                if is_internal(&name) {
                    continue;
                }
                let rel = if prefix.is_empty() { name } else { format!("{prefix}/{name}") };
                let md = e.metadata()?;
                if is_link(&md) || pats.ignored(&rel, md.is_dir()) {
                    continue;
                }
                if md.is_dir() {
                    stack.push((e.path(), rel));
                } else if md.is_file() {
                    let (size, mtime) = (md.len(), mtime_ns(&md));
                    let entry = match cache.get(&rel) {
                        Some(e) if e.size == size && e.mtime == mtime && !opts.full => e.clone(),
                        _ => {
                            let (content, chunks) = describe(&e.path())
                                .with_context(|| format!("cannot read {}", e.path().display()))?;
                            Entry { size, mtime, content, chunks }
                        }
                    };
                    entries.insert(rel, entry);
                }
            }
        }
        Ok(Self { root: root.to_path_buf(), entries })
    }
}

impl Plan {
    fn print(&self) {
        for f in &self.refs {
            println!("   write {}", f.path);
        }
        for p in &self.deletes {
            println!("   delete {p}");
        }
    }

    /// One preflight for both directions, before any write or deletion.
    fn check(&self, local: &Local, delete: bool) -> Result<()> {
        if !delete && !self.deletes.is_empty() {
            bail!("peer requested deletion without --delete");
        }
        let (doomed, held) = (self.deletes.len(), local.entries.len());
        if doomed > 0 && (self.total == 0 || doomed > held / 2) {
            bail!("refusing to delete {doomed} of {held} file(s): empty source or more than half the destination");
        }
        let mut seen = BTreeSet::new();
        for path in self.refs.iter().map(|f| &f.path).chain(&self.deletes) {
            safe_join(&local.root, path)?;
            if !seen.insert(path) {
                bail!("duplicate or conflicting path {path:?}");
            }
        }
        for path in &self.deletes {
            if !local.entries.contains_key(path) {
                bail!("deletion was not in the destination manifest: {path:?}");
            }
        }
        Ok(())
    }
}

// --------------------------------------------------------- framed messages

fn send(w: &mut impl Write, msg: &Msg) -> Result<()> {
    let body = bincode::serialize(msg)?;
    w.write_all(&u32::try_from(body.len())?.to_le_bytes())?;
    w.write_all(&body)?;
    w.flush()?;
    Ok(())
}

fn recv(r: &mut impl Read) -> Result<Msg> {
    let mut n = [0u8; 4];
    r.read_exact(&mut n).context("peer closed the connection")?;
    let len = u32::from_le_bytes(n) as usize;
    if len > (1 << 30) {
        bail!("frame too large");
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body).context("truncated frame")?;
    match bincode::deserialize(&body)? {
        Msg::Error(msg) => bail!("peer: {msg}"),
        msg => Ok(msg),
    }
}

fn send_error(w: &mut impl Write, msg: impl std::fmt::Display) {
    let _ = send(w, &Msg::Error(msg.to_string()));
}

// ---------------------------------------------- shared sender and receiver

fn chunk_index(entry: Option<&Entry>) -> HashMap<Hash, (u64, u64)> {
    entry.into_iter().flat_map(|e| &e.chunks).map(|c| (c.hash, (c.start, c.len))).collect()
}

fn send_chunks(w: &mut impl Write, root: &Path, fresh: &Cache, needs: &[FileNeed]) -> Result<()> {
    for need in needs {
        let entry = fresh.get(&need.path).context("peer requested an unknown file")?;
        if need.send.len() != entry.chunks.len() {
            bail!("invalid chunk needs for {}", need.path);
        }
        if !need.send.iter().any(|s| *s) {
            continue;
        }
        let mut f = BufReader::with_capacity(1 << 20, File::open(safe_join(root, &need.path)?)?);
        for (chunk, want) in entry.chunks.iter().zip(&need.send) {
            if *want {
                f.seek(SeekFrom::Start(chunk.start))?;
                let mut data = vec![0u8; chunk.len as usize];
                f.read_exact(&mut data)?;
                send(w, &Msg::Chunk(data))?;
            }
        }
    }
    send(w, &Msg::Done)
}

/// Stage and verify every changed file, then publish. Reuse is determined
/// from the chunk index, not a second list of flags parallel to the manifest.
fn assemble(
    root: &Path,
    refs: &[FileRef],
    have: &Cache,
    stream: &mut impl Read,
) -> Result<(u64, u64, Vec<(String, Entry)>)> {
    let mut staged = Vec::new();
    let (mut received, mut written) = (0, 0);
    for f in refs {
        let dest = safe_join(root, &f.path)?;
        make_dirs(root, &dest)?;
        let mut tmp = temp_file(dest.parent().context("missing file parent")?)?;
        // Updating content must not make a private file public or remove +x.
        let permissions = match fs::metadata(&dest) {
            Ok(md) => Some(md.permissions()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        if let Some(p) = permissions {
            tmp.as_file().set_permissions(p)?;
        } else {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tmp.as_file().set_permissions(fs::Permissions::from_mode(0o644))?;
            }
        }
        let local_at = chunk_index(have.get(&f.path));
        let mut existing = if f.chunks.iter().any(|c| local_at.contains_key(&c.hash)) {
            Some(BufReader::with_capacity(1 << 20, File::open(&dest)?))
        } else {
            None
        };
        for chunk in &f.chunks {
            let data = if let Some((start, len)) = local_at.get(&chunk.hash) {
                let rd = existing.as_mut().context("missing local reader")?;
                rd.seek(SeekFrom::Start(*start))?;
                let mut data = vec![0u8; *len as usize];
                rd.read_exact(&mut data)?;
                data
            } else {
                match recv(stream)? {
                    Msg::Chunk(data) => {
                        received += data.len() as u64;
                        data
                    }
                    _ => bail!("expected a chunk for {}", f.path),
                }
            };
            if data.len() as u64 != chunk.len || hash_of(&data) != chunk.hash {
                bail!("chunk mismatch in {}", f.path);
            }
            tmp.write_all(&data)?;
            written += data.len() as u64;
        }
        tmp.as_file().sync_all()?;
        let (content, chunks) = describe(tmp.path())?;
        if content != f.content {
            bail!("content mismatch in {}", f.path);
        }
        staged.push((f.path.clone(), tmp.into_temp_path(), content, chunks));
    }
    match recv(stream)? {
        Msg::Done => {}
        _ => bail!("expected the end of the chunk stream"),
    }
    let mut published = Vec::new();
    for (rel, tmp, content, chunks) in staged {
        let dest = safe_join(root, &rel)?;
        tmp.persist(&dest)?;
        let md = fs::metadata(dest)?;
        published.push((rel, Entry { size: md.len(), mtime: mtime_ns(&md), content, chunks }));
    }
    Ok((received, written, published))
}

fn send_tree(w: &mut impl Write, r: &mut impl Read, local: &Local, opts: &Opts) -> Result<Stats> {
    let theirs = match recv(r)? {
        Msg::Files(files) => files,
        _ => bail!("expected receiver's file list"),
    };
    let plan = Plan {
        refs: local
            .entries
            .iter()
            .filter(|(p, e)| theirs.get(*p) != Some(&e.content))
            .map(|(p, e)| FileRef { path: p.clone(), content: e.content, chunks: e.chunks.clone() })
            .collect(),
        deletes: theirs
            .keys()
            .filter(|p| opts.delete && !local.entries.contains_key(*p))
            .cloned()
            .collect(),
        total: local.entries.len() as u64,
    };
    if opts.dry {
        plan.print();
    }
    send(w, &Msg::Diffs(plan))?;
    if !opts.dry {
        let needs = match recv(r)? {
            Msg::Needs(files) => files,
            _ => bail!("expected chunk needs"),
        };
        send_chunks(w, &local.root, &local.entries, &needs)?;
    }
    match recv(r)? {
        Msg::Ok(stats) => Ok(stats),
        _ => bail!("receiver did not confirm the sync"),
    }
}

fn receive_tree(
    w: &mut impl Write,
    r: &mut impl Read,
    local: &mut Local,
    opts: &Opts,
) -> Result<Stats> {
    let files = local.entries.iter().map(|(p, e)| (p.clone(), e.content)).collect();
    send(w, &Msg::Files(files))?;
    let plan = match recv(r)? {
        Msg::Diffs(plan) => plan,
        _ => bail!("expected sender's plan"),
    };
    plan.check(local, opts.delete)?;
    let mut stats = Stats::default();
    if opts.dry {
        plan.print();
    } else {
        let needs = plan
            .refs
            .iter()
            .map(|f| {
                let held = chunk_index(local.entries.get(&f.path));
                FileNeed {
                    path: f.path.clone(),
                    send: f.chunks.iter().map(|c| !held.contains_key(&c.hash)).collect(),
                }
            })
            .collect();
        send(w, &Msg::Needs(needs))?;
        fs::create_dir_all(&local.root)?;
        let (received, assembled, published) =
            assemble(&local.root, &plan.refs, &local.entries, r)?;
        stats = Stats { files: published.len() as u64, received, assembled, deleted: 0 };
        local.entries.extend(published);
        // Only planned deletions, after every file has verified and published.
        for rel in &plan.deletes {
            let path = safe_join(&local.root, rel)?;
            fs::remove_file(&path).with_context(|| format!("deleting {}", path.display()))?;
            local.entries.remove(rel);
            stats.deleted += 1;
        }
    }
    send(w, &Msg::Ok(stats))?;
    Ok(stats)
}

fn transfer(
    w: &mut impl Write,
    r: &mut impl Read,
    root: &Path,
    cache_dir: &Path,
    sending: bool,
    opts: &Opts,
) -> Result<Stats> {
    let result = (|| {
        let mut local = Local::scan(root, cache_dir, opts, !sending)?;
        let stats = if sending {
            send_tree(w, r, &local, opts)?
        } else {
            receive_tree(w, r, &mut local, opts)?
        };
        if !opts.dry {
            if let Err(e) = save_cache(&cache_path(cache_dir, &local.root), &local.entries) {
                eprintln!("warning: could not save hash cache: {e:#}");
            }
        }
        Ok(stats)
    })();
    if let Err(e) = &result {
        send_error(w, format!("{e:#}"));
    }
    result
}

// ------------------------------------------------------- client and daemon

fn human_bytes(n: u64) -> String {
    if n >= 1048576 {
        format!("{:.1} MB", n as f64 / 1048576.0)
    } else if n >= 1024 {
        format!("{:.1} KB", n as f64 / 1024.0)
    } else {
        format!("{n} B")
    }
}

fn sync(
    local: &Path,
    remote: &str,
    host: &str,
    token: &str,
    cache_dir: &Path,
    pull: bool,
    opts: &Opts,
) -> Result<()> {
    let started = std::time::Instant::now();
    let local = if pull {
        resolve_target(None, &std::env::current_dir()?.join(local).to_string_lossy())?
    } else {
        local.canonicalize()?
    };
    let local_label = local.display().to_string();
    let remote_label = format!("{host}/{}", remote.strip_prefix('/').unwrap_or(remote));
    let (source, dest) = if pull {
        (&remote_label, &local_label)
    } else {
        (&local_label, &remote_label)
    };
    println!("source: {source}\ndest:   {dest}");
    let sock = TcpStream::connect(host).with_context(|| format!("connecting to {host}"))?;
    sock.set_nodelay(true).ok();
    let mut w = BufWriter::with_capacity(64 << 10, sock.try_clone()?);
    let mut r = BufReader::with_capacity(64 << 10, sock);
    send(
        &mut w,
        &Msg::Hello {
            version: VERSION,
            token: token.to_string(),
            path: remote.to_string(),
            pull,
            opts: opts.clone(),
        },
    )?;
    match recv(&mut r)? {
        Msg::Ready { version } if version == VERSION => {}
        Msg::Ready { version } => bail!("daemon speaks v{version}, this build speaks v{VERSION}"),
        _ => bail!("unexpected reply to hello"),
    }
    let stats = transfer(&mut w, &mut r, &local, cache_dir, !pull, opts)?;
    if !opts.dry {
        let elapsed = started.elapsed();
        let secs = elapsed.as_secs_f64();
        let rate = if stats.received >= 64 * 1024 && secs >= 0.05 {
            format!("  ({}/s)", human_bytes((stats.received as f64 / secs) as u64))
        } else {
            String::new()
        };
        println!(
            "   {} file(s) changed, {} transferred, {} written, {} deleted in {elapsed:.1?}{rate}",
            stats.files,
            human_bytes(stats.received),
            human_bytes(stats.assembled),
            stats.deleted
        );
    }
    Ok(())
}

fn session(sock: TcpStream, base: Option<&Path>, token: &str, cache_dir: &Path) -> Result<()> {
    let mut w = BufWriter::with_capacity(64 << 10, sock.try_clone()?);
    let mut r = BufReader::with_capacity(64 << 10, sock);
    let hello = (|| match recv(&mut r)? {
        Msg::Hello { version, token: t, path, pull, opts } => {
            if version != VERSION {
                bail!("protocol mismatch: client v{version}, daemon v{VERSION}");
            }
            if !token.is_empty() && t != token {
                bail!("bad token");
            }
            Ok((resolve_target(base, &path)?, pull, opts))
        }
        _ => bail!("expected hello"),
    })();
    let (root, pull, opts) = match hello {
        Ok(v) => v,
        Err(e) => {
            send_error(&mut w, format!("{e:#}"));
            return Err(e);
        }
    };
    send(&mut w, &Msg::Ready { version: VERSION })?;
    transfer(&mut w, &mut r, &root, cache_dir, pull, &opts)?;
    Ok(())
}

fn serve(base: Option<&Path>, listen: &str, token: &str, cache_dir: &Path) -> Result<()> {
    // `--root` is optional: it confines clients to a subtree, but each client
    // still names its own path.
    let base = match base {
        Some(b) => {
            fs::create_dir_all(b)?;
            Some(b.canonicalize()?)
        }
        None => None,
    };
    match &base {
        Some(b) => eprintln!("bbrsync serving on {listen}, confined to {}", b.display()),
        None => eprintln!("bbrsync serving on {listen}"),
    }
    let listener = TcpListener::bind(listen).with_context(|| format!("binding {listen}"))?;
    for conn in listener.incoming() {
        match conn {
            Ok(sock) => {
                sock.set_nodelay(true).ok();
                if let Err(e) = session(sock, base.as_deref(), token, cache_dir) {
                    eprintln!("session ended: {e:#}");
                }
            }
            Err(e) => eprintln!("accept failed: {e}"),
        }
    }
    Ok(())
}

// ------------------------------------------------------------ command line

fn remote_endpoint(value: &str) -> Result<Option<(&str, &str)>> {
    if value.is_empty() {
        bail!("endpoint path must not be empty");
    }
    let windows_drive = |s: &str| {
        let b = s.as_bytes();
        b.len() >= 3 && b[0].is_ascii_alphabetic() && b[1] == b':'
            && matches!(b[2], b'/' | b'\\')
    };
    if windows_drive(value) || value.starts_with(r"\\") {
        return Ok(None);
    }
    let authority = value.split('/').next().unwrap();
    let Some((host, port)) = authority.rsplit_once(':') else { return Ok(None) };
    if host.is_empty() || host.chars().any(|c| c.is_whitespace() || c == '\\' || c == '@') {
        bail!("invalid remote host in {value:?}; use HOST:PORT/PATH");
    }
    if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit())
        || !matches!(port.parse::<u16>(), Ok(1..=65535))
    {
        bail!("invalid remote port in {value:?}; use HOST:PORT/PATH");
    }
    if host.contains([':', '[', ']']) {
        authority.parse::<std::net::SocketAddr>()
            .context("IPv6 endpoints must use [ADDRESS]:PORT/PATH")?;
    }
    let path = &value[authority.len()..];
    let tail = path.strip_prefix('/').context("remote endpoint needs a path: HOST:PORT/PATH")?;
    // The slash separates the authority from an absolute Windows path. For a
    // Unix path, that same slash is its filesystem root.
    let path = if windows_drive(tail) || tail.starts_with(r"\\") { tail } else { path };
    Ok(Some((authority, path)))
}

fn parse_endpoints<'a>(source: &'a str, dest: &'a str) -> Result<(&'a str, &'a str, &'a str, bool)> {
    match (remote_endpoint(source)?, remote_endpoint(dest)?) {
        (None, Some((host, path))) => Ok((source, path, host, false)),
        (Some((host, path)), None) => Ok((dest, path, host, true)),
        (None, None) => bail!("exactly one of --source and --dest must be HOST:PORT/PATH"),
        (Some(_), Some(_)) => bail!("one endpoint must be local; remote-to-remote sync is not supported"),
    }
}

fn run(args: impl Iterator<Item = String>) -> Result<()> {
    let mut args = args.peekable();
    let serving = args.peek().map(String::as_str) == Some("serve");
    if serving {
        args.next();
    }
    let (mut source, mut dest, mut root, mut listen, mut cache_dir) =
        (None, None, None, None, None);
    let mut token = if serving { String::new() } else {
        std::env::var("BBRSYNC_TOKEN").unwrap_or_default()
    };
    let mut opts = Opts::default();
    let mut ignore = Vec::new();
    while let Some(arg) = args.next() {
        let (key, inline) = match arg.split_once('=') {
            Some((key, value)) => (key, Some(value)),
            None => (arg.as_str(), None),
        };
        let mut value = || -> Result<String> {
            if let Some(value) = inline {
                return Ok(value.to_owned());
            }
            let value = args.next().with_context(|| format!("{key} needs a value"))?;
            if value.starts_with("--") {
                bail!("{key} needs a value");
            }
            Ok(value)
        };
        match (key, serving) {
            ("--source" | "--dest", false) | ("--root" | "--listen", true) => {
                let slot = match key {
                    "--source" => &mut source,
                    "--dest" => &mut dest,
                    "--root" => &mut root,
                    _ => &mut listen,
                };
                if slot.is_some() {
                    bail!("{key} was specified more than once");
                }
                let value = value()?;
                if value.is_empty() { bail!("{key} needs a value"); }
                *slot = Some(value);
            }
            ("--token", _) => token = value()?,
            ("--cache-dir", _) => {
                if cache_dir.is_some() {
                    bail!("--cache-dir was specified more than once");
                }
                let value = value()?;
                if value.is_empty() {
                    bail!("--cache-dir needs a value");
                }
                cache_dir = Some(value);
            }
            ("--ignore" | "-i", false) => ignore.push(value()?),
            ("--dry-run", false) if inline.is_none() => opts.dry = true,
            ("--full", false) if inline.is_none() => opts.full = true,
            ("--delete", false) if inline.is_none() => opts.delete = true,
            ("--version", _) if inline.is_none() => {
                println!("bbrsync {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            ("--license", _) if inline.is_none() => {
                print!("{}", include_str!("../LICENSE"));
                return Ok(());
            }
            ("-h" | "--help", _) if inline.is_none() => {
                println!(
                    "usage:\n  \
                     bbrsync --source=PATH --dest=PATH [--dry-run] [--delete] [--full] [--ignore PATTERN] [--cache-dir DIR] [--token TOKEN]\n  \
                     bbrsync serve --listen=ADDRESS:PORT [--root=DIR] [--cache-dir=DIR] [--token=TOKEN]\n\n\
                     --source is read from; --dest receives changes. Their order does not matter.\n\
                     Exactly one endpoint is remote: HOST:PORT/PATH. The other is a local directory.\n\n  \
                     bbrsync --source=website --dest=192.168.1.10:7777/srv/site\n  \
                     bbrsync --dest=website --source=192.168.1.10:7777/srv/site\n\n\
                     Windows drive paths such as C:\\Sites\\website are local. Quote paths containing spaces.\n\
                     --dry-run previews without writes. --delete also removes destination-only files.\n\
                     --cache-dir overrides this process's operating-system cache directory.\n\
                     Existing destination files can be overwritten even without --delete. Keep backups.\n\
                     Use only on a trusted local network or VPN; traffic is not encrypted.\n\
                     --version prints the version; --license prints the MIT license."
                );
                return Ok(());
            }
            _ => bail!("unexpected argument {arg:?}; see --help"),
        }
    }
    let cache_dir = match cache_dir
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("BBRSYNC_CACHE_DIR").filter(|path| !path.is_empty()).map(PathBuf::from))
    {
        Some(path) => std::path::absolute(path)?,
        None => default_cache_dir()?,
    };
    if serving {
        return serve(
            root.as_deref().map(Path::new),
            &listen.context("missing --listen=ADDRESS:PORT")?,
            &token,
            &cache_dir,
        );
    }
    let source = source.context("missing --source=PATH")?;
    let dest = dest.context("missing --dest=PATH")?;
    let (local, remote, host, pull) = parse_endpoints(&source, &dest)?;
    opts.patterns = load_patterns(Path::new(local), &ignore)?;
    sync(Path::new(local), remote, host, &token, &cache_dir, pull, &opts)
}

fn main() -> Result<()> {
    run(std::env::args().skip(1))
}

#[cfg(test)]
mod tests;
