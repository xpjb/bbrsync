//! bbrsync: one-way, content-defined-chunk sync over a trusted connection.
//! Wire exchange:
//! receiver Files -> sender Diffs -> receiver Needs -> sender Chunks/Done -> receiver Ok.

mod transport;

use anyhow::{bail, Context, Result};
use bincode::Options as _;
use fastcdc::v2020::StreamCDC;
use flate2::{Compression, read::DeflateDecoder, write::DeflateEncoder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use transport::{Connection, Transport};
use std::time::UNIX_EPOCH;

const VERSION: u32 = 2;
const MIN_CHUNK: usize = 4096;
const AVG_CHUNK: usize = 16384;
const MAX_CHUNK: usize = 65536;
const TMP_SUFFIX: &str = ".bbrsync-tmp";
const PARTIAL_DIR: &str = ".bbrsync-partials";
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

struct Partial {
    slots: [PathBuf; 2],
    input: Option<(PathBuf, Vec<ChunkRef>)>,
    output: PathBuf,
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
    Chunk { compressed: bool, data: Vec<u8> },
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

struct Progress<'a> {
    out: &'a mut dyn Write,
    visible: bool,
    active: bool,
    index: usize,
    count: usize,
    path: String,
    done: u64,
    total: u64,
    started: std::time::Instant,
    last_draw: Option<std::time::Instant>,
    line_width: usize,
    line_visible: bool,
}

impl<'a> Progress<'a> {
    fn new(visible: bool, out: &'a mut dyn Write) -> Self {
        Self {
            out,
            visible,
            active: false,
            index: 0,
            count: 0,
            path: String::new(),
            done: 0,
            total: 0,
            started: std::time::Instant::now(),
            last_draw: None,
            line_width: 0,
            line_visible: false,
        }
    }

    fn status(&mut self, text: &str) {
        if !self.visible {
            return;
        }
        self.active = false;
        self.write_line(&format!("   {text}"), false);
    }

    fn begin_file(&mut self, index: usize, count: usize, path: &str, total: u64) {
        self.clear_line();
        self.active = self.visible && total > 0;
        self.index = index;
        self.count = count;
        self.path = if path.chars().count() <= 48 {
            path.to_owned()
        } else {
            let tail: String =
                path.chars().rev().take(47).collect::<String>().chars().rev().collect();
            format!("…{tail}")
        };
        self.done = 0;
        self.total = total;
        self.started = std::time::Instant::now();
        self.last_draw = None;
    }

    fn advance(&mut self, bytes: u64) {
        if !self.active {
            return;
        }
        self.done = self.done.saturating_add(bytes).min(self.total);
        let now = std::time::Instant::now();
        if self.done < self.total
            && now.duration_since(self.started) >= std::time::Duration::from_millis(250)
            && self.last_draw.map_or(true, |last| {
                now.duration_since(last) >= std::time::Duration::from_millis(200)
            })
        {
            self.render(now, false);
        }
    }

    fn finish_file(&mut self) {
        if !self.active {
            return;
        }
        let now = std::time::Instant::now();
        self.done = self.total;
        self.render(now, true);
        self.active = false;
    }

    fn render(&mut self, now: std::time::Instant, complete: bool) {
        let percent = (u128::from(self.done) * 100 / u128::from(self.total)) as u64;
        let elapsed = now.duration_since(self.started).as_secs_f64();
        let rate = if elapsed > 0.0 { (self.done as f64 / elapsed) as u64 } else { 0 };
        let line = format!(
            "   [{}/{}] {}  {:3}%  {}/{}  {}/s",
            self.index,
            self.count,
            self.path,
            percent,
            human_bytes(self.done),
            human_bytes(self.total),
            human_bytes(rate)
        );
        self.write_line(&line, complete);
        self.last_draw = Some(now);
    }

    fn clear(&mut self) {
        self.clear_line();
        self.active = false;
    }

    fn clear_line(&mut self) {
        if self.visible && self.line_visible {
            let _ = write!(self.out, "\r{:width$}\r", "", width = self.line_width);
            let _ = self.out.flush();
        }
        self.line_width = 0;
        self.line_visible = false;
    }

    fn write_line(&mut self, line: &str, complete: bool) {
        if !self.visible {
            return;
        }
        let width = line.chars().count();
        let padding = self.line_width.saturating_sub(width);
        let _ = write!(self.out, "\r{line}{:padding$}", "");
        if complete {
            let _ = writeln!(self.out);
            self.line_width = 0;
            self.line_visible = false;
        } else {
            self.line_width = width;
            self.line_visible = true;
        }
        let _ = self.out.flush();
    }
}

impl Drop for Progress<'_> {
    fn drop(&mut self) {
        if self.visible && self.line_visible {
            let _ = writeln!(self.out);
            let _ = self.out.flush();
        }
    }
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
        ["/bin", "/boot", "/dev", "/etc", "/lib", "/proc", "/sbin", "/sys", "/usr"]
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

fn partial_paths(root: &Path, rel: &str, content: &Hash) -> [PathBuf; 2] {
    let mut key = blake3::Hasher::new();
    key.update(b"bbrsync-partial-v1\0");
    key.update(rel.as_bytes());
    key.update(content);
    let key = key.finalize().to_hex();
    let dir = root.join(PARTIAL_DIR);
    [dir.join(format!("{key}.0")), dir.join(format!("{key}.1"))]
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

fn write_msg(w: &mut impl Write, msg: &Msg) -> Result<()> {
    let body = bincode::serialize(msg)?;
    w.write_all(&u32::try_from(body.len())?.to_le_bytes())?;
    w.write_all(&body)?;
    Ok(())
}

fn send(w: &mut impl Write, msg: &Msg) -> Result<()> {
    write_msg(w, msg)?;
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

fn chunks_index(chunks: &[ChunkRef]) -> HashMap<Hash, (u64, u64)> {
    chunks.iter().map(|chunk| (chunk.hash, (chunk.start, chunk.len))).collect()
}

fn chunk_index(entry: Option<&Entry>) -> HashMap<Hash, (u64, u64)> {
    entry.map_or_else(HashMap::new, |entry| chunks_index(&entry.chunks))
}

fn send_chunks(
    w: &mut impl Write,
    root: &Path,
    fresh: &Cache,
    needs: &[FileNeed],
    progress: &mut Progress<'_>,
) -> Result<()> {
    for (index, need) in needs.iter().enumerate() {
        let entry = fresh.get(&need.path).context("peer requested an unknown file")?;
        if need.send.len() != entry.chunks.len() {
            bail!("invalid chunk needs for {}", need.path);
        }
        let total = entry
            .chunks
            .iter()
            .zip(&need.send)
            .filter(|(_, want)| **want)
            .map(|(chunk, _)| chunk.len)
            .sum();
        progress.begin_file(index + 1, needs.len(), &need.path, total);
        if total == 0 {
            continue;
        }
        let mut f = BufReader::with_capacity(1 << 20, File::open(safe_join(root, &need.path)?)?);
        for (chunk, want) in entry.chunks.iter().zip(&need.send) {
            if *want {
                f.seek(SeekFrom::Start(chunk.start))?;
                let mut data = vec![0u8; chunk.len as usize];
                f.read_exact(&mut data)?;
                let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
                encoder.write_all(&data)?;
                let packed = encoder.finish()?;
                if packed.len() < data.len() {
                    write_msg(w, &Msg::Chunk { compressed: true, data: packed })?;
                } else {
                    write_msg(w, &Msg::Chunk { compressed: false, data })?;
                }
                progress.advance(chunk.len);
            }
        }
        progress.finish_file();
    }
    send(w, &Msg::Done)
}

/// Stage and verify every changed file, then publish. Reuse is determined
/// from the chunk index, not a second list of flags parallel to the manifest.
fn assemble(
    root: &Path,
    refs: &[FileRef],
    have: &Cache,
    partials: &HashMap<String, Partial>,
    stream: &mut impl Read,
    progress: &mut Progress<'_>,
) -> Result<(u64, u64, Vec<(String, Entry)>)> {
    let mut staged = Vec::new();
    let (mut received, mut written) = (0, 0);
    for (index, f) in refs.iter().enumerate() {
        let partial = partials.get(&f.path).context("missing partial transfer state")?;
        let local_at = chunk_index(have.get(&f.path));
        let partial_at = partial
            .input
            .as_ref()
            .map_or_else(HashMap::new, |(_, chunks)| chunks_index(chunks));
        let total = f
            .chunks
            .iter()
            .filter(|chunk| !local_at.contains_key(&chunk.hash) && !partial_at.contains_key(&chunk.hash))
            .map(|chunk| chunk.len)
            .sum();
        progress.begin_file(index + 1, refs.len(), &f.path, total);
        let dest = safe_join(root, &f.path)?;
        make_dirs(root, &dest)?;
        let mut tmp = OpenOptions::new().create(true).truncate(true).write(true).open(&partial.output)?;
        let permissions = match fs::metadata(&dest) {
            Ok(md) => Some(md.permissions()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        if let Some(permissions) = permissions {
            tmp.set_permissions(permissions)?;
        } else {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tmp.set_permissions(fs::Permissions::from_mode(0o644))?;
            }
        }
        let mut existing = if f.chunks.iter().any(|chunk| local_at.contains_key(&chunk.hash)) {
            Some(BufReader::with_capacity(1 << 20, File::open(&dest)?))
        } else {
            None
        };
        let mut saved = match &partial.input {
            Some((path, _)) if !partial_at.is_empty() => {
                Some(BufReader::with_capacity(1 << 20, File::open(path)?))
            }
            _ => None,
        };
        for chunk in &f.chunks {
            let (data, transferred) = if let Some((start, len)) = local_at.get(&chunk.hash) {
                let reader = existing.as_mut().context("missing local reader")?;
                reader.seek(SeekFrom::Start(*start))?;
                let mut data = vec![0u8; *len as usize];
                reader.read_exact(&mut data)?;
                (data, false)
            } else if let Some((start, len)) = partial_at.get(&chunk.hash) {
                let reader = saved.as_mut().context("missing partial reader")?;
                reader.seek(SeekFrom::Start(*start))?;
                let mut data = vec![0u8; *len as usize];
                reader.read_exact(&mut data)?;
                (data, false)
            } else {
                match recv(stream)? {
                    Msg::Chunk { compressed, data } => {
                        received += data.len() as u64;
                        let data = if compressed {
                            let mut unpacked = Vec::with_capacity(chunk.len as usize);
                            DeflateDecoder::new(data.as_slice())
                                .take(chunk.len + 1)
                                .read_to_end(&mut unpacked)?;
                            unpacked
                        } else {
                            data
                        };
                        (data, true)
                    }
                    _ => bail!("expected a chunk for {}", f.path),
                }
            };
            if data.len() as u64 != chunk.len || hash_of(&data) != chunk.hash {
                bail!("chunk mismatch in {}", f.path);
            }
            tmp.write_all(&data)?;
            written += data.len() as u64;
            if transferred {
                progress.advance(data.len() as u64);
            }
        }
        tmp.sync_all()?;
        drop(tmp);
        let (content, chunks) = describe(&partial.output)?;
        if content != f.content {
            bail!("content mismatch in {}", f.path);
        }
        progress.finish_file();
        staged.push((f.path.clone(), partial.output.clone(), partial.slots.clone(), content, chunks));
    }
    match recv(stream)? {
        Msg::Done => {}
        _ => bail!("expected the end of the chunk stream"),
    }
    let mut published = Vec::new();
    for (rel, tmp, slots, content, chunks) in staged {
        let dest = safe_join(root, &rel)?;
        tempfile::TempPath::try_from_path(tmp)?.persist(&dest)?;
        for slot in slots {
            match fs::remove_file(slot) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        let md = fs::metadata(dest)?;
        published.push((rel, Entry { size: md.len(), mtime: mtime_ns(&md), content, chunks }));
    }
    match fs::remove_dir(root.join(PARTIAL_DIR)) {
        Ok(()) => {}
        Err(e) if matches!(e.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty) => {}
        Err(e) => return Err(e.into()),
    }
    Ok((received, written, published))
}

fn send_tree(
    w: &mut impl Write,
    r: &mut impl Read,
    local: &Local,
    opts: &Opts,
    progress: &mut Progress<'_>,
) -> Result<Stats> {
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
        send_chunks(w, &local.root, &local.entries, &needs, progress)?;
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
    progress: &mut Progress<'_>,
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
        fs::create_dir_all(&local.root)?;
        let mut stack = vec![local.root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                let metadata = fs::symlink_metadata(entry.path())?;
                if metadata.is_file() && name.starts_with(".bbrsync-") && name.ends_with(TMP_SUFFIX) {
                    fs::remove_file(entry.path())?;
                } else if metadata.is_dir() && !is_link(&metadata) && name != PARTIAL_DIR {
                    stack.push(entry.path());
                }
            }
        }
        let partial_dir = local.root.join(PARTIAL_DIR);
        fs::create_dir_all(&partial_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&partial_dir, fs::Permissions::from_mode(0o700))?;
        }
        let expected: BTreeSet<_> = plan
            .refs
            .iter()
            .flat_map(|file| partial_paths(&local.root, &file.path, &file.content))
            .collect();
        for entry in fs::read_dir(&partial_dir)? {
            let entry = entry?;
            if !expected.contains(&entry.path()) {
                let metadata = fs::symlink_metadata(entry.path())?;
                if metadata.is_file() || is_link(&metadata) {
                    fs::remove_file(entry.path())?;
                }
            }
        }
        let mut partials = HashMap::new();
        for file in &plan.refs {
            let slots = partial_paths(&local.root, &file.path, &file.content);
            let wanted: HashMap<_, _> = file.chunks.iter().map(|chunk| (chunk.hash, chunk.len)).collect();
            let mut best = None;
            for (index, path) in slots.iter().enumerate() {
                match fs::metadata(path) {
                    Ok(metadata) if metadata.is_file() => match describe(path) {
                        Ok((_, chunks)) => {
                            let useful = chunks
                                .iter()
                                .filter(|chunk| wanted.get(&chunk.hash) == Some(&chunk.len))
                                .map(|chunk| chunk.len)
                                .sum::<u64>();
                            if useful > best.as_ref().map_or(0, |(_, useful, _)| *useful) {
                                best = Some((index, useful, chunks));
                            }
                        }
                        Err(_) => fs::remove_file(path)?,
                    },
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            let (input, output) = match best {
                Some((index, useful, chunks)) if useful > 0 => {
                    (Some((slots[index].clone(), chunks)), slots[1 - index].clone())
                }
                _ => (None, slots[0].clone()),
            };
            partials.insert(file.path.clone(), Partial { slots, input, output });
        }
        let needs = plan
            .refs
            .iter()
            .map(|file| {
                let mut held = chunk_index(local.entries.get(&file.path));
                if let Some((_, chunks)) = &partials[&file.path].input {
                    held.extend(chunks_index(chunks));
                }
                FileNeed {
                    path: file.path.clone(),
                    send: file.chunks.iter().map(|chunk| !held.contains_key(&chunk.hash)).collect(),
                }
            })
            .collect();
        send(w, &Msg::Needs(needs))?;
        let (received, assembled, published) =
            assemble(&local.root, &plan.refs, &local.entries, &partials, r, progress)?;
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
    progress: &mut Progress<'_>,
) -> Result<Stats> {
    let result = (|| {
        progress.status("scanning local files...");
        let mut local = Local::scan(root, cache_dir, opts, !sending)?;
        progress.status("comparing with peer...");
        let stats = if sending {
            send_tree(w, r, &local, opts, progress)?
        } else {
            receive_tree(w, r, &mut local, opts, progress)?
        };
        progress.clear();
        if !opts.dry {
            if let Err(e) = save_cache(&cache_path(cache_dir, &local.root), &local.entries) {
                eprintln!("warning: could not save hash cache: {e:#}");
            }
        }
        Ok(stats)
    })();
    progress.clear();
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
    transport: &dyn Transport,
    local: &Path,
    remote: &str,
    host: &str,
    token: &str,
    cache_dir: &Path,
    pull: bool,
    opts: &Opts,
    hide_progress: bool,
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
    let Connection { reader, writer } = transport.connect(host)?;
    let mut w = BufWriter::with_capacity(64 << 10, writer);
    let mut r = BufReader::with_capacity(64 << 10, reader);
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
    let stderr = std::io::stderr();
    let mut stderr = stderr.lock();
    let mut progress = Progress::new(!hide_progress && !opts.dry, &mut stderr);
    let stats = transfer(&mut w, &mut r, &local, cache_dir, !pull, opts, &mut progress)?;
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

fn session(connection: Connection, base: Option<&Path>, token: &str, cache_dir: &Path) -> Result<()> {
    let Connection { reader, writer } = connection;
    let mut w = BufWriter::with_capacity(64 << 10, writer);
    let mut r = BufReader::with_capacity(64 << 10, reader);
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
    let mut sink = std::io::sink();
    let mut progress = Progress::new(false, &mut sink);
    transfer(&mut w, &mut r, &root, cache_dir, pull, &opts, &mut progress)?;
    Ok(())
}

fn serve(transports: &[&dyn Transport], base: Option<&Path>, listen: &str, token: &str, cache_dir: &Path) -> Result<()> {
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
        Some(b) => eprintln!("bbrsync serving TCP and KCP on {listen}, confined to {}", b.display()),
        None => eprintln!("bbrsync serving TCP and KCP on {listen}"),
    }
    let incoming = transports.iter().map(|transport| transport.incoming(listen))
        .collect::<Result<Vec<_>>>()?;
    let (accepted, connections) = std::sync::mpsc::sync_channel(8);
    for (index, incoming) in incoming.into_iter().enumerate() {
        let accepted = accepted.clone();
        std::thread::Builder::new().name(format!("bbrsync-accept-{index}")).spawn(move || {
            for connection in incoming {
                if accepted.send(connection).is_err() {
                    break;
                }
            }
        }).context("starting transport listener")?;
    }
    drop(accepted);
    for connection in connections {
        match connection {
            Ok(connection) => {
                if let Err(e) = session(connection, base.as_deref(), token, cache_dir) {
                    eprintln!("session ended: {e:#}");
                }
            }
            Err(e) => eprintln!("accept failed: {e:#}"),
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
    let mut hide_progress = false;
    let mut kcp = false;
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
            ("--hide-progress", false) if inline.is_none() => hide_progress = true,
            ("--kcp", false) if inline.is_none() => kcp = true,
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
                     bbrsync --source=PATH --dest=PATH [--kcp] [--dry-run] [--delete] [--full] [--hide-progress] [--ignore PATTERN] [--cache-dir DIR] [--token TOKEN]\n  \
                     bbrsync serve --listen=ADDRESS:PORT [--root=DIR] [--cache-dir=DIR] [--token=TOKEN]\n\n\
                     --source is read from; --dest receives changes. Their order does not matter.\n\
                     Exactly one endpoint is remote: HOST:PORT/PATH. The other is a local directory.\n\n  \
                     bbrsync --source=website --dest=vibe:7777/srv/site\n  \
                     bbrsync --dest=website --source=vibe:7777/srv/site\n\n\
                     Windows drive paths such as C:\\Sites\\website are local. Quote paths containing spaces.\n\
                     --dry-run previews without writes. --delete also removes destination-only files.\n\
                     --cache-dir overrides this process's operating-system cache directory.\n\
                     --kcp uses the daemon's reliable UDP transport; TCP is the default.\n\
                     Transfers show progress on standard error; --hide-progress suppresses it.\n\
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
            &[&transport::tcp::TCP, &transport::kcp::KCP],
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
    let transport: &dyn Transport = if kcp { &transport::kcp::KCP } else { &transport::tcp::TCP };
    sync(
        transport,
        Path::new(local),
        remote,
        host,
        &token,
        &cache_dir,
        pull,
        &opts,
        hide_progress,
    )
}

fn main() -> Result<()> {
    run(std::env::args().skip(1))
}

#[cfg(test)]
mod tests;
