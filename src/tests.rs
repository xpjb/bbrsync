//! All filesystem tests use fresh disposable directories. The only TCP peer is
//! a test-owned loopback listener on an OS-assigned port; never a real daemon.
use super::*;
use std::time::Duration;

struct Tree {
    _scratch: tempfile::TempDir,
    base: PathBuf,
    local: PathBuf,
    remote: PathBuf,
}

impl Tree {
    fn new() -> Self {
        let scratch = tempfile::Builder::new().prefix("bbrsync-test-").tempdir().unwrap();
        let base = scratch.path().canonicalize().unwrap();
        let local = base.join("local");
        let remote = base.join("remote");
        fs::create_dir(&local).unwrap();
        fs::create_dir(&remote).unwrap();
        Self { _scratch: scratch, base, local, remote }
    }

    fn ends(&self, pull: bool) -> (&Path, &Path) {
        if pull {
            (&self.remote, &self.local)
        } else {
            (&self.local, &self.remote)
        }
    }

    fn run(&self, pull: bool, opts: &Opts) -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let host = listener.local_addr()?.to_string();
        let base = self.base.clone();
        let daemon = std::thread::spawn(move || {
            let (sock, _) = listener.accept()?;
            sock.set_read_timeout(Some(Duration::from_secs(5)))?;
            sock.set_write_timeout(Some(Duration::from_secs(5)))?;
            session(sock, Some(&base), "")
        });
        let result = sync(&self.local, self.remote.to_str().unwrap(), &host, "", pull, opts);
        let server = daemon.join().expect("test daemon panicked");
        result.and(server)
    }
}

fn options() -> Opts {
    Opts { patterns: DEFAULT_PATTERNS.into_iter().map(str::to_owned).collect(), ..Opts::default() }
}

fn put(root: &Path, name: &str, data: impl AsRef<[u8]>) {
    let path = root.join(name);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, data).unwrap();
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let path = e.path();
            assert!(!e.file_type().unwrap().is_symlink());
            let key = path.strip_prefix(root).unwrap().to_path_buf();
            if e.file_type().unwrap().is_dir() {
                out.insert(key, Vec::new());
                stack.push(path);
            } else {
                out.insert(key, fs::read(path).unwrap());
            }
        }
    }
    out
}

fn one_file(path: &str, data: &[u8]) -> FileRef {
    let chunk = ChunkRef { hash: hash_of(data), start: 0, len: data.len() as u64 };
    FileRef { path: path.into(), content: hash_of(data), chunks: vec![chunk] }
}

fn wire(messages: Vec<Msg>) -> std::io::Cursor<Vec<u8>> {
    let mut bytes = Vec::new();
    for msg in messages {
        send(&mut bytes, &msg).unwrap();
    }
    std::io::Cursor::new(bytes)
}

#[test]
fn deletion_is_opt_in_in_both_directions() {
    for pull in [false, true] {
        for delete in [false, true] {
            let tree = Tree::new();
            let (src, dst) = tree.ends(pull);
            put(src, "keep", b"new");
            put(dst, "keep", b"old");
            put(dst, "local-only", b"irreplaceable");
            let opts = Opts { delete, ..options() };
            tree.run(pull, &opts).unwrap();
            assert_eq!(fs::read(dst.join("keep")).unwrap(), b"new");
            assert_eq!(dst.join("local-only").exists(), !delete);
        }
    }
}

#[test]
fn dry_run_does_not_modify_either_tree() {
    for pull in [false, true] {
        let tree = Tree::new();
        let (src, dst) = tree.ends(pull);
        put(src, "keep", b"new");
        put(src, "new-directory/new-file", b"new");
        put(dst, "keep", b"old");
        put(dst, "local-only", b"precious");
        put(dst, "old.bbrsync-tmp", b"not ours to remove");
        put(dst, CACHE_FILE, b"a cache must not be rewritten by dry-run");
        let before_src = snapshot(src);
        let before_dst = snapshot(dst);
        tree.run(pull, &Opts { dry: true, delete: true, ..options() }).unwrap();
        assert_eq!(snapshot(src), before_src);
        assert_eq!(snapshot(dst), before_dst);
    }
}

#[test]
fn missing_destinations_are_not_created_by_dry_run() {
    for pull in [false, true] {
        let mut tree = Tree::new();
        if pull {
            tree.local = tree.base.join("new-local");
        } else {
            tree.remote = tree.base.join("new-remote");
        }
        let (src, dst) = tree.ends(pull);
        put(src, "file", b"data");
        tree.run(pull, &Opts { dry: true, ..options() }).unwrap();
        assert!(!dst.exists());
        tree.run(pull, &options()).unwrap();
        assert_eq!(fs::read(dst.join("file")).unwrap(), b"data");
    }
}

#[test]
fn empty_source_is_refused_before_any_mutation() {
    for pull in [false, true] {
        let tree = Tree::new();
        let (_, dst) = tree.ends(pull);
        put(dst, "keep", b"precious");
        let before = snapshot(dst);
        let err = tree.run(pull, &Opts { delete: true, ..options() }).unwrap_err();
        assert!(format!("{err:#}").contains("refusing to delete"));
        assert_eq!(snapshot(dst), before);
    }
}

#[test]
fn mass_delete_guard_runs_before_overwrites_in_both_directions() {
    for pull in [false, true] {
        for held in [2, 6] {
            let tree = Tree::new();
            let (src, dst) = tree.ends(pull);
            put(src, "keep", b"would overwrite");
            put(src, "new-file", b"must not appear");
            put(dst, "keep", b"precious original");
            for i in 0..held {
                put(dst, &format!("extra-{i}"), b"precious");
            }
            let before = snapshot(dst);
            let err = tree.run(pull, &Opts { delete: true, ..options() }).unwrap_err();
            assert!(format!("{err:#}").contains("refusing to delete"));
            assert_eq!(snapshot(dst), before);
        }
    }
}

#[test]
fn ignored_paths_are_untouched_in_both_directions() {
    for pull in [false, true] {
        let tree = Tree::new();
        let (src, dst) = tree.ends(pull);
        put(src, "keep", b"new");
        put(dst, "keep", b"old");
        put(dst, "extra", b"delete this");
        put(src, "ignored/data", b"must not overwrite");
        put(dst, "ignored/data", b"private");
        put(src, "ignored/new", b"must not create");
        put(dst, "ignored/local", b"must not delete");
        let mut opts = Opts { delete: true, ..options() };
        opts.patterns.push("ignored/".into());
        tree.run(pull, &opts).unwrap();
        assert_eq!(fs::read(dst.join("keep")).unwrap(), b"new");
        assert!(!dst.join("extra").exists());
        assert_eq!(fs::read(dst.join("ignored/data")).unwrap(), b"private");
        assert_eq!(fs::read(dst.join("ignored/local")).unwrap(), b"must not delete");
        assert!(!dst.join("ignored/new").exists());
    }
}

#[test]
fn no_global_temp_or_empty_directory_sweep() {
    let tree = Tree::new();
    put(&tree.local, "keep", b"new");
    put(&tree.remote, "keep", b"old");
    put(&tree.remote, "extra", b"delete this");
    put(&tree.remote, "old.bbrsync-tmp", b"not ours");
    fs::create_dir_all(tree.remote.join("empty/valuable-structure")).unwrap();
    fs::create_dir_all(tree.remote.join(".git/empty")).unwrap();
    tree.run(false, &Opts { delete: true, ..options() }).unwrap();
    assert_eq!(fs::read(tree.remote.join("old.bbrsync-tmp")).unwrap(), b"not ours");
    assert!(tree.remote.join("empty/valuable-structure").is_dir());
    assert!(tree.remote.join(".git/empty").is_dir());
}

#[test]
fn files_composed_only_of_reused_chunks_are_still_rebuilt() {
    for pull in [false, true] {
        let tree = Tree::new();
        let (src, dst) = tree.ends(pull);
        let old = vec![b'x'; MAX_CHUNK * 3];
        put(dst, "file", &old);
        let (_, chunks) = describe(&dst.join("file")).unwrap();
        let new = &old[..chunks[0].len as usize];
        put(src, "file", new);
        let source = Local::scan(src, &options(), false).unwrap();
        let target = Local::scan(dst, &options(), true).unwrap();
        let entry = &source.entries["file"];
        let held = chunk_index(target.entries.get("file"));
        assert_ne!(entry.content, target.entries["file"].content);
        assert!(entry.chunks.iter().all(|c| held.contains_key(&c.hash)));
        tree.run(pull, &options()).unwrap();
        assert_eq!(fs::read(dst.join("file")).unwrap(), new);
    }
}

#[test]
fn empty_files_are_written() {
    for pull in [false, true] {
        let tree = Tree::new();
        let (src, dst) = tree.ends(pull);
        put(src, "new-empty", b"");
        put(src, "truncate", b"");
        put(dst, "truncate", b"old");
        tree.run(pull, &options()).unwrap();
        assert_eq!(fs::read(dst.join("new-empty")).unwrap(), b"");
        assert_eq!(fs::read(dst.join("truncate")).unwrap(), b"");
    }
}

#[test]
fn unchanged_source_does_not_hide_destination_changes() {
    for pull in [false, true] {
        let tree = Tree::new();
        let (src, dst) = tree.ends(pull);
        put(src, "file", b"source");
        tree.run(pull, &options()).unwrap();
        put(dst, "file", b"destination changed independently");
        tree.run(pull, &options()).unwrap();
        assert_eq!(fs::read(dst.join("file")).unwrap(), b"source");
        // A no-op still completes the protocol, rather than deadlocking.
        tree.run(pull, &options()).unwrap();
    }
}

#[test]
fn full_rehashes_both_peers() {
    for pull in [false, true] {
        for poison_source in [false, true] {
            let tree = Tree::new();
            let (src, dst) = tree.ends(pull);
            put(src, "file", b"source");
            put(dst, "file", b"target");
            let (bad, other) = if poison_source { (src, dst) } else { (dst, src) };
            let mut cache = Local::scan(bad, &options(), false).unwrap().entries;
            let (content, chunks) = describe(&other.join("file")).unwrap();
            let entry = cache.get_mut("file").unwrap();
            entry.content = content;
            entry.chunks = chunks;
            save_cache(bad, &cache).unwrap();
            tree.run(pull, &Opts { full: true, ..options() }).unwrap();
            assert_eq!(fs::read(dst.join("file")).unwrap(), b"source");
        }
    }
}

#[test]
fn receiver_rejects_unrequested_deletion_before_writing() {
    let tree = Tree::new();
    put(&tree.local, "keep", b"old");
    put(&tree.local, "extra", b"precious");
    let before = snapshot(&tree.local);
    let mut input = wire(vec![Msg::Diffs(Plan {
        refs: vec![one_file("keep", b"new")],
        deletes: vec!["extra".into()],
        total: 1,
    })]);
    let err = transfer(&mut Vec::new(), &mut input, &tree.local, false, &options()).unwrap_err();
    assert!(format!("{err:#}").contains("without --delete"));
    assert_eq!(snapshot(&tree.local), before);
}

#[test]
fn receiver_rejects_unscanned_or_duplicate_delete_paths() {
    let tree = Tree::new();
    for i in 0..6 {
        put(&tree.local, &format!("file-{i}"), b"keep");
    }
    let local = Local::scan(&tree.local, &options(), true).unwrap();
    for paths in
        [vec![".git/config"], vec!["file-0", "file-0"], vec!["../outside"], vec![CACHE_FILE]]
    {
        let plan = Plan {
            refs: vec![],
            deletes: paths.into_iter().map(str::to_owned).collect(),
            total: 6,
        };
        assert!(plan.check(&local, true).is_err());
    }
    let plan =
        Plan { refs: vec![one_file("file-0", b"new")], deletes: vec!["file-0".into()], total: 6 };
    assert!(plan.check(&local, true).is_err());
}

#[test]
fn failed_transfer_does_not_publish_or_delete() {
    for fail_hash in [false, true] {
        let tree = Tree::new();
        put(&tree.local, "a", b"old a");
        put(&tree.local, "b", b"old b");
        put(&tree.local, "extra", b"precious");
        let before = snapshot(&tree.local);
        let mut refs = vec![one_file("a", b"new a"), one_file("b", b"new b")];
        if fail_hash {
            refs[1].content = [0; 32];
        }
        let mut messages = vec![
            Msg::Diffs(Plan { refs, deletes: vec!["extra".into()], total: 2 }),
            Msg::Chunk(b"new a".to_vec()),
        ];
        if fail_hash {
            messages.push(Msg::Chunk(b"new b".to_vec()));
            messages.push(Msg::Done);
        } // Otherwise the stream ends halfway through the transfer.
        let mut input = wire(messages);
        assert!(transfer(
            &mut Vec::new(),
            &mut input,
            &tree.local,
            false,
            &Opts { delete: true, ..options() }
        )
        .is_err());
        assert_eq!(snapshot(&tree.local), before);
    }
}

#[test]
fn source_scan_error_cannot_turn_into_deletion() {
    for pull in [false, true] {
        let tree = Tree::new();
        let (src, dst) = tree.ends(pull);
        put(src, "file", b"source");
        put(dst, "keep", b"precious");
        // A non-directory source is an error, never an empty manifest.
        let before = snapshot(dst);
        assert!(Local::scan(&src.join("file"), &options(), false).is_err());
        assert!(Local::scan(&src.join("missing"), &options(), false).is_err());
        assert_eq!(snapshot(dst), before);
    }
}

#[test]
fn operand_order_is_the_only_direction_switch() {
    assert_eq!(parse_operands("local", ":/remote").unwrap(), ("local", "/remote", false));
    assert_eq!(parse_operands(":/remote", "local").unwrap(), ("local", "/remote", true));
    assert!(parse_operands("local", "remote").is_err());
    assert!(parse_operands(":a", ":b").is_err());
}

#[cfg(unix)]
#[test]
fn destination_symlinks_do_not_overwrite_outside_the_tree() {
    use std::os::unix::fs::symlink;
    for pull in [false, true] {
        let tree = Tree::new();
        let (src, dst) = tree.ends(pull);
        let outside = tree.base.join("outside");
        put(&outside, "file", b"precious");
        symlink(&outside, dst.join("link")).unwrap();
        put(src, "link/file", b"must not overwrite");
        assert!(tree.run(pull, &options()).is_err());
        assert_eq!(fs::read(outside.join("file")).unwrap(), b"precious");
        assert!(!dst.join(CACHE_FILE).exists());
    }
}

#[cfg(unix)]
#[test]
fn confinement_checks_resolved_roots_and_relative_destinations() {
    use std::os::unix::fs::symlink;
    let tree = Tree::new();
    symlink(&tree.remote, tree.local.join("escape")).unwrap();
    assert!(resolve_target(Some(&tree.local), "escape").is_err());
    assert!(resolve_target(Some(&tree.local), "escape/not-created").is_err());
    // Pure validation only: no system directory is scanned or modified.
    assert!(resolve_target(Some(Path::new("/")), ".").is_err());
    assert!(resolve_target(Some(Path::new("/")), "etc").is_err());
    assert!(resolve_target(None, "/").is_err());
}

#[cfg(unix)]
#[test]
fn existing_permissions_survive_content_updates() {
    use std::os::unix::fs::PermissionsExt;
    for mode in [0o600, 0o755] {
        let tree = Tree::new();
        put(&tree.local, "file", b"new");
        put(&tree.remote, "file", b"old");
        fs::set_permissions(tree.remote.join("file"), fs::Permissions::from_mode(mode)).unwrap();
        tree.run(false, &options()).unwrap();
        assert_eq!(
            fs::metadata(tree.remote.join("file")).unwrap().permissions().mode() & 0o777,
            mode
        );
    }
}

#[cfg(unix)]
#[test]
fn cache_temp_symlink_is_not_followed_or_removed() {
    use std::os::unix::fs::symlink;
    let tree = Tree::new();
    put(&tree.base, "outside", b"precious");
    symlink(tree.base.join("outside"), tree.local.join(format!("{CACHE_FILE}.new"))).unwrap();
    put(&tree.local, "file", b"data");
    tree.run(false, &options()).unwrap();
    assert_eq!(fs::read(tree.base.join("outside")).unwrap(), b"precious");
    assert!(tree.local.join(format!("{CACHE_FILE}.new")).is_symlink());
}

#[test]
fn mixed_reused_and_received_chunks_follow_manifest_order() {
    for pull in [false, true] {
        let tree = Tree::new();
        let (src, dst) = tree.ends(pull);
        let old: Vec<u8> = (0..MAX_CHUNK * 4).map(|i| (i % 251) as u8).collect();
        let mut new = old.clone();
        new[MAX_CHUNK + 123] ^= 0xff;
        put(src, "file", &new);
        put(dst, "file", &old);
        let source = Local::scan(src, &options(), false).unwrap();
        let target = Local::scan(dst, &options(), true).unwrap();
        let held = chunk_index(target.entries.get("file"));
        let chunks = &source.entries["file"].chunks;
        assert!(chunks.iter().any(|c| held.contains_key(&c.hash)));
        assert!(chunks.iter().any(|c| !held.contains_key(&c.hash)));
        tree.run(pull, &options()).unwrap();
        assert_eq!(fs::read(dst.join("file")).unwrap(), new);
    }
}

#[test]
fn wrong_chunk_order_or_extra_chunks_cannot_publish_or_delete() {
    for data in [vec![b"new b", b"new a"], vec![b"new a", b"new b", b"extra"]] {
        let tree = Tree::new();
        put(&tree.local, "a", b"old a");
        put(&tree.local, "b", b"old b");
        put(&tree.local, "extra", b"precious");
        let before = snapshot(&tree.local);
        let mut messages = vec![Msg::Diffs(Plan {
            refs: vec![one_file("a", b"new a"), one_file("b", b"new b")],
            deletes: vec!["extra".into()],
            total: 2,
        })];
        messages.extend(data.into_iter().map(|d| Msg::Chunk(d.to_vec())));
        messages.push(Msg::Done);
        let mut input = wire(messages);
        assert!(transfer(
            &mut Vec::new(),
            &mut input,
            &tree.local,
            false,
            &Opts { delete: true, ..options() }
        )
        .is_err());
        assert_eq!(snapshot(&tree.local), before);
    }
}

#[test]
fn file_hashes_are_plain_blake3_and_empty_files_need_no_chunks() {
    let tree = Tree::new();
    for data in [vec![], b"small file".to_vec(), vec![42; MAX_CHUNK * 3]] {
        put(&tree.local, "file", &data);
        let (content, chunks) = describe(&tree.local.join("file")).unwrap();
        assert_eq!(content, hash_of(&data));
        assert_eq!(chunks.is_empty(), data.is_empty());
        assert_eq!(chunks.iter().map(|c| c.len).sum::<u64>(), data.len() as u64);
    }
}

#[test]
fn ignore_rules_preserve_anchoring_directory_only_and_last_match() {
    let lines =
        ["# comment", "", "*.log", "!keep.log", "/build", "node_modules/", "assets/**/secret"]
            .map(str::to_owned);
    let pats = Patterns::parse(&lines);
    for (path, is_dir, ignored) in [
        ("logs/error.log", false, true),
        ("logs/keep.log", false, false),
        ("build", true, true),
        ("sub/build", true, false),
        ("project/node_modules", true, true),
        ("node_modules", false, false),
        ("assets/secret", false, true),
        ("assets/a/b/secret", false, true),
        ("assets/ordinary", false, false),
    ] {
        assert_eq!(pats.ignored(path, is_dir), ignored, "{path}");
    }
}
