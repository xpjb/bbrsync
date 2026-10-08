# bbrsync

Chunk-based file synchronization for trusted local networks and VPNs.

## Before you use it

**This tool can lose your data.**

This is something I vibe-coded. I've tried to prevent data loss, but don't assume
I caught every bug. It can overwrite or delete files permanently. Keep backups,
test on disposable folders first, and inspect a dry run before syncing important
data. Don't use it with the only copy of something you care about.

Read the code, do your own research, and ask your AI to review it if that helps.
Neither an AI review nor a successful test guarantees safety.

## Network security

Use it only on a **trusted local network or VPN**. Don't expose it to the public
internet. Traffic is plain TCP: bbrsync does not provide encryption or TLS. The
optional token does not encrypt data.

A peer with access can read and write files with the daemon account's permissions,
within the configured `--root`. This is a trusted-peer tool, not a public service
or a sandbox against malicious peers or local processes.

## Usage

Start a daemon bound to its local-network or VPN address. `--listen` is required:

```sh
bbrsync serve --listen=192.168.1.10:7777 --root=/srv/site
```

`--root` limits access to that subtree. Without it, the daemon accepts absolute
paths accessible to its account, subject to destination safety checks. An
optional `--token=SECRET` must match the client's token.

**`--source` is read from. `--dest` receives the changes.** Their order does not
matter. Exactly one endpoint must be remote, written as `HOST:PORT/PATH`. The
other is a local directory. The contents of the source go directly into the
destination; the source directory's name is not appended.

```sh
# Send the local website directory to the daemon.
bbrsync --source=website --dest=192.168.1.10:7777/srv/site

# Copy it from the daemon into the local website directory.
bbrsync --dest=website --source=192.168.1.10:7777/srv/site

# Preview before writing anything.
bbrsync --source=website --dest=192.168.1.10:7777/srv/site --dry-run
```

Absolute Windows drive paths are local paths. Quote arguments containing spaces:

```powershell
.\bbrsync.exe --source="C:\My Sites\website" --dest=192.168.1.10:7777/srv/site
.\bbrsync.exe --dest="C:\My Sites\website" --source=192.168.1.10:7777/srv/site
```

For a daemon running on Windows, a remote endpoint can be
`HOST:PORT/C:/Sites/website`. IPv6 addresses use brackets:
`[fd00::1]:7777/srv/site`. Values can also be separated from their flags by a
space, such as `--source website`. Duplicate source or destination flags are
errors; they do not silently override an earlier path.

A missing source is an error. A missing destination is created on an actual run,
not on a dry run. `BBRSYNC_TOKEN` supplies the client's optional token default.

- **`--delete`:** also remove destination-only files. Off by default. Without it,
  destination-only files are untouched; same-named files can still be overwritten.
  This is a mirror/copy tool, not a versioned backup.
- **`--dry-run`:** report the plan without creating, overwriting, deleting, or
  updating caches on either side.
- **`--full`:** bypass hash caches and re-read file contents.
- **`-i/--ignore PATTERN`:** add an ignore rule, applied to both sides.

## Deletion safety

Before changing files, the receiver checks the transfer plan:

- Refuse deletion unless the command specified `--delete`.
- Refuse deletion from an empty source or of more than half the destination's
  scanned files, including small trees.
- Delete only paths in the destination's scanned manifest, after transferred
  files have been verified and published. Scan/transfer errors do not authorize
  deletion; deletion failures are reported.
- Reject traversal and existing symlink/junction components in file paths.
  Resolved roots are checked against `--root`; filesystem roots and Unix system
  trees such as `/etc`, `/usr`, and `/root` are refused as destinations.

Unrelated empty directories and old temporary files are left alone. These checks
protect against some mistakes, not every failure or a hostile local process
racing filesystem changes. They cannot make a wrong source/destination choice
harmless, especially when files are overwritten.

## Ignoring files

Rules come from the **client's local directory** `.bbrsyncignore`, then command
line `-i` rules. The resulting rules apply to both endpoints, including pulls.
Default exclusions are `.git`, `.hg`, `.svn`, `.DS_Store`, `Thumbs.db`, `*.swp`
and `*~`. bbrsync's own cache, ignore and temporary files are always excluded.

```text
node_modules/     # directories of this name
*.log             # matching names at any depth
/build            # at the root
!keep.log         # override an earlier rule
```

The last matching rule wins. `*`, `?` and `**` are supported; this is not a
complete gitignore implementation. Excluded directories aren't traversed.
Dotfiles such as `.env` are ordinary content unless explicitly ignored.

## Limits

FastCDC uses 4 / 16 / 64 KiB minimum/average/maximum chunks and BLAKE3 hashes. File
bytes are streamed; manifests and chunk indexes occupy memory proportional to
the tree. A `.bbrsync-cache` keyed by size and modification time avoids rehashing
unchanged files. Changes preserving both values require `--full`. Corrupt caches
are discarded and rebuilt.

Changed files are assembled in temporary files, verified, synced, then published
by rename. Existing destination permissions are preserved. Publication is atomic
**per file**, not across the whole tree: a failed run can leave some files updated
and others unchanged.

Regular UTF-8-named files are supported. Source symlinks are skipped; destination
link conflicts are refused. Empty directories, timestamps, ownership and source
permissions are not replicated. A rename is copy plus optional deletion.
Filesystem naming rules still apply: Windows cannot represent every Unix filename
or necessarily distinguish names differing only in case.

## License

MIT; see [LICENSE](LICENSE), or run `bbrsync --license`. The software is provided
without warranty. File corruption, overwrites and permanent data loss are
possible; the license's warranty and liability disclaimers apply.

## Build and checks

```sh
cargo build --release
cargo build --release --target x86_64-unknown-linux-musl
cargo xwin build --release --target x86_64-pc-windows-msvc
cargo nextest run
```

Wire protocol: **v1**. Tests use disposable directories and test-owned loopback
connections, never a live daemon or an existing data tree. Compiled binaries are
available in GitHub Releases.
