# bbrsync

One-way file sync over one TCP connection, using content-defined chunks to send
only changed bytes. The same sender and receiver implement both push and pull.

## Usage

Run the daemon on your **Tailscale address** (or another trusted private network):

```sh
bbrsync serve --listen 100.x.y.z:7777 [--root DIR] [--token SECRET]
```

`--root`, when supplied, limits daemon paths to that subtree. Otherwise the
client must specify an absolute daemon path. This is a trusted-peer tool, not a
public service: it has no TLS, and an authorized peer has the daemon account's
file access within the selected tree. The optional token does not encrypt data.

Exactly one operand has a leading colon, identifying the daemon-side path:

```sh
bbrsync ./site :/srv/site --host 100.x.y.z:7777          # push
bbrsync :/srv/site ./site --host 100.x.y.z:7777          # pull
bbrsync ./site :/srv/site --host host:7777 --dry-run
bbrsync ./site :/srv/site --host host:7777 --delete
```

The contents of the source directory go directly into the destination directory.
A missing source is an error. A missing destination is created on an actual run,
not on a dry run. `BBRSYNC_HOST` and `BBRSYNC_TOKEN` supply client defaults.

- **`--delete`:** also remove destination-only files. Off by default in **both**
  directions. Without it, destination-only files are untouched; same-named files
  can still be overwritten. This is a mirror/copy tool, not a versioned backup.
- **`--dry-run`:** report the plan without creating, overwriting, deleting, or
  updating caches on either side.
- **`--full`:** bypass both peers' hash caches and re-read file contents.
- **`-i/--ignore PATTERN`:** add an ignore rule, applied to both sides.

## Deletion safety

One common receiver checks the entire plan before changing files:

- Refuse a deletion unless the command specified `--delete`.
- Refuse deletion from an empty source or of more than half the destination's
  scanned files, including small trees.
- Delete only paths in the destination's scanned manifest, and only after all
  transferred files have been verified and published. Scan/transfer errors do
  not authorize deletion; deletion failures are reported, not silently ignored.
- Reject traversal and existing symlink/junction components in file paths.
  Resolved roots are checked against `--root`; filesystem roots and Unix system
  trees such as `/etc`, `/usr`, and `/root` are refused as destinations.

There is no recursive cleanup sweep: unrelated empty directories and old temp
files are left alone. These checks protect against mistakes, not a hostile local
process racing filesystem changes. They cannot make an intentionally wrong
source/destination choice harmless, especially when files are overwritten.

## Ignoring files

Rules come from the **client's local directory** `.bbrsyncignore`, then command
line `-i` rules; the same effective list is used at both ends, including pulls.
Default exclusions are `.git`, `.hg`, `.svn`, `.DS_Store`, `Thumbs.db`, `*.swp`
and `*~`. bbrsync's own state, ignore and temporary files are always excluded.

```text
node_modules/     # directories of this name
*.log             # matching names at any depth
/build            # at the root
!keep.log         # override an earlier rule
```

The last matching rule wins. `*`, `?` and `**` are supported; this is a small glob
syntax, not a complete gitignore implementation. Excluded directories aren't
traversed. Dotfiles such as `.env` are ordinary content unless explicitly ignored.

## Design and limits

```text
receiver Files -> sender Diffs -> receiver Needs -> sender Chunks/Done -> receiver Ok
```

A dry run stops after the plan is checked and acknowledged. Every real run
compares both manifests; unchanged local files do not prove the remote is current.

FastCDC uses 4 / 16 / 64 KiB min/average/max chunks and BLAKE3 hashes. File bytes
are streamed; manifests and chunk indexes take memory proportional to the tree.
A `.bbrsync-cache` keyed by `(size, mtime)` avoids rehashing unchanged files.
Changes preserving both values require `--full`. Corrupt caches are discarded
and rebuilt.

Changed files are assembled in uniquely created temporary files, including files
whose chunks can all be reused. Every temp is verified and synced before any is
published by rename. Existing destination permissions are preserved. Regular
failures clean up this run's temps; a hard kill can leave ignored temp files.
Publication is atomic **per file**, not across the whole tree.

Regular UTF-8-named files are supported. Source symlinks are skipped; destination
link conflicts are refused. Empty directories, timestamps, ownership and source
permissions are not replicated. A rename is copy plus optional deletion.

## Build and checks

```sh
cargo build --release
cargo build --release --target x86_64-unknown-linux-musl
cargo xwin build --release --target x86_64-pc-windows-msvc
cargo nextest run
```

Wire protocol: **v1**. Tests use fresh disposable directories and test-owned
loopback connections, never a live daemon or an existing data tree. Compiled
binaries belong in release downloads, not in the source repository.
