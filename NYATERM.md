# NyaTerm fork notes

This branch carries [NyaTerm](https://github.com/nyakang/nyaterm)'s local changes
to `russh-sftp` on top of an unmodified upstream base.

- Fork: <https://github.com/nyakang/russh-sftp>
- Upstream: <https://github.com/AspectUnk/russh-sftp>
- Base revision: `c2776c64c27e554dda0e0304925f890833fea5b1`
  (`russh-sftp` 3.0.0, upstream `master` on 2026-09-22)
- Branch: `nyaterm`

These changes prevent stalled writes and leaked server handles during uploads,
downloads, remote editing, cancellation and error cleanup, and let a caller
address remote paths that are not valid UTF-8.

## Patches

1. `feat: carry SFTP path fields as raw bytes` — protocol path fields become
   `Vec<u8>` via `serde_bytes`, with `_bytes` counterparts on the client APIs.
   The `String` APIs are unchanged and delegate through UTF-8. Server-side path
   packets are bridged back to the `Handler` string interface with lossy
   conversion. Handle bytes, file contents, and OpenSSH extension payload
   schemas are untouched.
2. `fix: make request and handle lifetimes self-cleaning` — `PendingRequest`
   owns its timeout and removes its own pending-map entry on timeout and on
   drop; a stream failure wakes every pending request through a `fail_pending`
   callback instead of leaving each to time out; a late reply to a timed-out
   request is logged instead of terminating the handler loop; `close` releases
   the handle on every outcome; dropped files close through the tracked
   `close_detached` instead of the untracked `close_nowait`; shutdown is
   idempotent and a stuck write acknowledgement surfaces as `TimedOut`.
3. `feat: expose positional reads, OpenSSH symlink order, and server limits` —
   `File::read_at`, `symlink_openssh` (OpenSSH swaps the symlink argument order
   relative to the draft this crate follows), and `SftpSession::limits` /
   `max_open_handles` / `effective_max_packet_len`.
4. `test: cover request, handle, and limit lifetimes`.

## Not carried here

- The NyaTerm snapshot repoints the `russh` dev-dependency to a sibling
  `vendor/russh` path and keeps a committed `Cargo.lock` (with a `!/Cargo.lock`
  ignore override). Both are vendor-layout concerns: this branch keeps
  upstream's `russh = "0.62.5"` dev-dependency and no lock file. A consumer that
  needs the patched russh should express that with a `[patch]` entry in its own
  workspace.

## Validation

```sh
cargo test --lib          # 16 passed
cargo check --all-targets
```

The 2026-09-22 merge to `c2776c64c2` conflicted in the client request,
file-I/O, runtime, and high-level session layers. The resolution adopts
upstream 3.0's `Request` future, creation-time deadlines, borrowed write
encoding, and pipelined reads/writes. It retains raw-byte path APIs, OpenSSH
symlink ordering, server-limit accessors, POSIX rename, explicit transport
failure wakeups, tracked detached close, release-on-all-outcomes handle
accounting, idempotent shutdown, and `TimedOut` I/O classification.


## Atomic replacement API (UI/UX migration)

Add `SftpSession::posix_rename_bytes`, gated on the advertised OpenSSH extension.
This lets NyaTerm replace a symlink without unlinking the existing path first.
Unsupported servers fail without mutation. Payload tests cover byte paths/order;
an unsupported-session regression checks failure without a protocol request.
Validation: Windows, `cargo test --lib`, `cargo fmt --all -- --check`.
