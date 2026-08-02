# Durable POSIX I/O Foundations

> **Status: implemented primitive layer.** The `chronos::io` library provides the blocking Linux
> and macOS file, directory, synchronization, no-replace rename, and advisory-lock operations needed
> by the future WAL. It does not implement WAL naming, segment installation, append state,
> acknowledgment, recovery, or replay.

## Purpose and boundary

Durable storage code needs more than a successful `write`. It must know which bytes reached the
kernel, which synchronization operation covered them, and whether a namespace change crossed a
directory durability boundary. It must also preserve the distinction between a short transfer,
interruption, end of file, permanent I/O failure, and unsupported platform operation.

`chronos::io` is intentionally not a virtual filesystem. Its interface contains only the POSIX
mechanisms already required by the accepted WAL design:

- explicit-offset regular-file reads and complete writes;
- file size inspection and non-growing truncation;
- data and full-file synchronization;
- directory-relative regular-file open and exclusive creation;
- same-directory atomic no-replace rename and directory synchronization; and
- a nonblocking process-level whole-file advisory lock.

The library depends on `chronos::common` for byte views and explicit `Status`/`Result` failures. The
WAL codec remains independent from I/O; a future WAL writer will compose `chronos::wal` and
`chronos::io` rather than hiding storage inside the codec.

## Public interface and ownership

The public header is `chronos/io/posix_io.hpp`, exported by `chronos::io`.

`PosixDirectory`, `PosixFile`, and `PosixAdvisoryLock` are move-only RAII owners. Their integer file
descriptors are private and cannot be borrowed, duplicated, or used to perform an operation outside
the audited interface. Moving transfers ownership and invalidates the source. The classes are not
internally synchronized; callers must serialize conflicting operations and keep borrowed byte spans
alive for each call.

Destructors close best-effort because destructors have no error channel. Code for which close failure
is material calls the idempotent `close()` method and handles its `Status`. A handle is invalidated
before the close call, even on error, so a stale descriptor number can never be retried after the
kernel may have reused it.

`PosixAdvisoryLock` owns the lock descriptor until close or destruction. Traditional POSIX `fcntl`
record locks are process-associated: closing another descriptor for the same inode in the same
process can release the lock. The future WAL directory owner must therefore be the only code that
opens `LOCK` and must retain the returned lock object for its complete lifetime.

## Explicit-offset transfer semantics

`PosixFile::read_at` repeatedly calls `pread` at `offset + completed`. It retries `EINTR`, continues
after short reads, stops only when the destination is full or a zero-byte read reports EOF, and
returns the number of bytes transferred. EOF is not itself an error. If a hard error follows a
partial read, the returned status records the completed count and the destination retains that
prefix.

`PosixFile::write_all_at` similarly retries `EINTR` and short `pwrite` results until the source is
complete. A zero-byte write before completion is an I/O error rather than an infinite retry. A hard
failure after a prefix remains an error and reports its progress; the future WAL writer must poison
its append state rather than append another record after that prefix.

Before the first syscall, both operations prove that the starting offset and final addressed byte
fit signed `off_t`. Per-call lengths are capped at `SSIZE_MAX`, and each retry recomputes the checked
offset. Empty transfers validate their starting offset but perform no syscall.

These methods do not use or mutate a shared file position. Concurrent disjoint explicit-offset
operations may be meaningful to another subsystem, but the accepted WAL still has one serialized
logical writer.

## Files, directories, and names

The directory object opens its final path component with `O_DIRECTORY`, `O_CLOEXEC`, and
`O_NOFOLLOW`, then verifies the descriptor with `fstat`. Entry operations accept only a nonempty
basename: empty names, `.`, `..`, embedded NUL bytes, and `/` are rejected. They use `openat` against
the owned directory descriptor and verify opened entries are regular files. This avoids ambient
working-directory races and final-component symlink traversal.

Exclusive creation uses `O_CREAT | O_EXCL | O_NOFOLLOW`; it never opens or truncates a colliding
name. Permissions are limited to the ordinary `0777` permission bits and remain subject to the
process umask.

No-replace rename takes old and new basenames in one named request so they cannot be accidentally
swapped at a call site. Both resolve under the same directory descriptor. Linux uses
`renameat2(..., RENAME_NOREPLACE)` and macOS uses `renameatx_np(..., RENAME_EXCL)`. There is no
check-then-rename fallback: `ENOSYS`, `ENOTSUP`, `EOPNOTSUPP`, or the fixed-operation `EINVAL` case
returns `kNotSupported` rather than weakening atomicity.

## Synchronization and truncation

`sync_data()` uses `fdatasync` on Linux. Darwin does not expose `fdatasync` through its public libc
headers, so the macOS implementation uses the stronger-metadata `fsync` operation. `sync_all()` and
directory `sync()` use `fsync`. All retry `EINTR` and require a successful return.

This mapping does not strengthen the accepted platform contract: Linux remains the production
durability reference. macOS is a correctness-development platform, and the project still makes no
power-loss claim until its storage/device behavior and any need for `F_FULLFSYNC` are separately
qualified.

`truncate(new_size)` first obtains and validates the current regular-file size. It rejects growth,
converts the requested size to `off_t` with bounds checking, and retries `ftruncate` after `EINTR`.
It does not synchronize afterward; WAL tail repair must explicitly perform the required file and
directory synchronization in the accepted order.

## Failure behavior

Expected operating-system failures return `Status` or `Result`. `ENOENT` maps to `kNotFound`,
`EEXIST` to `kAlreadyExists`, unsupported-operation errors to `kNotSupported`, lock contention to
`kUnavailable`, invalid names/modes/types to `kInvalidArgument`, representability failures to
`kOutOfRange`, and remaining syscall failures to `kIoError`. Diagnostics name the failed operation,
retain numeric `errno`, and include transfer progress when relevant.

Every retryable operation captures `errno` immediately and retries only `EINTR`. `close` is the
deliberate exception: it is called once and never retried. On Linux an interrupted close may already
have released the descriptor, while retrying that numeric value can close an unrelated descriptor
that another thread has since opened. Other platforms differ, so invalidating ownership and
surfacing the close failure is the only safe common rule.

## Deterministic testing

Production calls a concrete syscall adapter. Unit tests substitute a narrow internal adapter whose
methods correspond one-for-one with the operations above. Scripted outcomes force `EINTR`, short
transfers, EOF, hard errors, zero progress, lock contention, unsupported rename, and close failure,
while recording descriptors, offsets, lengths, flags, and ordering. This seam is not installed as a
public filesystem interface.

Separate integration tests use the host filesystem to exercise exclusive creation, explicit-offset
overwrite/read, size, synchronization, non-growing truncation, no-replace rename, symlink rejection,
directory sync, and cross-process advisory-lock contention. These tests prove syscall integration on
the running host, not crash persistence or filesystem qualification.

## Complexity and tradeoffs

Transfer methods are `O(n)` in bytes and use `O(1)` auxiliary memory. Metadata, synchronization,
rename, open, close, and lock operations use constant userspace work plus their filesystem cost. No
operation allocates proportional to file size; path and diagnostic strings may allocate.

Blocking POSIX calls are the auditable reference path. Direct I/O, `mmap`, `io_uring`, asynchronous
completion, preallocation, a page cache, and a general filesystem abstraction are deliberately
absent. A future backend must preserve the same error and durability boundaries and requires
evidence before replacing this path.

## Likely interview questions

- Why must a full-write loop advance both buffer and explicit file offset after a short write?
- Why is a zero-byte `pwrite` treated differently from a zero-byte `pread`?
- Why must the complete offset range be checked before the first syscall?
- Why is `close` not retried after `EINTR`?
- What durability property does directory `fsync` add after rename?
- Why is check-then-rename not equivalent to atomic no-replace rename?
- How can closing an unrelated descriptor release a traditional POSIX record lock?
- Why does a passing macOS filesystem test not establish the Linux production durability contract?
