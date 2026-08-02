# WAL Recovery Implementation

> **Status: implemented physical recovery layer.** The authoritative bytes and classifications are
> defined by [WAL v1](../formats/wal-v1.md); lifecycle and durability requirements are defined by
> the [recovery architecture](../architecture/wal-recovery.md). This document explains the current
> C++ implementation without redefining either contract.

## Purpose and public interfaces

Recovery turns an existing WAL directory into either a proved physical history or one precise
failure. The public interfaces in `chronos::wal` separate observation, mutation, and continued
writing:

- `scan_wal(path)` acquires the existing `LOCK`, performs one read-only physical verification pass,
  and returns a `WalRecoveryReport`. It never creates or removes an entry.
- `inspect_wal(path, sink)` is also locked and read-only. For a clean history it performs whole-log
  preflight and then deterministic replay into the sink; an incomplete final tail is reported
  without callbacks.
- `recover_wal(config, options, sink)` performs writer-startup recovery and closes the recovered
  handles instead of returning a writer.
- `WalWriter::open_existing(config, options, sink)` performs the same recovery and transfers the
  directory, lock, and active file into a writer positioned at the verified end.
- `chronos-waldump` composes `inspect_wal` with a sink that accepts every structurally decoded
  physical type and prints metadata, not payload bytes.

`WalRecoveryOptions::repair_incomplete_final_tail` defaults to false. A mutable recovery call
therefore needs explicit authorization before truncating even the one repairable classification.

## Discovery and ownership

Every authoritative operation opens the dedicated WAL directory without following its final path
component and acquires the process-level exclusive advisory lock before discovery. Read-only paths
require an existing regular `LOCK`; writer recovery may create it. This distinction prevents an
inspection attempt from changing a missing-history directory. The lock remains held across all scan
passes, repair, cleanup, replay, startup synchronization, and transfer into a reopened writer.

Discovery snapshots and sorts directory entries. It accepts only:

- one regular `LOCK`;
- exact final segment names whose numeric sequence starts at 1 and has no gap; and
- exact regular temporary segment names from the reserved format namespace.

Malformed reserved names, unrelated entries, symlinks, special files, a missing lock, and a missing
final segment fail closed. Temporary files are counted but never treated as history. Mutable
recovery removes recognized temporaries only after the named WAL has verified (and any authorized
repair has completed), tolerates an already absent entry, and synchronizes the directory after a
cleanup batch. Read-only paths leave them untouched.

## Physical verification

The scanner processes final segments in numeric order. It verifies exact file limits and every
segment header before records, then enforces:

- a common nonzero WAL identity;
- filename/header segment-number agreement;
- segment and first-record sequences beginning at 1 and remaining contiguous;
- a nonempty record set in every closed segment;
- record sequence continuity across all files;
- complete record framing and CRC32C before any callback; and
- terminal `UINT64_MAX` record sequence only at physical end of the final segment.

The scanner reads one fixed header and allocates at most one format-bounded encoded record at a
time. It does not retain payload history. Directory discovery storage is proportional to the number
of entries, while record processing memory is `O(maximum record length)` and time is `O(total WAL
bytes)`.

Only two physical EOF shapes in the numerically final segment produce
`kIncompleteFinalTail`: fewer than 40 bytes remain after a verified record boundary, or a complete
checksum-valid record header declares a total length beyond EOF. The report names the last verified
physical position and observed size. The same shapes in a closed segment are corruption. A complete
record with an invalid header, padding, or full-record checksum is always corruption, including in
the final segment.

## Verification, preflight, and replay passes

Recovery deliberately rereads the WAL:

1. a verification pass proves every physical byte and records the exact history summary;
2. a preflight pass re-verifies each record and asks the sink whether every physical/application
   semantic is supported without publishing state;
3. a replay pass re-verifies again and invokes `replay` in segment/offset/record-sequence order; and
4. mutable recovery performs a final verification before enabling the writer.

Each later pass must produce the same verified history summary as the preceding pass. This check
turns unexpected out-of-band changes into failure rather than allowing callbacks over a different
history. The advisory lock excludes cooperating writers; deployment isolation is still required
because POSIX advisory locks cannot stop an uncooperative process.

`WalReplayRecord` owns decoded header and position values but borrows its payload only for the
callback duration. `preflight` must not publish logical state. The implementation cannot roll back a
sink whose `replay` method fails, so the caller must replay into fresh or resettable state and discard
that state on failure. Application-kind semantics remain outside this physical layer.

## Repair and startup durability

Authorized repair rechecks the active file size, header, identity, and segment number immediately
before mutation. It truncates only to the reported verified end, calls full-file synchronization,
synchronizes the WAL directory, and verifies the complete history again. Repeating recovery over
the repaired bytes is clean and performs no further truncation.

Before returning recovered state, writer recovery opens the final segment read-write, rechecks its
size and identity, performs a conservative full-file synchronization, and synchronizes the WAL
directory. This startup barrier covers a surviving rename and any process-crash page-cache bytes or
completed repair before records can become acknowledgment-eligible. The recovered written and
durable positions are both the verified physical end.

`open_existing` preserves the existing WAL identity, final segment, record sequence, and physical
offset. If a configured runtime rotation target is smaller than that already-valid final file, the
next append rotates before writing; it never subtracts unchecked lengths from the existing end.
Sequence exhaustion remains terminal.

## Failure behavior and idempotence

Expected outcomes remain distinct:

- missing directory, `LOCK`, or final history is `NotFound`;
- lock contention is `Unavailable`;
- incomplete final tail without repair authorization is `OutOfRange` on mutable recovery;
- contradictory framing, checksum failure, gaps, mixed identities, or forbidden tails are
  `Corruption`;
- unknown required semantics may be rejected by the sink with `NotSupported`; and
- failed reads, truncation, synchronization, cleanup, or close operations retain their I/O status
  and context.

No repair or cleanup occurs before named-history verification. A failed synchronization after
truncation does not claim success; the next authorized recovery reclassifies the resulting bytes and
repeats the required barrier. Temporary cleanup is similarly convergent. Logical replay idempotence
is a sink/application obligation because physical recovery does not define mutation identities yet.

## Testing and remaining evidence

Named suites cover discovery and reserved names, active-lock rejection, clean and bounded scans,
both permitted incomplete-tail shapes, complete-record corruption, segment/record/identity gaps,
preflight-before-replay, deterministic cross-segment replay, explicit and repeated repair, injected
sync failure, temporary cleanup, exact reopen state, subsequent rotation, writer lock lifetime, and
the inspector's output and exit codes. Public recovery headers also compile independently.

These tests do not prove storage survival across power loss. The Linux production contract still
requires CI compiler/sanitizer evidence and a future process-kill/crash-image harness that interrupts
every durable transition. macOS remains a correctness-development platform rather than a qualified
power-loss durability platform.

## Complexity and tradeoffs

Multiple passes increase startup reads but ensure no replay callback happens before all physical
bytes and semantics have been accepted. Retaining an index could reduce rereads but would consume
memory proportional to record count and would still need protection from out-of-band mutation. The
current blocking, bounded-memory approach prioritizes a small auditable correctness boundary.

WAL v1 has no checkpoint deletion, so startup work grows with the entire retained history. Recovery
does not add preallocation, mmap, asynchronous I/O, compression, encryption, or speculative
filesystem abstractions.

## Likely interview questions

- Why is a checksum-invalid complete final record corruption rather than a torn-tail repair case?
- Why must semantic preflight finish before the first replay callback?
- Why does a read-only scanner require an existing lock file instead of creating one?
- Why is the lock transferred into the reopened writer?
- What is synchronized after truncation, temporary cleanup, and startup, and why?
- How does recovery stay bounded when one WAL contains many records?
- Why can physical recovery be idempotent while logical replay still needs a resettable sink?
- Why does an advisory lock not remove the deployment isolation assumption?
