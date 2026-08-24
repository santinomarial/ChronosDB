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
- `inspect_wal_suffix(path, checkpoint, sink)` accepts an externally durable WAL identity, record
  sequence, segment, and exact record-end offset. It permits missing covered prefix segments,
  verifies the required suffix, and preflights/replays only records after the checkpoint.
- `recover_wal_from_checkpoint(...)` adds authorized final-tail repair, recognized-temporary
  cleanup, and startup synchronization to the same suffix proof, then closes all handles.
- `recover_wal(config, options, sink)` performs writer-startup recovery and closes the recovered
  handles instead of returning a writer.
- `WalWriter::open_existing(config, options, sink)` performs the same recovery and transfers the
  directory, lock, and active file into a writer positioned at the verified end.
- `WalWriter::open_existing_from_checkpoint(...)` performs checkpoint recovery and transfers that
  same ownership without requiring already reclaimed prefix segments to reappear.
- `WalWriter::reclaim_checkpointed_segments(checkpoint)` revalidates the live namespace and exact
  suffix, fully scans every candidate closed segment, removes only the covered increasing prefix,
  and synchronizes the directory once. The active highest segment is never removed.
- `WalWriter::resolve_replay_checkpoint(sequence)` maps one durable logical sequence to its exact
  verified record-end coordinate after scanning the complete retained namespace. It returns no
  coordinate when that logical prefix is older than every retained file.
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

Discovery snapshots and sorts directory entries. Ordinary whole-history recovery accepts only:

- one regular `LOCK`;
- exact final segment names whose numeric sequence starts at 1 and has no gap; and
- exact regular temporary segment names from the reserved format namespace.

Malformed reserved names, unrelated entries, symlinks, special files, a missing lock, and a missing
final segment fail closed. Temporary files are counted but never treated as history. Mutable
recovery removes recognized temporaries only after the named WAL has verified (and any authorized
repair has completed), tolerates an already absent entry, and synchronizes the directory after a
cleanup batch. Read-only paths leave them untouched.

Checkpoint inspection retains the same exact name/type policy but allows gaps wholly before the
coordinate. Every present covered-prefix segment still has its size, header checksum, filename
number, and WAL identity verified. The required suffix starts with either the coordinate segment or
its immediate successor, remains consecutive through the active highest segment, and must use the
checkpoint WAL identity. Missing required suffix state is corruption.

The writer's logical-prefix resolver derives its scan checkpoint from the first retained segment.
For segment 1 this is the canonical empty boundary. For a later first segment it is the immediately
preceding segment/sequence required by suffix recovery. A requested sequence before that boundary
is already physically absent; an equal boundary is directly reusable; a later sequence must be
observed at an exact decoded record end. In every case the complete retained suffix is scanned and
must match the live writer before resolution succeeds.

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

For suffix inspection, a present coordinate segment is physically scanned from its header and the
checkpoint must equal the exact end of its named record. If that segment was removed, its immediate
successor header must declare `checkpoint.record_sequence + 1`. Physical verification still covers
all required bytes, while sink callbacks are filtered to strictly later sequences. An incomplete
active tail is reportable only after the checkpoint boundary has already been proved.

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

Both writer reopen paths preserve the existing WAL identity, final segment, global record sequence,
and physical offset. Checkpoint reopening derives the next sequence from the verified required
suffix, or from the checkpoint when it is exactly the active end. If a configured runtime rotation
target is smaller than that already-valid final file, the next append rotates before writing; it
never subtracts unchecked lengths from the existing end. Sequence exhaustion remains terminal.

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
the inspector's output and exit codes. Public recovery headers also compile independently. A
Linux-only packaged-daemon case damages one CRC-covered identity byte in the established active
segment header, requires the exact header-checksum failure before socket admission, and proves the
complete segment is not rewritten by the failed startup. Another case creates a complete application
record through packaged SQL INSERT, corrupts a body byte while leaving its header intact, and
requires the complete-record CRC32C failure plus byte-for-byte segment preservation. This executes
the rule that a complete corrupt final record is never an incomplete-tail repair candidate. The
complementary packaged case appends a one-byte incomplete tail after a valid active header. Default
startup requires explicit repair authorization and preserves the complete segment, including that
suffix.

The subprocess crash harness interrupts initial and successor installation, complete/short append,
data synchronization, grouped completion, repair/reopen, and locking on real host files, then runs
the production recovery oracle. These tests still do not prove storage survival across power loss.
The Linux production contract requires CI compiler/sanitizer evidence and qualified local
filesystem/device testing. macOS remains a correctness-development platform rather than a qualified
power-loss durability platform.

## Complexity and tradeoffs

Multiple passes increase startup reads but ensure no replay callback happens before all physical
bytes and semantics have been accepted. Retaining an index could reduce rereads but would consume
memory proportional to record count and would still need protection from out-of-band mutation. The
current blocking, bounded-memory approach prioritizes a small auditable correctness boundary.

Checkpoint suffix inspection is read-only and does not itself authorize deletion, repair, cleanup,
or writer reopening. The explicit mutable recovery APIs do authorize repair/cleanup under the WAL
lock but never delete a final segment. Reclamation is a separate explicit writer operation whose
checkpoint must come from an already durable selected manifest. It validates all candidates before
the first unlink, advances removal metrics only after directory sync, and poisons the writer on a
mutation failure. A crash may retain any covered subset; repeating recovery and reclamation
converges. Recovery does not add preallocation, mmap, asynchronous I/O, compression, encryption, or
speculative filesystem abstractions.

Logical-to-physical resolution is also read-only and does not replace the reclamation revalidation.
It costs `O(retained WAL bytes)` and intentionally retains no per-record index: reclamation is
infrequent, memory stays bounded by one record plus directory state, and a complete suffix scan
detects corruption beyond the requested boundary before any higher-level batch begins deletion.

## Likely interview questions

- Why is a checksum-invalid complete final record corruption rather than a torn-tail repair case?
- Why must semantic preflight finish before the first replay callback?
- Why does a read-only scanner require an existing lock file instead of creating one?
- Why is the lock transferred into the reopened writer?
- What is synchronized after truncation, temporary cleanup, and startup, and why?
- How does recovery stay bounded when one WAL contains many records?
- Why can physical recovery be idempotent while logical replay still needs a resettable sink?
- Why does an advisory lock not remove the deployment isolation assumption?
