# WAL Design

> **Status: design accepted; physical lifecycle implemented.** The normative bytes are in
> [WAL v1](../formats/wal-v1.md), and the normative lifecycle/recovery behavior is in
> [WAL recovery](../architecture/wal-recovery.md). The in-memory codec, blocking POSIX operations,
> exclusive writer, locked verification, explicit repair, replay passes, existing-history reopen
> path, bounded durability coordinator, and subprocess crash/recovery harness exist. The first
> application-kind codec and single-tablet execution/recovery path are implemented. This learning
> document explains the reasoning and should not substitute for either specification; implementation
> details are in [WAL recovery implementation](wal-recovery.md).

## Purpose

A write-ahead log converts volatile application intent into an ordered sequence that can be checked
and replayed after a crash. It must answer several different questions without conflating them:

- Which record comes next?
- Did the operating system accept every byte of the record?
- Did a synchronization operation cover it?
- Is the file layout complete and uncorrupted?
- Can this binary understand the record's logical meaning?
- Which complete operations should be replayed after an ambiguous client response?

WAL v1 deliberately keeps those answers separate. Sequence numbers establish order, full-write
completion gates `ASYNC`, data synchronization gates `LOCAL_SYNC`, CRC32C establishes accidental
integrity, semantic preflight establishes compatibility, and idempotent logical replay handles
ambiguous acknowledgments.

## Subsystem boundaries

The `chronos::wal` library implements WAL identity/position values, checked layout calculation,
segment and record codecs, allocation-free validation over borrowed bytes, the serialized writer,
and physical recovery. Its encoder writes into caller-owned storage and leaves that storage
unchanged on failure. The independent `chronos::io` library supplies checked explicit-offset
transfer loops, file and directory synchronization, non-growing truncation, exclusive relative
creation, atomic no-replace rename, entry removal, and process-level advisory locking. The writer
and recovery driver compose these primitives in the specified lifecycle order. Current and
remaining boundaries are:

- the implemented directory owner that acquires `LOCK` and owns the active descriptor;
- the implemented physical codec that encodes and validates segment headers and records exactly as
  WAL v1;
- the implemented serialized appender that assigns sequences, handles short writes, rotates, and
  tracks written and durable frontiers;
- the implemented read-only scanner, explicit tail-repair path, whole-log semantic preflight/replay
  driver, and existing-history opener;
- the implemented durability coordinator that releases requests only at their effective-mode
  boundary; and
- application-kind codecs and fresh/resettable logical state that give replay domain semantics.

The physical WAL handles opaque versioned application entries. It does not decide table mutation,
deduplication, schema, row-version, or tablet semantics. Those belong to future kind-specific
payload contracts and apply logic.

## Current writer interface and ownership

`WalWriter::create_new` accepts an existing dedicated WAL directory whose parent installation has
already been synchronized. Configuration and a read-only directory-content preflight complete
before an injectable `WalLogIdGenerator` is called. The creator then acquires `LOCK`, repeats the
classification under that lock, and installs segment 1 through exclusive temporary creation,
complete header write, exact-size verification, file sync, no-replace rename, and directory sync.
Only an empty directory or a regular `LOCK` is accepted. Existing history, recognized orphan
temporaries, malformed reserved names, unrelated entries, symlinks, and nonregular entries are
rejected without cleanup. The system generator uses platform entropy; deterministic tests inject
fixed identities. Identity failure occurs before lock creation and leaves the directory unchanged.

The caller, not `WalWriter`, creates and durably installs the `wal/` directory under the database
root. This ownership is deliberate: the writer has no parent-directory descriptor and therefore
cannot prove durability of the `wal/` name. Existing/crashed history goes through
`WalWriter::open_existing`, never `create_new`.

`open_existing` acquires the same exclusive ownership lock, verifies the entire physical history,
optionally performs only an explicitly authorized incomplete-final-tail repair, removes recognized
orphan temporaries after verification, preflights and replays through a caller-supplied sink,
re-verifies, crosses the startup synchronization barriers, and returns a writer at the exact valid
end. It preserves the WAL identity and record-sequence history. The lock transfers into the returned
writer, so there is no unlocked interval between recovery and append.

`append_application_entry` finishes encoding one bounded record before its first `pwrite`, assigns
the next sequence, writes at the explicit active end offset, and returns record start/end positions.
Its success is only the complete-write (`ASYNC`-eligible) boundary. `synchronize` performs the WAL
data sync and advances the durable position/sequence. It does not acknowledge a request or implement
group commit. Rotation synchronizes the prior file, durably installs its successor, closes the prior
descriptor, and writes the complete record at offset 64 of the successor.

`WalWriterConfig` defaults its runtime target to 64 MiB and its maximum application payload to the
v1 hard maximum. Deployments and deterministic tests may choose smaller values. Validation requires
the target to exceed the header, remain at or below 64 MiB, and fit the header plus one record at the
configured payload maximum. The configured payload includes the application envelope, cannot be
smaller than that envelope or larger than the v1 maximum, and is checked before encoding, rotation,
sequence mutation, or file I/O. Earlier rotation does not alter segment bytes: the header continues
to encode the frozen v1 64 MiB limit.

`WalWriter` is move-only and not internally synchronized. One caller serializes every operation and
keeps payload bytes alive through the append call. Any attempted record-write error, sync error, or
rotation lifecycle error retains the first failure and permanently rejects later append/sync calls.
Validation errors before I/O do not poison the writer. `close` adds no implicit synchronization.

`WalCommitCoordinator` is the current multi-producer boundary above this interface. Starting it
transfers the writer to one worker thread; producers can only copy a request into bounded FIFO
admission and wait on an owning completion. The admission mutex assigns a monotonic sequence, which
defines the order in which the worker invokes append. Request and exact encoded-byte charges remain
until completion, so in-flight appends and sync waiters cannot escape backpressure accounting.

The coordinator opens a sync window after the first `LOCAL_SYNC` append. Count, encoded-byte, and
delay limits bound the window; `ASYNC` records inside it complete immediately but count toward its
physical work. One successful frontier releases every covered local waiter. Rotation may supply
that frontier for the prior segment. The detailed API, synchronization argument, failure behavior,
and metrics are described in [WAL commit coordination](wal-commit-coordinator.md).

## Current codec interface and ownership

The public headers are `chronos/wal/types.hpp` and `chronos/wal/codec.hpp`, exported by
`chronos::wal`. `WalId`, `PhysicalWalPosition`, `SegmentHeader`, `RecordLayout`, and `RecordHeader`
are owning values. `DecodedRecord` owns its header values but borrows its payload as a `ByteView`;
the complete encoded input must therefore outlive the decoded result and every copy of that view.
The codec is not internally synchronized because it owns no shared mutable state.

`calculate_record_layout` derives padding and total size before access or allocation.
`encode_segment_header` and `encode_record_header` return fixed-size owning arrays. `encode_record`
writes a complete record into caller-owned storage only after every fallible validation has passed;
on error the entire destination is unchanged. Payload may alias the destination. The decode
functions accept arbitrary borrowed bytes, use alignment-safe little-endian loads, validate CRCs and
structure before returning views, and allocate no payload memory. Only diagnostic construction may
allocate.

`PhysicalWalPosition` is deliberately not a new durable encoding. It is the checked in-memory tuple
of WAL identity, segment number, and aligned byte offset needed by later storage code.
`advance_physical_wal_position` rejects invalid frame sizes and any record that would cross the
segment limit; the current writer performs rotation before submitting such a record.

Decoder errors preserve the recovery distinction: incomplete input is `kOutOfRange`, contradictory
or checksum-invalid bytes are `kCorruption`, and a checksum-valid unknown required outer feature is
`kNotSupported`. Unknown nonzero record formats and physical types remain structurally decodable as
required by WAL v1; the replay sink decides whether their semantics are supported during whole-log
preflight.

## Why segmentation helps

Segmentation bounds individual files, gives rotation an auditable installation protocol, and creates
a future unit for checkpoint-based reclamation. It does not by itself bound total WAL usage: WAL v1
does not delete old segments, so a later checkpoint must prove another durable representation covers
their records before reclamation is legal.

A record never crosses a segment. That can waste some trailing capacity, but it means every record's
framing and checksum live in one file, every incomplete append is local to the active segment, and a
closed segment can be immutable. The tradeoff favors recovery simplicity over a small utilization
gain.

## The three integrity layers

The segment header checksum protects history identity, segment number, first record sequence, format,
and logical size. A record-header checksum protects the lengths, type, flags, and sequence that the
decoder must validate before trusting the record extent. A full-record checksum then covers the
stored header, payload, and deterministic padding.

The length complement is not a substitute for CRC32C. It is a cheap structural relationship that
rejects many damaged lengths before allocation or a large read. Likewise CRC32C is not a security
boundary: an attacker can recompute it. Authentication or encryption would require a new design with
key ownership, nonce, metadata-coverage, and upgrade rules.

## Why no preallocation

Preallocation can reduce allocation latency and fragmentation, but it makes physical EOF stop meaning
“the writer never completed bytes beyond here.” Recovery would then need another durable end marker,
careful ordering between that marker and data, and rules for stale preallocated bytes.

WAL v1 instead grows a file only through record writes. With one serialized writer that stops after a
partial hard error, the highest file can have only a valid record prefix plus one attempted suffix.
That is the foundation for the narrow incomplete-final-tail rule. Preallocation can be reconsidered
only with evidence and a replacement proof, not added invisibly to the same recovery logic.

## Installation and directory durability

Writing and synchronizing a temporary file proves its content, but does not make the final filename
durable. Atomic rename gives an old-or-new namespace transition to concurrent observers; directory
synchronization makes the new name survive the covered crash envelope. These are distinct
properties.

There are two directory levels in the proof. Database creation synchronizes the database root after
creating `wal/`, which protects the WAL-directory name. Segment installation synchronizes `wal/`
after renaming a `.cwal` file, which protects the segment name. Skipping either level can lose the
only pathname leading to otherwise synchronized bytes.

WAL v1 installs an empty segment completely before appending any record. If a crash loses a rename
that had not crossed directory sync, no request from that segment could have been acknowledged. The
previous segment was synchronized before rotation, so it remains a complete fallback prefix.

A rename that had not been synchronized may also happen to remain visible after the first crash.
Recovery cannot infer whether the old directory sync completed, so writer startup always synchronizes
the WAL directory again before publishing recovered state or accepting records. That turns the
surviving name into a proved boundary for the next crash instead of assuming visibility implies
durability.

## Acknowledgment is not replay membership

A physical record has no “client saw the response” bit. If all bytes were written but the process
failed before responding, recovery can legitimately replay an unacknowledged record. If an `ASYNC`
response was delivered but the system crashed before synchronization, recovery can legitimately
lose or tail-repair that record. `LOCAL_SYNC` removes the latter outcome only within its platform
failure envelope.

This asymmetry is why the future application record must atomically encode idempotency identity,
request digest, logical outcome, and mutation. A retry can then return the prior outcome instead of
duplicating the operation, regardless of whether the first response was observed.

## Recovery as validation before mutation

Applying while scanning is tempting because it avoids a second pass, but it creates a partially
recovered state if a later segment is corrupt or unsupported. WAL recovery therefore has three
logical passes:

1. physical verification and optional explicit final-tail repair;
2. whole-log semantic support/body preflight; and
3. ordered replay.

A scanner may store bounded descriptors or reread files. The important property is that no logical
state is published until all required bytes and semantics are known to be acceptable. Unknown record
types remain inspectable because generic framing is stable, but normal recovery cannot skip their
state transitions.

## Ownership, lifetime, and synchronization

The process advisory lock covers the directory owner for its entire lifetime. Inside the process,
one logical writer serializes sequence allocation, file offsets, record writes, rotation, sync
frontiers, and acknowledgments. Other threads may retain request state while waiting, but borrowed
payload bytes must remain alive until the record is completely encoded; the encoded record storage
must remain alive until its write path completes.

A group-sync frontier is a snapshot of the active segment identity, covered byte offset, and final
record sequence. Requests covered in that same file may share the successful sync; offsets from
different segments are never compared, and later appends may not “ride backward” on a sync that did
not cover them. Any hard write or sync error poisons the writer, because continuing could place
valid-looking bytes after an incomplete record.

Closed segment files are immutable. Verifiers/recovery hold the writer lock so the active file also
cannot change underneath offsets and checksums. A future concurrent inspector needs a separate
snapshot protocol or must call its result non-authoritative.

## Failure behavior

Important outcomes are deliberately not one generic error:

- **clean:** complete supported physical history;
- **incomplete final tail:** exactly one permitted short suffix after a fully verified prefix;
- **corruption:** bytes contradict framing, integrity, identity, or sequence and are never skipped;
- **unsupported:** bytes are structurally sound but require an unknown version/type/kind/flag;
- **I/O failure:** the storage operation itself did not provide the requested evidence;
- **writer locked:** another owner may be active; and
- **missing history:** opening an existing database cannot manufacture segment 1.

This taxonomy makes operator action safer. Installing a newer binary may resolve `unsupported`;
restoring known-good media may resolve corruption; explicit repair may resolve only the incomplete
tail. Treating all three as “truncate and continue” would be data loss.

## Complexity and resource bounds

Physical encoding and validation are `O(record_length)` time with bounded arithmetic before access.
Appending performs `O(record_length)` copying/checksum work plus operating-system writes. A complete
startup verification and replay is `O(total WAL bytes)` because old-segment checkpoints are deferred.
Memory can remain `O(maximum record length)` plus bounded descriptors by using two verified passes;
an implementation need not retain the entire WAL.

The 64 MiB segment and 16 MiB record limits are format contracts, not performance claims. They cap
single-object work and hostile length influence. Group commit can amortize sync cost but adds queueing
delay; its count/byte/time policy needs measurement and a documented maximum wait.

## Tradeoffs and deferred optimizations

- **Buffered writes:** simple and portable, but copy through the page cache. Direct I/O requires
  alignment, sector, partial-write, and sync evidence before consideration.
- **Encoded-record buffering:** makes a full checksum available before write and bounds partial
  failure to one suffix, at the cost of up to one maximum record buffer.
- **No footer:** avoids a second close-time durable authority, but closure is inferred from a valid
  successor.
- **No compression/encryption:** keeps framing and corruption analysis auditable. Either feature
  would add bombs, key/nonce lifecycle, and different integrity coverage.
- **No `mmap`/`io_uring`:** avoids dirty-page and asynchronous completion state before a correct
  blocking reference exists. A future backend must pass the identical crash oracle.
- **No reclamation:** prevents premature deletion now, but startup time and disk usage grow until the
  manifest/checkpoint phase supplies a proven coverage boundary.

## Validation and benchmark methodology

Correctness evidence comes first: golden bytes, independent round trips, corruption/truncation
fixtures, coverage-guided fuzzing, and failpoint crashes at each write/sync/rename/repair boundary.
Every randomized failure retains its seed and durable image. The durability oracle records sent,
fully written, synchronized, acknowledged, recovered, and applied identities separately by mode.
The implemented subprocess tier uses fixed failpoints and `SIGKILL` over real host files; physical
power-cut and qualified-device campaigns remain a stronger, separate evidence tier.

Performance work must use the [benchmark contract](../benchmarks/benchmark-contract.md). WAL reports
record/payload sizes, segment rotation frequency, mode, group policy, filesystem, mount options,
device/cache assumptions, write and sync counts, queue delay, acknowledgment latency distribution,
throughput, CPU, allocations, and recovery scan/replay time. Comparing `ASYNC` with `LOCAL_SYNC` as
though their guarantees were equal is invalid.

The implemented [WAL benchmark harness](../benchmarks/wal-benchmarks.md) supplies this focused
production-path measurement and exact recovery reconciliation. It retains raw per-operation samples
and verified images; its smoke tests and local output are not published performance claims.

## Likely interview questions

- Why does a successful `rename` not by itself prove the new filename is durable?
- Why is a complete final record with a bad checksum corruption rather than an incomplete tail?
- How does no-preallocation make EOF useful to recovery?
- Why does the writer stop after a hard partial-write error?
- What is the difference between a record being fully written, synchronized, acknowledged, and
  replayed?
- How can one sync safely cover multiple `LOCAL_SYNC` requests?
- Why must recovery preflight unknown types before applying the first known record?
- Why may recovery contain an operation whose client saw no acknowledgment?
- What assumptions are necessary to prove an acknowledged `LOCAL_SYNC` record survives an OS crash?
- What additional durable authority would preallocation or checkpoint-based deletion require?
