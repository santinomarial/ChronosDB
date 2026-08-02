# ADR 0013: WAL v1 Format and Recovery

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB durability and recovery maintainers

## Context

[ADR 0006](0006-wal-durability-and-group-commit.md) established named acknowledgment modes and the
need for versioned, checksummed, append-only WAL records, but deliberately deferred the physical
format, segment lifecycle, synchronization sequence, and exact torn-tail boundary. Those decisions
must be fixed before implementation: otherwise independent encoders can disagree, a recovery path
can confuse corruption with an incomplete write, and later optimization can move acknowledgment
ahead of its promised persistence boundary.

The WAL will eventually carry corrupted or partially written bytes after crashes. Framing therefore
must protect the lengths and identities used to bound reads before higher layers interpret payloads.
The first design must also be implementable with ordinary portable buffered I/O, without relying on
preallocation, direct I/O, memory mapping, or a Linux-only asynchronous interface.

## Accepted decision

ChronosDB WAL v1 is the [specified](../formats/wal-v1.md) segmented, append-only, checksummed
single-node log, and its lifecycle follows the [recovery architecture](../architecture/wal-recovery.md).

The accepted physical decisions are:

- all multi-byte fields are explicitly little-endian fixed-width integers;
- records use bounded self-checking headers, zero padding, and full-record CRC32C coverage;
- records never cross a segment boundary;
- there is one logical writer per WAL directory, serialized under a process-level advisory lock;
- WAL v1 uses no preallocation;
- the `wal/` directory itself is durably installed by synchronizing its database-root parent before
  initial segment creation or any acknowledgment;
- a final segment name is discoverable only after its complete header is written to a temporary
  same-directory file, the file is synchronized, it is atomically renamed, and the directory is
  synchronized;
- no record is appended or acknowledged in a new segment before that installation boundary;
- the prior segment is synchronized before a newer segment becomes active;
- once a segment ceases to be the final active segment, its bytes are never modified in place;
- the final active segment is append-only and may be truncated only by explicit synchronized
  recovery-tail repair;
- `ASYNC` acknowledgment waits for the complete record to be accepted successfully through the WAL
  file write path but does not wait for stable-media synchronization;
- `LOCAL_SYNC` acknowledgment waits for a successful WAL data-synchronization operation covering
  the complete record, after any required segment installation boundary;
- `QUORUM_SYNC` is rejected until replication exists;
- recovery physically verifies the entire WAL, repairs only the narrowly defined incomplete suffix
  of the highest segment when explicitly authorized, re-verifies, preflights semantic support, and
  only then replays in sequence order; writer startup re-synchronizes the WAL directory before
  publishing recovered state or accepting new records;
- middle-of-log corruption, complete-record checksum failure, missing segments, and discontinuities
  are never skipped or silently truncated;
- an unknown physical record type can be structurally framed and checksummed, but normal recovery
  rejects unsupported type/application semantics before replay rather than skipping them; and
- checkpoint-driven removal of old segments is deferred.

The WAL v1 directory identity, consecutive segment/record sequences, exact 64-byte segment header,
40-byte record header, 64 MiB segment limit, 16 MiB record limit, CRC scopes, padding formula,
filename grammar, and compatibility policy are durable contracts in the format specification. They
cannot be changed by an implementation prompt or optimization without a new accepted format/ADR.

WAL compression, encryption, direct I/O, memory-mapped writing, and `io_uring` are deferred. The
initial implementation owns its WAL framing, segmented persistence, and recovery under
[ADR 0011](0011-dependency-and-build-versus-buy-policy.md); no database or WAL engine dependency may
replace them.

## Detailed rationale

No-preallocation makes the actual file end meaningful. Combined with one writer and a rule that a
failed partial append poisons the writer, this permits one deterministic repairable case: a short
suffix at the verified end of only the highest segment. A full header or record with contradictory
lengths or bad integrity is stronger evidence of corruption and fails closed.

Header CRC32C protects the fields needed for bounded framing before the decoder trusts a declared
record extent. The full-record CRC then covers the header, payload, and deterministic padding. A
length complement catches simple torn/incorrect length fields and makes the relationship testable
before allocation. CRC32C is for accidental corruption, not malicious integrity.

Installing an empty segment before appending records creates a simple proof boundary. If a crash
makes an unsynchronized rename disappear, no record from that segment was acknowledgment-eligible.
Synchronizing the previous segment before installation makes all closed segments durable under the
local contract and prevents rotation from leaving older acknowledged data behind an ambiguous
directory transition.

Whole-log verification separates physical trust from logical side effects. Structural support
preflight prevents an old binary from applying a prefix and only then discovering an unknown record.
Failing rather than skipping preserves the authoritative operation sequence.

## Alternatives considered

- **One ever-growing file:** removes rotation but creates unbounded recovery and operational file
  size and makes future retention coarse.
- **Records spanning segments:** uses tail capacity but couples two files into one atomic framing and
  expands crash states for little benefit.
- **Preallocated segments:** can reduce allocation latency, but makes EOF unable to distinguish
  unwritten reservation from a torn append without a separate durable end marker. It is not needed
  before evidence shows allocation is a bottleneck.
- **Segment footer or mutable durable end pointer:** can mark closure, but introduces another ordered
  write and recovery authority. Successor existence plus complete record framing is sufficient for
  v1.
- **Checksum only the payload:** leaves lengths, type, flags, and sequence vulnerable before safe
  interpretation.
- **Treat any final checksum failure as a torn tail:** hides real corruption and could discard an
  acknowledged synchronized record.
- **Search for the next magic after corruption:** permits cross-application and destroys proof that
  the recovered sequence is complete.
- **Rename without directory synchronization:** cannot establish durability of a new final name
  under the target filesystem contract.
- **Acknowledge on write under one generic durable mode:** conflates page-cache acceptance with stable
  storage and contradicts ADR 0006.
- **Use `mmap`, direct I/O, or `io_uring` initially:** increases platform/alignment/completion state
  without evidence and does not remove synchronization obligations.

## Consequences

- The WAL writer must buffer or otherwise finish encoding one bounded record before its first file
  write.
- Up to less than one record of segment capacity may remain unused at rotation; the simplicity is
  intentional and must not be “optimized” by record splitting.
- Every rotation includes prior-file synchronization plus new-file and directory synchronization,
  so segment size affects tail latency and must be measured without weakening ordering.
- A write or sync failure poisons the writer until locked recovery; availability yields to avoiding
  middle-of-log ambiguity.
- Recovery may replay complete records that were never acknowledged, and `ASYNC` records may be lost.
  Higher-layer idempotency remains required.
- WAL v1 alone cannot reclaim old segments. Disk usage is unbounded until checkpoint/manifest work
  accepts and implements a coverage/reclamation protocol.
- Physical framing can be implemented before application mutation payloads, but no production
  application entry may invent an unversioned kind-specific body.

## Affected invariants

This decision directly supports invariants [1, 4, 8, 9, 10, 14, and 18](../architecture/invariants.md):
acknowledgment boundaries, ordered replay, idempotent recovery/retry, complete integrity coverage,
versioned compatibility, and prevention of weaker optimization paths. It also prepares invariant 2
by defining the WAL side of future checkpoint/manifest ordering without prematurely specifying
reclamation.

## Validation plan

- Maintain byte-for-byte golden fixtures for headers, records, application envelopes, segment
  rotation, boundary lengths, and cross-segment sequence continuity.
- Generate values and payload sizes around every width, alignment, record, and segment limit; compare
  independent encoder/decoder implementations and cross-endian fixtures.
- Fuzz physical and application framing under ASan/UBSan with hostile lengths, complements, flags,
  versions, types, padding, CRCs, truncation, splicing, gaps, and reordered segments.
- Use a fault-injecting filesystem interface to force every short write and error and crash before
  and after WAL-directory creation, parent-directory sync, file sync, rename, WAL-directory sync,
  record write, group-sync frontier, acknowledgment, startup namespace barrier, and repair step.
- Reconcile sent, fully written, synchronized, acknowledged, physically recovered, and logically
  replayed identities by mode. No acknowledged `LOCAL_SYNC` identity may be absent under covered
  failures; only the accepted `ASYNC` envelope may lose acknowledgments.
- Repeat recovery and recovery-tail repair over copied images, including crashes during repair, and
  require identical classification/state.
- Verify unknown versions/types are reported unsupported before replay and that corruption is never
  bypassed by magic resynchronization.

## Deferred decisions

Application kind/body formats, group-commit count/byte/delay parameters, default durability mode,
checkpoint and manifest formats, checkpoint-driven segment removal, operational retry policy after
device remediation, production filesystem qualification beyond the stated Linux contract,
encryption, compression, direct I/O, memory-mapped writing, `io_uring`, and `QUORUM_SYNC` replica
persistence remain deferred.

## Migration or reversal implications

Format 1.0 bytes and assigned meanings are immutable once written. A future physical format requires
its own specification, accepted ADR, retained reader or offline converter, and explicit history
transition; v1 forbids mixing physical segment formats in one WAL identity. A new record or
application kind may use the existing framing, but an old reader will reject it before replay until
support is installed. Weakening an acknowledgment guarantee requires a newly named mode, never a
reinterpretation of `LOCAL_SYNC`.

Because old-segment removal is deferred, the first implementation cannot discard v1 history merely
to simplify an upgrade. Migration must retain the only required durable copy until a later accepted
checkpoint/manifest protocol proves coverage.

## References

- [WAL v1 format](../formats/wal-v1.md)
- [WAL recovery architecture](../architecture/wal-recovery.md)
- [Consistency and durability contract](../product/consistency-and-durability.md)
- [ADR 0006: WAL durability and group commit](0006-wal-durability-and-group-commit.md)
- [Roadmap phases 2 and 3](../roadmap.md)
