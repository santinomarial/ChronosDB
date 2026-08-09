# ADR 0071: Segmented Multi-Raft persistence and recovery

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB distributed-systems maintainers

## Context

ADR 0010 requires many logical groups to share one physical persistence stream. ADR 0069 and the
Multiplexed Raft Persistent-State Record v1 established checksummed full-state records and a
persist-before-send transition, but did not own files, synchronization, rotation, or recovery.

## Accepted decision

One single-thread-affine `RaftPersistentLog` owns an existing dedicated directory, its `LOCK`, and
the active segment. Segments have independently checksummed version-1 headers and contain an exact
concatenation of Multiplexed Raft Persistent-State Record v1 values. Segment numbers and physical
record sequences are contiguous. Logical group indexes remain inside each record and never derive
from a file offset.

`append` establishes only a complete operating-system write boundary. `synchronize` calls the
platform data-synchronization primitive and advances the local durable physical sequence. Rotation
synchronizes and closes the predecessor, writes and fully synchronizes a temporary successor
header, atomically renames it without replacement, and synchronizes the directory before allowing a
record into the successor. No outbound Raft message covered by a transition may be released until
its required append/sync policy has completed.

Recovery holds the exclusive lock, rejects unknown/non-regular entries, requires contiguous segment
and record sequences, validates headers before length-driven allocation, validates every complete
record, and reconstructs the latest full state per group. A caller-selected repair mode may truncate
only a structurally incomplete suffix in the highest segment after the last fully verified record;
checksum-invalid complete headers or records are never repaired. Repair synchronizes the truncated
file and directory. Repeated recovery over unchanged bytes is idempotent.

The initial implementation uses full-state records and a bounded caller-provided batch.
`DurableMultiRaftRuntime` executes deterministic operations, appends every persistent transition,
performs one sync for the batch, and only then releases its transitions and outbound messages. It
does not yet claim majority durability, expose `QUORUM_SYNC`, or reclaim segment prefixes.

## Consequences and alternatives

Full-state records increase write amplification but make the latest state of every group explicit
and independently decodable. One file per group was rejected because it destroys shared batching;
embedding Raft state into WAL v1 was rejected because it would reinterpret a frozen application
format and create ambiguous durability semantics. Automatic corruption truncation was rejected
because it could hide loss of a previously durable record.

## Affected invariants and validation

Invariants 1, 4, 5, 8, 10, 14, and 18 apply. Focused tests cover rotation, shared-group recovery,
latest-state reconstruction, sequence continuation, exclusive ownership, explicit incomplete-tail
repair, fail-closed complete-record corruption, batched persist-before-send release, and durable
applied-index recovery. Injected syscall failures, process crash points, asynchronous worker
scheduling, reclamation, sustained corruption campaigns, and Linux power-loss qualification remain
required.
