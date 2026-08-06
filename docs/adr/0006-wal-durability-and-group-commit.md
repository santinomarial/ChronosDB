# ADR 0006: WAL Durability and Group Commit

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB durability and recovery maintainers

## Context

An acknowledgment is meaningful only when its failure envelope is named. System calls such as `write`, memory mapping, and even synchronization have platform, filesystem, device, and power-loss qualifications. ChronosDB also needs batching to avoid forcing one stable-media operation per event while preserving an exact per-request durability contract.

## Accepted decision

ChronosDB defines three acknowledgment durability modes:

1. **`ASYNC`:** acknowledgment may occur before stable-media synchronization. Acknowledged data may be lost after some failures. This mode must never be described as durable.
2. **`LOCAL_SYNC`:** acknowledgment occurs only after the relevant local log bytes have been synchronized according to the documented platform contract. It is intended to survive process termination and ordinary operating-system crashes on the local node. Hardware, drive-cache, filesystem, and kernel limits are documented rather than hidden.
3. **`QUORUM_SYNC`:** available only after replication exists. Acknowledgment occurs after a majority of replicas meet the documented persistence condition. It survives failure of a minority of replicas under the stated membership, storage, and fault assumptions.

The requested/effective mode is part of the operation result and benchmark configuration. No implementation may silently downgrade it.

WAL records are framed, versioned, length-delimited, and checksummed. Framing and integrity coverage must let readers validate bounds before interpreting fields. WAL segments are append-only. Recovery accepts a partial final record as an incomplete tail at the defined end, but corruption before the valid end of the durable log is surfaced and does not become silent truncation.

Group commit batches synchronization work for multiple requests. Each request is acknowledged only when the synchronization/quorum boundary required by its own effective mode has completed. Grouping may delay an acknowledgment within a documented bound; it does not weaken it.

Client batch identities are durably associated with applied input to support idempotent retry. Checkpoint advancement and WAL truncation/reclamation are separate crash-safe operations: a durable checkpoint may establish coverage before obsolete segments are removed. Recovery is idempotent.

## Detailed rationale

Named modes make latency/durability tradeoffs explicit and comparable. An asynchronous option is useful when producers can replay, but calling it durable would violate the product's evidence standard. Local synchronization provides a single-node contract, and quorum synchronization extends the same acknowledgment vocabulary once replication exists.

Append-only checksummed segments give recovery a monotonic structure and permit safe group commit. Separating checkpoint publication from deletion prevents one interrupted cleanup operation from erasing the only recoverable copy.

## Alternatives considered

- **Synchronous flush per event:** simple semantics but needlessly multiplies synchronization operations when independent requests can share one boundary; it remains approximable with group size one.
- **No WAL:** would force heads or columnar parts to become synchronously durable before acknowledgment and make short-batch recovery and ordering much harder.
- **Rely only on `mmap`:** dirty-page visibility is not a persistence contract; explicit synchronization, error handling, and durable ordering are still required.
- **Acknowledge on `write` without naming persistence:** conflates kernel acceptance with stable storage and makes recovery guarantees impossible to state.
- **One vague “durable” mode:** hides replica/local differences and hardware assumptions from clients and benchmarks.

## Consequences

- APIs, metrics, logs, tests, and benchmarks must record requested and effective durability mode.
- `ASYNC` may lose acknowledged data within its specified envelope and requires candid operational guidance.
- `LOCAL_SYNC` needs a per-platform filesystem/device contract and error propagation.
- `QUORUM_SYNC` remains unavailable until Raft exists; its replica persistence condition needs a later specification.
- Group-commit scheduling affects latency and fairness and must be bounded and measured.

## Affected invariants

This decision is the primary policy for invariants [1, 4, 8, 9, 10, 14, and 18](../architecture/invariants.md): acknowledged durability, ordered application, idempotent recovery/retry, integrity/versioning, and prohibition on semantic weakening for speed.

## Validation plan

- Maintain golden, round-trip, corruption, truncation, and fuzz tests for WAL records.
- Crash-inject before and after append, synchronization, checkpoint publication, acknowledgment, segment rotation, and reclamation.
- For each mode, compare acknowledged identities with recovered identities under every covered failure; allow loss only within the explicit `ASYNC` contract.
- Verify group members are not acknowledged before their required shared boundary and that mixed modes do not inherit a weaker result.
- Measure acknowledgment distributions, synchronization counts, batch size, recovery time, and throughput separately by mode and storage configuration.

## Deferred decisions

Record fields, checksum algorithm, segment size/naming, synchronization syscall sequence, directory-sync requirements, default mode, group-commit delay/size policy, error retry policy, batch-identity scope/retention, checkpoint format, and `QUORUM_SYNC` replica persistence details remain deferred.

**Retrospective note (2026-08-06):** [ADR 0013](0013-wal-v1-format-and-recovery.md) resolves WAL
v1 framing, naming, synchronization, and recovery, while
[ADR 0017](0017-manifest-generations-installation-and-checkpoints.md) resolves the first
single-node manifest/checkpoint format and external WAL-prefix coverage protocol. The server
default, retry pruning/horizon, and distributed persistence decisions remain deferred.

## Migration or reversal implications

The mode names and meanings become protocol contracts once exposed. Tightening a guarantee can be compatible; weakening one requires a new mode/version rather than reinterpretation. WAL format changes require versioned readers or an offline migration. Existing synchronized data must never be relabeled asynchronous during upgrade.

## References

- [Architecture WAL](../architecture/overview.md)
- [Roadmap phases 2 and 3](../roadmap.md)
- [Invariant 1](../architecture/invariants.md)
