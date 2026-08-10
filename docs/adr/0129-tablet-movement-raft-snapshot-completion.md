# ADR 0129: Tablet movement Raft snapshot completion

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB ingestion and distributed-systems maintainers
- **Extends:** [ADR 0078](0078-two-stage-raft-snapshot-installation.md) and
  [ADR 0128](0128-tablet-movement-rtas-handoff.md)
- **Extended by:** [ADR 0130](0130-durable-tablet-movement-ready-reconciliation.md)

## Context

Durable RTAS bytes are only the application half of follower snapshot installation. The target must
still exact-match the pending leader request and durably install its full Raft `SnapshotMetadata`
before releasing a success response. Calling the generic completion operation with metadata copied
from movement's compact fields would omit the voter checkpoint, configuration index, and part-set
checksum and could acknowledge a different snapshot.

## Decision

`complete_recovered_tablet_movement_raft_snapshot` is the composed completion path. It requires an
authoritative recovered movement in `kCatchingUp`, invokes the verified RTAS handoff first, and then
exact-matches the resulting full metadata against the `GroupSnapshotInstall` emitted by receipt of
the leader's request. Group identity, movement source, local target node, and every
`SnapshotMetadata` field must agree.

Only after those checks does the adapter submit `CompleteSnapshotInstallOperation{installed=true}`
through `DurableMultiRaftRuntime`. It requires one persistent transition for the exact group and
snapshot, the matching commit boundary, and one success response addressed from the movement target
to its source. The response term must equal the persisted current term and its physical sequence
must equal the runtime's synchronized durable frontier. The adapter returns that owning outbound
message only after `execute_batch` has completed its persist-and-sync boundary.

The application RTAS may become durable before pending-request validation or Raft persistence. Such
a file is an unreferenced immutable future candidate and is safe to exact-retry. If Raft persistence
succeeds but the process stops before movement advances/checkpoints, a repeated leader request is
answered from the already-persisted Raft snapshot; the in-memory pending request is not recovery
authority. Movement may advance to ready only after successful completion (or equivalent exact
reconciliation of the already-installed Raft boundary).

This adapter does not send the returned response, install physical Manifest/CSEG files, advance the
movement checkpoint, or reclaim any snapshot objects.

## Rationale and alternatives

Composing the existing RTAS owner and durable Raft runtime preserves their independently tested
durability protocols. Passing only movement index/term/Manifest fields was rejected because they do
not identify the complete Raft snapshot. Returning success before runtime synchronization was
rejected because a crash could make the leader count an unrecoverable follower.

## Consequences and validation

The caller must retain the pending request value from the synchronized receive transition and
serialize the RTAS owner, durable runtime, and movement owner. A mismatch installs no Raft snapshot
and emits no response. The RTAS file may already exist, which is harmless and diagnosable.

Invariants 1, 4, 5, 8, 10, 11, 14, and 18 apply. Real-filesystem tests prove RTAS-before-Raft
ordering, exact pending source/full-metadata/target binding, synchronized success response release,
movement catch-up only after completion, durable runtime reopen, advanced-phase rejection, and
missing-pending failure. Syscall fault injection, process kills at each boundary, response
transport, leader retry orchestration, physical part transfer, and reclamation remain deferred.

**Retrospective update (ADR 0130):** that decision reconciles a persisted Raft snapshot with a
checkpoint-behind catching-up movement and installs the ready checkpoint before advancing live
state.

## Migration and rollback

No durable or network format changes. Rollback may use the lower-level two-stage API only if it
preserves the same exact RTAS-first and sync-before-response checks. Existing installed RTAS and Raft
snapshot state remain valid.

## References

- [Raft Tablet Application Snapshot v1 format](../formats/raft-tablet-application-snapshot-v1.md)
- [Committed Raft tablet application learning guide](../learning/raft-tablet-application.md)
- [Tablet movement checkpoint learning guide](../learning/tablet-movement-checkpoint.md)
