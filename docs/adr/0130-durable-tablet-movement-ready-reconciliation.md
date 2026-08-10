# ADR 0130: Durable tablet movement ready reconciliation

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB ingestion and distributed-systems maintainers
- **Extends:** [ADR 0127](0127-composed-tablet-movement-checkpoint-recovery.md) and
  [ADR 0129](0129-tablet-movement-raft-snapshot-completion.md)
- **Extended by:** [ADR 0132](0132-durable-tablet-reconfiguration-phase-checkpoints.md)

## Context

RTAS and Raft snapshot state can both become durable before the next movement checkpoint. A crash
in that window correctly reopens movement as `kCatchingUp`, while the target Raft group already has
the included applied boundary. Recovery needs to reconcile those durable authorities without
requiring the now-lost in-memory pending snapshot request, advancing live state before its new
checkpoint, or changing a legacy checkpoint's storage representation.

## Decision

`checkpoint_recovered_tablet_movement_catch_up` accepts one authoritative recovered catching-up
generation, expected table, locked RTAS/checkpoint owners, and the reopened durable Raft runtime. It
reuses RTAS handoff validation, then requires the local target group to own the movement target and
to contain the exact full RTAS `SnapshotMetadata` at commit and applied indexes covering the
movement boundary.

The adapter copies the recovered prefix into a private candidate movement and advances that
candidate to `kReady`. It installs generation `current + 1` from the candidate before mutating the
supplied live movement. An external-prefix generation remains an external reference and is
revalidated against its exact chunk owner. A self-contained generation remains self-contained and
copies its received bytes into the new envelope. Generation exhaustion fails before any mutation.

Only after the ready generation reaches the checkpoint directory durability boundary does the live
movement advance and adopt the new generation number. If installation fails, the caller retains an
unchanged catching-up movement and can retry. An exact already-installed generation is an idempotent
retry, covering a process that lost only its live-memory advancement.

This reconciliation does not need a pending Raft request: persisted Raft state is the authority
after snapshot completion. It does not route the earlier success response, promote membership,
install physical Manifest/CSEG files, or reclaim snapshots.

## Rationale and alternatives

Mutating live movement and then writing its checkpoint was rejected because an I/O failure would
leave memory ahead of durable orchestration. Requiring a pending request after restart was rejected
because that request is intentionally in-memory while the installed Raft snapshot is durable.
Converting legacy generations to external references was rejected because it would invent a chunk
ownership dependency absent from their recovery path.

## Consequences and validation

Candidate construction temporarily copies the bounded snapshot prefix. This favors rollback-free
failure behavior over memory efficiency; any optimization must preserve the checkpoint-before-live
ordering. The caller serializes all four owners.

Invariants 1, 4, 5, 8, 10, 11, 14, and 18 apply. Real-filesystem tests cover the RTAS/Raft-durable
and checkpoint-behind crash across full reopen, external-owner requirement, reference preservation,
legacy self-contained preservation, ready recovery, and missing-Raft refusal without generation or
live-phase change. Allocation failure, syscall faults, process kills during the ready checkpoint,
response routing, promotion, physical part transfer, and reclamation remain deferred.

## Migration and rollback

No durable format changes. Mixed-generation recovery remains authoritative. Rollback software can
load the newly installed generation using its existing self-contained or external-reference path.

## References

- [Tablet Movement External-Prefix Reference v1 format](../formats/tablet-movement-checkpoint-reference-v1.md)
- [Raft Tablet Application Snapshot v1 format](../formats/raft-tablet-application-snapshot-v1.md)
- [Tablet movement checkpoint learning guide](../learning/tablet-movement-checkpoint.md)
