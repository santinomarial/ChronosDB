# ADR 0240: Write-Synchronous Live Checkpoint Gate

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB live-query, service, and recovery maintainers

## Context

An applied append can be evaluated and retained by a durable subscription owner while that owner's
new generation remains only dirty in memory. If the database write is acknowledged and the process
crashes before checkpoint installation, the database may recover the mutation without recovering
the corresponding replay suffix. The online write observer is not invoked for startup WAL replay,
so exposing that dirty suffix to clients would make crash resume depend on unimplemented replay
composition.

The database write cannot be rejected after application. A coordinator checkpoint failure must
therefore terminate replay authority rather than relabel the write or allow an undurable token.

## Decision

`SingleNodeLiveAppendFanout` synchronously checkpoints every affected durable coordinator after one
of three applied-position transitions: successful result publication, schema invalidation, or
explicit continuity loss. The callback returns to the database write path only after the checkpoint
generation and directory entry are synchronized.

If installation fails, `mark_replay_unavailable` clears all retained changes, expires every source
through its current latest position, and overflows every active snapshot/live subscriber without
advancing a source. The fan-out then permanently disables that plan binding. Metrics separately
report successful checkpoints, checkpoint failures, replay invalidations, and containment failures.
The already-applied write result remains authoritative.

This is the conservative correctness baseline for the current single-node runtime. A future
recovery path may replay exact committed append records after a coordinator checkpoint and then
relax write-synchronous installation, but it must be implemented and proven before doing so.

## Consequences

An online acknowledged append cannot outrun its configured live coordinators' durable replay
state. Checkpoint I/O is added to the synchronous observer path and may be expensive; no performance
claim is made. The architecture deliberately pays that cost until source-log replay and retention
are composed end to end.

Checkpoint failure can leave the checkpoint store poisoned or crash durability uncertain. The
volatile replay invalidation prevents continued delivery in the running process, while disabling
the binding prevents later positions from being silently ignored. Operators must treat the metrics
as a terminal plan-health signal and rebuild from a fresh snapshot after repairing storage.

## Validation

Focused tests verify checkpoint generation advancement and clean state after successful result,
schema, evaluation-failure, and publication-failure paths. An injected corrupt target generation
forces checkpoint failure and proves subscriber overflow, replay invalidation, and permanent plan
disablement. Manager coverage proves replay invalidation expires the current vector without moving
its positions.

## References

- [ADR 0101](0101-durable-multi-tablet-subscription-owner.md)
- [ADR 0238](0238-fail-closed-subscription-continuity-loss.md)
- [ADR 0239](0239-bounded-single-node-live-append-fanout.md)
