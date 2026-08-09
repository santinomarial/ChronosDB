# ADR 0096: Plan-bound subscription snapshot execution

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB query, live-query, storage, and networking maintainers
- **Extends:** [ADR 0047](0047-exact-append-only-snapshot-tablet-scan.md),
  [ADR 0068](0068-live-handoff-and-resume-token-v1.md), and
  [ADR 0094](0094-native-protocol-1-1-subscriptions.md)

## Context

The single-tablet subscription manager can atomically register a continuation boundary, and the
vector engine can execute a physical plan over one immutable aggregate storage epoch. Running those
operations independently leaves room to execute a historical snapshot at a different position than
the registered continuation. Protocol 1.1 also needs canonical query-result batches and an explicit
snapshot end before READY.

## Accepted decision

`SnapshotSubscription` is one thread-affine service owner for the finite historical half of a
single-tablet subscription. It first registers the plan/schema-bound subscription, then acquires an
aggregate storage snapshot, and exact-validates database, table, tablet, WAL lineage, and applied
record sequence against the registered boundary. A mismatch cancels the subscription; it never
executes at a nearby position.

After validation, the owner instantiates the existing checked storage-backed physical pipeline.
Every physical chunk is shape-checked against caller-supplied bound output metadata and copied into
the existing self-describing `QUERY_RESULT` encoding. End of the physical plan emits an empty
schema-bearing `QUERY_RESULT` with `END_STREAM`. Only the following successful call encodes READY
and changes the manager from snapshot to live state. Committed changes published after registration
remain manager-owned throughout execution and become pollable only after READY.

The owner borrows the manager and query resource context; both outlive it. The instantiated physical
pipeline pins its exact storage publication. Destruction, execution failure, encoding failure, or a
boundary mismatch before READY cancels the manager state and releases the pipeline. After READY the
manager owns the continuing live state independently.

## Consequences and alternatives

This slice executes an already-lowered single-tablet physical plan. ADR 0097 now supplies SQL
parsing, binding, lowering, and canonical plan fingerprint construction. Durable plan lookup on
resume and reactor worker dispatch remain later service work. ADR 0102 extends the same guarded
transition to one exact multi-tablet storage epoch and a global physical pipeline. Raft-backed
snapshot positions remain outside these WAL-bound owners and fail closed.

Acquiring the storage snapshot before registration was rejected because a commit between those
operations could be omitted. Accepting a storage position lower or higher than the manager boundary
was rejected because either direction breaks the exact snapshot-plus-suffix proof. Treating an empty
snapshot as no `QUERY_RESULT` was rejected because the protocol state machine requires an explicit
finite-stream boundary before READY.

## Affected invariants and validation

Invariants 4, 8, 11, 12, 15, and 17 apply. Focused tests execute an ordered SQL physical plan over a
real aggregate storage publication, publish the next committed change while the historical query is
open, verify it is unavailable before READY and present afterward, decode every emitted protocol
payload, and reject/cancel an exact-boundary mismatch. Disconnect races, resource-failure sweeps,
multi-chunk socket backpressure, real reactor dispatch, and broader multi-tablet operator/schedule
matrices remain in the Phase 18 ledger; ADR 0102 adds the focused multi-tablet correctness evidence.
