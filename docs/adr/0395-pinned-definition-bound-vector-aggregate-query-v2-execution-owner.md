# ADR 0395: Pinned definition-bound vector aggregate query v2 execution owner

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and distributed-systems maintainers
- **Extends:** [ADR 0383](0383-owned-cross-tablet-vector-aggregate-definitions.md),
  [ADR 0385](0385-bounded-vector-aggregate-coordinator-v2.md),
  [ADR 0390](0390-finite-definition-bound-vector-aggregate-query-sender-v2.md)

## Context

The compatible Fragment-v2 snapshot owned cross-tablet-proved definitions, while finite aggregate
senders and the all-type coordinator were separately usable. An embedding still had to keep the
Manifest pin alive, create one sender per dispatch with shared query-memory authority, deliver each
complete state vector once, report terminal failure once, and retain the global plan beside the
eventual merged result.

## Decision

`DistributedVectorAggregateQueryExecutionV2` is a move-only, single-threaded portable execution
owner. Creation accepts only `CompatibleDistributedVectorSnapshotV2` with an ungrouped plan and
nonempty exact aggregate definitions. It validates source identity, query/database/generation/plan
authority, unique tablets, and finite query-memory bounds before creating one shared
`QueryResourceContext`, one immutable aggregate sender per dispatch, and one coordinator over the
exact plan-tablet order.

The compatible snapshot and Manifest pin remain owned for the execution lifetime. Every sender gets
a value copy of the cross-tablet-proved definition vector and a handle to the shared query resource
authority. Tablet lookup uses an owned ordered index. Attempts, response vectors, transport failure,
backoff, state, and hints delegate to exactly one sender.

On sender success, the complete retained definition-width vector enters the coordinator exactly
once. Any admission failure becomes that tablet's authoritative failure. Terminal sender failure is
reported exactly once; backoff or pending state never reaches the coordinator. No prefix is exposed.

`finish` stays unavailable until every sender succeeds and is delivered. It copies the global plan
before consuming the coordinator and returns that plan attached to the exact definitions, schema,
and globally finalized scalar values. The owner has no socket, thread, callback, or internal clock;
callers serialize methods and supply monotonic time.

## Alternatives considered

- **Accept caller-assembled dispatches/definitions:** rejected because that loses compatible-snapshot
  and cross-tablet schema proof.
- **Create independent resource contexts per tablet:** rejected because variable extrema must have
  one query-wide memory ceiling before coordinator merge.
- **Admit states incrementally:** rejected because later transport or sequence failure would leak an
  incomplete tablet contribution.
- **Finalize Native Protocol bytes here:** rejected because transport scheduling must still own
  cancellation and whole-query terminal publication around this portable result.

## Consequences

Creation is `O(tablets log tablets)` and retains one Manifest pin, query resource authority, bounded
sender per tablet, coordinator, and ordered index. Event lookup is `O(log tablets)` and merge work is
`O(tablets × aggregate width)`. One thread owns state transitions, so no inter-thread memory-ordering
argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): retries retain canonical Fragment-v2 bytes and
  aggregate response formats.
- [Invariant 6](../architecture/invariants.md): sender, coordinator, retained bytes, and shared query
  memory remain independently finite.
- [Invariant 10](../architecture/invariants.md): every sender and coordinator uses the exact pinned
  cross-tablet definition authority.
- [Invariant 11](../architecture/invariants.md): the Manifest pin outlives all attempts and merge.
- [Invariant 14](../architecture/invariants.md): query, tablet, ordinal, sequence, and terminal
  identity remain exact.
- [Invariant 15](../architecture/invariants.md): leader hints never rewrite admitted dispatches.
- [Invariant 18](../architecture/invariants.md): snapshot, resource, sender, coordinator, and result
  lifetimes have one owner.

## Validation plan

Use a two-tablet compatible ungrouped snapshot with two COUNT states. Prove incomplete finish,
foreign-tablet rejection, complete per-tablet acceptance, plan-order deterministic merge, exact
global values, retained definitions/schema, finite retry backoff, terminal failure propagation, and
row-mode rejection before sender construction. Run header self-containment, installed consumption,
formatting, static analysis, ASan/UBSan, and the full serialized suite.

## Migration or rollback considerations

No durable or wire bytes change. Later TCP scheduling can own this object and copy its definitions
and resource handle into one-attempt clients. Rollback must disable aggregate orchestration or
restore equivalent pinned ownership; it must not reconstruct definitions from output columns.

## References

- [Owned cross-tablet vector aggregate definitions](0383-owned-cross-tablet-vector-aggregate-definitions.md)
- [Bounded vector aggregate coordinator v2](0385-bounded-vector-aggregate-coordinator-v2.md)
- [Finite definition-bound vector aggregate query sender v2](0390-finite-definition-bound-vector-aggregate-query-sender-v2.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
