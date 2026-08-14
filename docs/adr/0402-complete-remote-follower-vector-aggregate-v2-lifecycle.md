# ADR 0402: Complete remote follower vector aggregate v2 lifecycle

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, cluster, networking, and replicated-runtime maintainers
- **Extends:** [ADR 0350](0350-complete-remote-follower-grouped-query-lifecycle.md),
  [ADR 0400](0400-packaged-bounded-stale-vector-aggregate-v2-query.md),
  [ADR 0401](0401-placement-backed-vector-raft-observation-batch-construction.md)

## Context

The packaged bounded-stale aggregate-v2 constructor required an already-correlated authority
vector. Placement-backed vector batch construction and authenticated acquisition existed, but an
embedding still had to retain the original plan, result schema, and Manifest pin while polling the
remote phase, then transfer the complete batch into query execution without exposing partial state.

## Decision

`ReplicatedFollowerDistributedVectorAggregateQueryV2` is a move-only, single-threaded owner with
`ACQUIRING_AUTHORITY`, `EXECUTING`, `COMPLETE`, `FAILED`, and `CANCELLED` phases. Creation validates
the follower-bounded-stale ungrouped aggregate plan, standalone result schema, replicated query
configuration, and exact source/authenticator/authorizer identity shared by authority acquisition
and execution.

It constructs the placement-backed vector observation batch, then owns that acquisition together
with the original plan, caller result schema, acquire-pinned Manifest snapshot, and borrowed query
configuration. Only a complete group-sorted authority result transfers into
`create_replicated_follower_distributed_vector_aggregate_query_v2`; no observation prefix escapes.
The resulting aggregate TCP owner then becomes the sole execution phase.

Polling is phase-aware and bounded by the caller wait plus each nested deadline. Authority or
execution failure cancels any survivor and becomes sticky. Explicit cancellation targets exactly
the active phase and publishes no result. Metrics expose acquisition counters throughout and add
execution counters only after transfer. Final result access returns a stable borrowed reference to
the TCP owner's retained finalized Native Protocol result only in `COMPLETE`.

Catalog, metadata barrier, projection, authentication, authorization, authority TLS contexts, and
query TLS contexts are borrowed and must outlive the composite owner. Plan, result schema, Manifest
pin, acquisition state, compatible snapshot, routes, query resources, senders, coordinator,
clients, and finalized result are owned. The owner has no internal thread; callers serialize every
method.

## Alternatives considered

- **Return the authority vector to the embedding:** rejected because it reopens the exact
  plan/schema/Manifest correlation this owner closes.
- **Copy the result schema after acquisition:** rejected because allocation/failure would leave its
  lifetime outside the retained phase owner.
- **Merge acquisition into the TCP scheduler:** rejected because authority and query transports
  have distinct protocols, metrics, retry bounds, and cancellation semantics.

## Consequences

The remote follower aggregate-v2 path is now one cancellable lifecycle from committed placement to
one globally finalized Native result. At most one authority pair per group and one query attempt per
tablet are active under their existing bounds. Phase transition transfers ownership once and opens
no query socket before authority completion. One thread serializes transitions, so no inter-thread
memory-ordering argument applies. No durable or network bytes change.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): no follower query begins before complete correlated
  committed authority is bound.
- [Invariant 6](../architecture/invariants.md): one plan, result schema, Manifest snapshot, proof
  batch, and route set survive as one compatible lifecycle.
- [Invariant 11](../architecture/invariants.md): the Manifest pin and query resources outlive both
  phases and the retained result.
- [Invariant 14](../architecture/invariants.md): existing observation, vector aggregate, and Native
  Protocol formats remain exact and versioned.
- [Invariant 18](../architecture/invariants.md): phase composition preserves every nested deadline,
  authentication, retry, and all-or-nothing publication gate.

## Validation plan

Run real loopback mutual-TLS leader and follower observation servers over a committed two-replica
placement. Require the owner to begin in authority acquisition, expose no result, acquire each
observation exactly once, enter aggregate execution with phase metrics, and cancel without result or
live clients. Cover policy mismatch, invalid result schema, header self-containment,
installed-consumer, formatter, changed-line static analysis, ASan/UBSan, and the full serialized
suite. A later end-to-end task should drive the execution phase through a real aggregate worker to
`COMPLETE` and decode the final Native result.

## Retrospective validation evidence (2026-08-13)

The end-to-end completion follow-up now drives the same owner from two real mutual-TLS observation
services into a real definition-bound aggregate-v2 mutual-TLS service at the unchanged committed
follower endpoint. A deterministic worker returns one complete `COUNT(*)` state, the owner reaches
`COMPLETE`, and the test decodes the retained Native Protocol payload as one non-null `INT64` value
of three. It also proves stable repeated result access, completed-state poll idempotence, rejection
of post-completion cancellation, one authority request per peer, one worker execution, one completed
query transport attempt, and clean server shutdown. The separately covered production inbound
real-CSEG worker remains the authority/execution proof for installed data; this follow-up closes the
composite phase-transition and final-publication evidence without changing a wire or ownership
decision.

The production-chain follow-up then replaces the deterministic worker with
`ReplicatedDistributedVectorAggregateQueryTcpServerV2`. Its request-local provider reacquires the
pinned Manifest, schema, placement, group, and follower-read authority and executes over a real
installed temporal CSEG. The complete remote owner returns and decodes the expected one-row
`COUNT(*) = 2` and `SUM(value) = 4.0` result, with one completed authority pair, one completed query
attempt, two production provider acquisitions (definition binding and execution), certificate
fingerprints on both sides, and deterministic shutdown of every listener and Raft owner. This
closes the single-tablet one-process production-composition evidence; multi-process, multi-tablet,
and fault-injected campaigns remain Phase 18 validation work.

## Migration or rollback considerations

Remote bounded-stale aggregate-v2 embeddings should replace manual phase orchestration with this
owner and retain all borrowed policy objects through its lifetime. Rollback is wire- and
durable-format compatible but restores manual correlation and must disable the composite path.

## References

- [Complete remote follower grouped-query lifecycle](0350-complete-remote-follower-grouped-query-lifecycle.md)
- [Packaged bounded-stale vector aggregate v2 query](0400-packaged-bounded-stale-vector-aggregate-v2-query.md)
- [Placement-backed vector Raft observation batch construction](0401-placement-backed-vector-raft-observation-batch-construction.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
