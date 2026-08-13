# ADR 0375: Proof-revalidated schema-bound vector row worker v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, service, Manifest, and cluster maintainers
- **Extends:** [ADR 0318](0318-request-local-real-cseg-query-worker-service.md),
  [ADR 0352](0352-canonical-distributed-vector-plan-intent.md),
  [ADR 0369](0369-authenticated-schema-bound-vector-query-receiver-v2.md)

## Context

The authenticated vector-v2 receiver had only an embedding seam. No production implementation
acquired one coherent request-local Manifest, schema, placement, group, and read-barrier context,
reproved it at the worker, resolved current logical winners from real temporal CSEGs, or encoded the
result under the admitted Fragment-v2 schema.

Vector intent also names final `ORDER BY` and `LIMIT`. Applying either independently at each tablet
can discard or reorder rows needed by the global result. Aggregate modes have a stricter gap: final
AVG and variance values are not mergeable without additional state, while Result Exchange v2 is
bound to the final result schema. Treating final per-tablet values as global partials would be
incorrect.

## Decision

`execute_distributed_vector_rows_fragment_v2` is the synchronous query-layer row-fragment worker.
It canonically re-encodes the complete in-memory Fragment-v2 value, accepts only row mode, and
reuses the aggregate/grouped worker's local route, placement, leadership, barrier, Manifest,
tablet, source, durable-position, recovery-schema, and part-range gates. It independently proves
the admitted result descriptors against the current projected schema before part I/O.

The worker loads generation-pinned temporal CSEGs through the existing validated loader, resolves
current winners and tombstones, applies the optional event-time predicate, and materializes the
caller-ordered row projection through the bounded vector pipeline. A bounded query resource context
accounts chunk memory. A borrowed synchronous consumer sees each nonempty chunk only during its
call; any later failure requires the caller to discard earlier consumed chunks. Tablet-local
execution deliberately emits every matching row in source order and leaves final ordering and limit
untouched.

`ReplicatedDistributedVectorQueryWorkerV2` implements the receiver's worker service. Its distinct
context provider acquires one coherent owning request-local authority value for the exact
Fragment-v2 dispatch. The adapter retains that context through execution, encodes each chunk as the
unchanged Native Protocol v1 result payload, bounds result messages and exact Result-Exchange-v2
frame bytes, and returns one complete value-owned terminal stream. Empty input returns the canonical
sequence-one terminal-only message. Aggregate modes return `NOT_SUPPORTED` before part I/O until a
versioned all-type merge-state contract exists.

The worker and provider are single-owner and synchronous. Concurrent embeddings must serialize
calls or synchronize their provider. No durable or wire bytes change.

## Alternatives considered

- **Apply `ORDER BY` and `LIMIT` inside each worker:** rejected because the intent defines final
  output semantics and no merge-preservation proof exists.
- **Return final aggregate values as partials:** rejected because AVG and variance cannot be merged
  from those values, and the schema has no hidden state columns.
- **Trust the coordinator-side binder:** rejected because placement, barrier, Manifest, schema, and
  storage authority can change before a worker executes.
- **Publish encoded batches incrementally:** rejected because a later worker or allocation failure
  would expose partial success. The service returns only its complete retained vector.

## Consequences

Schema-bound row fragments now have a production bridge from authenticated dispatch to real CSEG
bytes without weakening snapshot authority or applying unsafe local final semantics. Memory is
bounded by temporal-resolution limits, one query-resource budget, per-chunk limits, native payload
limits, message count, and exact retained exchange-frame bytes. Work is linear in validated part
bytes plus selected rows and output cells.

ADRs 0376–0379 subsequently supply the production owned receiver/server composition, portable
sender-to-coordinator execution, multi-tablet TCP scheduling, and bounded global row ordering/
limit. All-type aggregate merge state, authority rebinding, and process integration remain separate
tasks. This ADR does not claim Phase 16 completion.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only committed, applied temporal winners are read.
- [Invariant 6](../architecture/invariants.md): one exact generation and bounded worker resources
  remain pinned through synchronous execution.
- [Invariant 10](../architecture/invariants.md): every output chunk and native payload exact-matches
  the admitted Fragment-v2 result schema.
- [Invariant 11](../architecture/invariants.md): the request-local Manifest owner outlives all part
  views and result materialization.
- [Invariant 14](../architecture/invariants.md): query, tablet, group, route, position, schema, and
  result identities stay explicit.
- [Invariant 15](../architecture/invariants.md): unsafe tablet-local final semantics and unsupported
  aggregate modes fail closed.
- [Invariant 18](../architecture/invariants.md): the row consumer's borrowed lifetime and the
  service's complete value-owned publication are explicit.

## Validation plan

The real-Manifest/real-CSEG query worker case executes a Fragment-v2 row projection and proves that
two local rows survive a final descending-order/limit-one intent for later global processing. It
rejects schema mismatch, aggregate mode, stale local placement, invalid fixed configuration, and a
loader that fails the exactly-once callback contract before publishing output. The production
service encodes and decodes the exact two-row schema-bound native batch and terminal stream. Header
self-containment and installed consumption cover both public boundaries. ASan/UBSan, relevant
static analysis, pinned formatting, and the full serialized suite are required before completion.

## Migration or rollback considerations

No bytes change. Embeddings may install the service behind the existing v2 receiver for row mode.
Rollback removes that worker and leaves the authenticated receiver seam unavailable; it must not
replace the worker with a path that skips fresh local authority or applies per-tablet final
ordering/limit.

## References

- [Canonical distributed vector plan intent](0352-canonical-distributed-vector-plan-intent.md)
- [Authenticated schema-bound vector query receiver v2](0369-authenticated-schema-bound-vector-query-receiver-v2.md)
- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
- [Distributed aggregate exchange learning guide](../learning/distributed-aggregate-exchange.md)
