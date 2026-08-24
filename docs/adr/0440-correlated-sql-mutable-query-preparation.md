# ADR 0440: Correlated SQL mutable query preparation

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, query, metadata, Raft, and distributed-query maintainers
- **Extends:** [ADR 0437](0437-correlated-replicated-mutable-fragment-binding.md),
  [ADR 0439](0439-schema-bound-distributed-row-sql-lowering.md)

## Context

Schema-bound distributed SQL lowering produced the exact projection, filter, plan intent, and
result schema, while replicated snapshot binding required a complete `DistributedVectorQueryPlan`
whose tablet positions and leaders already matched correlated Raft authorities. A Native request
owner would otherwise have to enumerate committed tablets, join group bindings, copy publication
positions, invent tablet event-time summaries, and preserve plan order before invoking the binder.
That duplicated the most authority-sensitive part of request construction outside the retained
snapshot.

## Decision

`ReplicatedQuerySnapshot::prepare_linearizable_mutable_vector_rows_query` accepts one nonnil query
identity, one borrowed `DistributedVectorRowsSqlPlan`, a canonical correlated group-authority
vector, and borrowed node TLS contexts. The retained snapshot:

1. validates the complete authority vector and exact SQL table/schema identity;
2. requires complete residency for every committed placement and a bounded nonempty tablet set;
3. walks the snapshot's canonical tablet-ID order, joins each tablet to its committed group, exact
   local applied position, and current-leader observation;
4. builds a leader-linearizable `DistributedVectorQueryPlan` with the SQL intent;
5. delegates to the existing all-or-none fragment binder; and
6. resolves only the resulting serving nodes through the retained committed metadata publication.

`TabletSnapshot` currently carries no authenticated event-time extrema. Preparation therefore uses
the full signed-nanosecond domain for each planner fragment. These bounds are conservative and
disable tablet pruning; they cannot falsely omit a tablet. The exact SQL event-time predicate still
executes at every worker.

The returned package owns only proof-bound fragments and finite routes. The temporary authority
plan is required only during binding because each fragment copies its canonical intent and exact
authority fields. TLS contexts remain explicitly borrowed.

## Consequences

Callers no longer reconstruct a tablet plan between SQL lowering and committed route composition.
Every returned fragment reflects one table-wide, schema-stable, barrier-covered snapshot, and the
fragment vector has canonical tablet order. Missing group authority, stale schema, partial
residency, malformed publication position, binding failure, route failure, or allocation failure
returns no prepared query.

The method is single-threaded and performs linear tablet construction plus the established
`O(tablets log tablets)` binding and route joins. It adds no dependency, synchronization algorithm,
durable format, or network frame.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only committed/applied Raft positions covered by
  the correlated leader barrier can enter a fragment.
- [Invariant 6](../architecture/invariants.md): the table-wide plan, SQL schema, publications,
  metadata, barriers, and routes derive from one retained snapshot boundary.
- [Invariant 11](../architecture/invariants.md): fragments and endpoint vectors are owned; TLS
  borrowing remains explicit.
- [Invariant 18](../architecture/invariants.md): full-domain tablet bounds are conservative, and no
  partial query is returned after any authority or route failure.

## Validation

The real two-tablet replicated recovery test binds and lowers SQL with repeated projections, exact
event-time truth, ORDER BY, and LIMIT, then prepares two canonical proof-bound fragments and one
deduplicated committed route. It rejects nil query identity, a missing group authority, and a stale
schema identity. Existing fragment, route, service allocation, installed-consumer, sanitizer,
formatting, and static-analysis gates remain applicable.

## Migration and rollback

The API is additive and not yet installed in the Native reactor request lifecycle. Rollback removes
the preparation method and binding value without changing SQL, durable, or wire formats.

## References

- [Committed mutable query route composition](0438-committed-mutable-query-route-composition.md)
- [Distributed row SQL lowering](../learning/distributed-row-sql-lowering.md)
- [Replicated ingest database ownership](../learning/replicated-ingest-database.md)
