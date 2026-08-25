# ADR 0500: Packaged mutable grouped Native execution

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB SQL, cluster, replicated-service, and Native protocol maintainers
- **Extends:** [ADR 0487](0487-direct-grouped-sufficient-state-sql-lowering.md),
  [ADR 0498](0498-atomic-mutable-grouped-native-publication.md), and
  [ADR 0499](0499-shared-mutable-grouped-query-control-endpoint.md)

## Context

The packaged Native service still sent every mutable GROUP BY through complete row exchange even
when the direct SQL lowerer, proof-revalidated TabletState worker, grouped sufficient-state
scheduler, final projection, and shared private endpoint could execute the query. The grouped
scheduler also required a TCP self-route, which the mutable request format correctly rejects.

## Decision

For a replicated Native GROUP BY, first invoke the direct grouped sufficient-state lowerer. A
successful product is prepared from the same pinned catalog, complete committed placement set, and
correlated all-group read-authority vector as mutable rows. `NOT_SUPPORTED` is the only result that
selects the established row-backed grouped plan. Invalid, exhausted, or internal direct-lowering
failures remain failures and cannot silently select a more permissive path.

The mutable grouped scheduler now admits an explicitly configured in-process worker when a
fragment's serving node equals the coordinator node. The local sender retains the same immutable
fragment, grouped authority, retry policy, canonical nested re-encoding, decode accounting, and
terminal publication checks without constructing the invalid `CHDMREQ1` self-route. Other
fragments continue through the committed route and authenticated shared mTLS endpoint. Global
merge, optional final projection, order, limit, and complete Native encoding remain inside the
all-tablet scheduler.

Retryable local or remote failure discards the entire attempt. The Native owner reacquires every
group authority and publication under the original deadline, then accepts a replacement only when
the logical fragment identity and complete key/aggregate authority are unchanged. The packaged
daemon owns stable local row and grouped workers before the shared listener and supplies both to
the Native service.

## Consequences

Eligible direct-key/direct-input grouped SQL now exchanges sufficient states in the packaged local,
remote, and mixed-leader lifecycle. Computed pre-group keys, computed aggregate inputs, and hidden
ordering expressions retain the row-backed correctness path. No durable or network format changes.
The owner remains single-thread-affine, so this decision adds no shared-memory ordering algorithm.

This is not partitioned shuffle: each tablet still sends its complete local group set to the Native
coordinator. Skew policy and destination-partitioned exchange remain separate work requiring an
explicit routing contract and evidence.

## Validation

The two-tablet replicated service fixture now proves direct grouped SQL uses two grouped sessions
on the shared mTLS endpoint, while a computed-key GROUP BY still uses row sessions. It decodes the
exact nullable STRING/COUNT result and proves the in-process grouped worker produces byte-identical
Native output. A mixed local/remote scheduler test merges equal keys before publication and covers
missing local authority. The warning-as-error build, 109 service tests, 264 cluster tests, six
service allocation-failure tests, 39 cluster allocation-failure tests, and focused ASan/UBSan cases
pass. Changed-file LLVM 18 formatting and whitespace checks pass. Repository-wide formatting and
static-analysis toolchain limitations are recorded with the implementing change.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): every local or remote worker revalidates the exact
  committed/applied fragment proof before execution.
- [Invariant 6](../architecture/invariants.md): SQL lowering, all-tablet preparation, grouped
  authority, and replacement identity remain one coherent query product.
- [Invariant 10](../architecture/invariants.md): local and remote results both pass canonical
  authority-bound encoding and decode validation.
- [Invariant 14](../architecture/invariants.md): existing `CHDMREQ1` and `CHDVGRP2` bytes remain
  unchanged; self-routing is represented only in memory.
- [Invariant 15](../architecture/invariants.md): retry, decode, merge, sort, result, deadline, and
  Native publication bounds remain finite.
- [Invariant 18](../architecture/invariants.md): path selection changes exchange shape without
  weakening SQL semantics or atomic result publication.

## Migration and rollback

This is an additive pre-alpha configuration and in-memory selection rule. Rollback removes the
local grouped worker/configuration and direct Native selection, returning all mutable GROUP BY
queries to the existing row-backed path without stored-data or protocol migration.

## Unresolved questions

- Define destination-partitioned grouped shuffle routing and an explicit skew policy.
- Add multi-process split-leader grouped sufficient-state fault and differential qualification.
- Measure state exchange, coordinator memory, and tail latency before optimizing listener or
  shuffle concurrency.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Packaged native daemon](../learning/packaged-native-daemon.md)
- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
