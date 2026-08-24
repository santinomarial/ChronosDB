# ADR 0444: Proof-revalidated local and remote Native row merge

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, query, cluster, and distributed-systems maintainers
- **Extends:** [ADR 0442](0442-bounded-native-distributed-row-request-wiring.md),
  [ADR 0443](0443-database-owned-mutable-worker-context.md)

## Context

The mutual-TLS mutable-query carrier correctly rejects a route whose source and serving node are
equal. A real daemon coordinator can nevertheless lead some selected tablets itself while other
leaders are remote. Treating the local fragment as a TCP self-route made otherwise valid all-local
and mixed-leader queries fail; independently finalizing local and remote subsets would apply global
ordering and LIMIT incorrectly and risk partial publication.

## Decision

`NativeProtocolService` partitions one prepared, plan-ordered fragment set by serving node:

1. fragments led by the configured source node execute synchronously through a borrowed production
   mutable worker;
2. every other fragment remains in the bounded mutual-TLS TCP scheduler, with only its required
   committed routes;
3. both complete terminal streams feed one coordinator created for the original complete tablet
   order and result schema; and
4. one global row finalizer applies ordering, LIMIT, result shaping, and Native batch encoding only
   after every tablet has closed successfully.

The local worker is not a trust shortcut. It uses the same database-owned context provider and
proof-revalidating worker as the TCP server, so it reobserves current Raft authority, pins a
coherent database snapshot, and exact-matches the fragment before scanning. If a local fragment is
prepared without a local worker, the request fails closed. The existing carrier continues to
reject self-routes.

The service keeps one whole-query deadline. It checks the deadline after each synchronous local
fragment; remote scheduling uses that same absolute deadline. Current synchronous worker execution
is finitely bounded by worker/query limits but cannot be interrupted mid-call. Fresh all-group
authority rebinding and reactor-visible cancellation remain separate lifecycle work.

## Consequences

All-local, all-remote, and mixed-leader requests now share one all-or-none result boundary. There is
no new durable or wire format. Remote messages are validated once by the remote subset scheduler
and again by the complete coordinator. During the handoff, the bounded remote result and the
complete coordinator can transiently retain two copies of those encoded messages; removing that
copy requires a consuming coordinator interface and evidence that it preserves failure atomicity.

The config, local worker, authentication and authorization owners, TLS-context span, and referenced
TLS client contexts are borrowed and must outlive the Native service. The service remains
thread-affine and synchronous.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): every local fragment independently revalidates
  current leader and committed placement authority.
- [Invariant 6](../architecture/invariants.md): local and remote results retain the same prepared
  query, plan, schema, and correlated snapshot authority.
- [Invariant 11](../architecture/invariants.md): the local worker owns its pinned tablet/schema
  context through synchronous execution; the coordinator owns accepted message bytes.
- [Invariant 18](../architecture/invariants.md): no Native result is published until every selected
  tablet has produced one valid terminal stream and global finalization succeeds.

## Validation

The recovered two-tablet integration executes the same projected, filtered, globally ordered and
limited Native SQL through both a real mutual-TLS remote server and the direct local production
worker. It requires identical row payloads and terminal `QUERY_END`. A source-node configuration
without the local worker returns one terminal error and no rows. The milestone gate also runs the
complete service suites, allocation-injection coverage, ASan/UBSan, installed-consumer compilation,
formatting, and static analysis where the host SDK permits it.

## Migration and rollback

Embeddings whose source node can own selected tablet leaders must supply the production local
worker. Remote-only test embeddings may leave it null. Rollback removes local partitioning and the
config pointer but restores the inability to execute self-led fragments; it does not change any
fragment, result, route, or protocol bytes.

## References

- [Bounded Native distributed row request wiring](0442-bounded-native-distributed-row-request-wiring.md)
- [Database-owned mutable worker context](0443-database-owned-mutable-worker-context.md)
- [Distributed mutable vector row request ownership](../learning/distributed-mutable-vector-rows-query-execution.md)
