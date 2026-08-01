# ADR 0001: Project Scope and Workloads

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB architecture maintainers

## Context

ChronosDB needs a stable product boundary before storage, query, and networking choices become expensive to reverse. The target workloads combine large event histories, continuous arrival, late data, corrections, analytical scans, and live outputs. Treating those requirements as unrelated features would produce separate semantics for historical and streaming paths and make the snapshot-to-stream boundary an application problem.

The representative workloads are financial market data—trades, quotes, order-book events, corrections, ASOF analysis, and backtesting—and infrastructure observability—metrics, structured events, traces, labels, dashboards, and continuous aggregates. Both families require typed data, explicit time semantics, high-cardinality filtering, retention, and predictable behavior under bursts.

## Accepted decision

ChronosDB is an event-time-native real-time analytical database. Its primary data model is typed relational event tables, each with a designated event-time column and database-managed system-time visibility.

The same coherent engine owns:

- historical SQL over stable committed snapshots;
- continuous and incremental analytics over committed changes; and
- gap-free historical-to-live subscriptions anchored to deterministic commit positions.

Financial market data and infrastructure observability are the primary workload families. ASOF joins, latest-by queries, time bucketing, deduplication, corrections, retention, late-data handling, and resumable live results are central design inputs. They are not optional adapters or marketing-only extensions.

ChronosDB is not a general-purpose OLTP database. It will not optimize its primary storage or execution model around arbitrary in-place row updates, high-contention point transactions, or general distributed transactions.

## Detailed rationale

Historical and live paths need a common schema, expression model, commit order, correction model, and event-time policy. Owning both sides permits the historical snapshot and continuation stream to share one committed boundary, which is necessary to prevent an omitted change during handoff. Typed relational tables make types, nullability, keys, temporal columns, and query semantics explicit enough for optimization and validation.

The two workload families are broad enough to expose skew, burstiness, high cardinality, out-of-order input, and long-retention scans without expanding scope to every database use case. Their common pressure favors columnar ingestion and scans while still demanding durable identities and versioned corrections.

## Alternatives considered

- **Pure metrics database:** would fit regular numeric samples but underserve trades, quotes, structured event payloads, corrections, ASOF joins, and general typed relations.
- **Generic document store:** would make flexible ingestion easy, but weak typing and opaque documents would obstruct column projection, temporal semantics, vectorized execution, and predictable SQL binding.
- **Key-value store:** would provide simple point access but force analytical schema, indexes, scans, aggregation, temporal versioning, and subscriptions into application-side conventions.
- **Market-data fan-out server without a historical database:** would address low-latency distribution but leave replay, backtesting, corrections, audit history, and snapshot-to-live handoff to separate systems.
- **General OLTP engine:** would prioritize mutation and transaction machinery that conflicts with immutable columnar parts and scan-oriented execution, while leaving live analytical semantics as an add-on.

## Consequences

- Product requirements and benchmarks must trace to the documented workload families.
- Table, storage, SQL, and subscription specifications must treat event time, corrections, and retention as core semantics.
- The engine accepts less breadth in transactional features and SQL compatibility.
- Historical and live results must agree on types, visibility, and supported expressions.
- New workload families require evidence that they fit the model or a new ADR changing scope.

## Affected invariants

This decision establishes the product need behind invariants [6, 9, 12, 13, 15, and 17](../architecture/invariants.md): stable snapshots, idempotent input, deterministic resume boundaries, dual-time corrections, bounded subscribers, and a gap-free handoff. It also reinforces invariant 18 by rejecting performance claims that weaken those semantics.

## Validation plan

- Maintain representative schemas and historical/live query cases for both workload families.
- Trace each supported SQL and subscription feature to at least one documented workload.
- Test snapshot plus continuation against a reference committed log, including retries, late data, and corrections.
- Benchmark mixed ingest, historical scan, and live fan-out with declared data distributions rather than isolated peak throughput alone.

## Deferred decisions

The exact SQL grammar, row-identity declarations, correction and cancellation syntax, retention policy language, watermark finalization rules, supported ASOF variants, and subscription change-record format remain deferred to their roadmap phases.

## Migration or reversal implications

There is no deployed format to migrate. Reversing this scope after storage and SQL implementation would change table contracts, data layout, and user-visible semantics and therefore requires a superseding ADR. A later adjacent workload can be added without reversal only if it preserves this event-relational and temporal model.

## References

- [Product vision](../product/vision.md)
- [Representative workloads](../product/workloads.md)
- [Architecture overview](../architecture/overview.md)
- [Architecture non-goals](../architecture/non-goals.md)
