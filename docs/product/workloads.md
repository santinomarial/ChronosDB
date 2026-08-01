# Representative Workloads

The canonical proposed schemas are in the [data-model contract](data-model.md), and query syntax follows [SQL v1](sql-v1.md). Neither is implemented. Cardinalities describe design envelopes, not benchmark claims or guaranteed limits.

All tables designate one event-time column. System time is database-managed and distinct from fields such as `recv_ts`, which represent source or receive metadata.

## Trades

Canonical proposed DDL: [trades](data-model.md#trades).

- **Expected cardinality:** tens of thousands of symbols across venues; billions to trillions of retained events in large deployments; `trade_id` is high cardinality.
- **Ordering:** feeds are usually close to event-time order per venue/symbol but can interleave across sessions, connections, and symbols.
- **Out of order:** network paths, venue retransmission, and feed arbitration can deliver older trades after newer ones. Policies use watermarks and allowed lateness rather than assuming monotonic timestamps.
- **Correction and deduplication:** `(symbol, venue, trade_id)` is the logical row identity; an explicitly authorized correction creates a new system-time version, while an identical retry is idempotent. Cancellation/tombstone write syntax remains deferred.
- **Common projections:** `ts`, symbol, price, size, venue, flags, and notional (`price * size`).
- **Common filters:** symbol/venue sets, event-time ranges, price or size thresholds, trade identity, and flags.
- **Common aggregations:** volume, notional, VWAP, trade count, OHLC, realized volatility inputs, and time bars.
- **Historical queries:** backtests, session summaries, liquidity studies, trade replay, and “as known at system time” reconstruction.
- **Live queries:** rolling VWAP, per-symbol bars, unusual-size alerts, and historical results followed by committed updates.
- **Stress patterns:** bursts at market open or news events, hot symbols, tiny batches mixed with recovery bursts, late corrections to closed windows, and long scans concurrent with ingest.

```sql
SELECT symbol,
       time_bucket(INTERVAL '1 minute', ts) AS minute,
       sum(price * size) / sum(size) AS vwap
FROM trades
WHERE ts >= TIMESTAMP '2026-01-02 14:30:00Z'
GROUP BY symbol, time_bucket(INTERVAL '1 minute', ts);

SUBSCRIBE SELECT symbol, time_bucket(INTERVAL '1 second', ts) AS second,
       sum(price * size) / sum(size) AS vwap
FROM trades
GROUP BY symbol, time_bucket(INTERVAL '1 second', ts);
```

## Quotes

Canonical proposed DDL: [quotes](data-model.md#quotes).

- **Expected cardinality:** many more rows than trades; tens of thousands of symbols, many venues/channels, and extremely high sequence cardinality.
- **Ordering:** sequence numbers should be monotonic within a feed channel, while event time can tie or regress; cross-channel ordering is not assumed.
- **Out of order:** redundant feeds, packet recovery, and channel failover can reorder or replay quote messages. Sequence gaps must be observable rather than silently hidden.
- **Correction and deduplication:** the feed identity deduplicates exact retransmission. A venue correction or busted sequence is represented as a committed version or explicit action, never an in-place mutation with lost history.
- **Common projections:** best bid/ask, sizes, midpoint, spread, imbalance, venue, and event time.
- **Common filters:** symbol/venue, session and event-time range, crossed/locked states, spread thresholds, and sequence ranges.
- **Common aggregations:** time-weighted spread, depth imbalance, update rate, volatility inputs, and per-symbol/venue quality metrics.
- **Historical queries:** reconstruct top of book, ASOF-join trades with prevailing quotes, measure slippage, and compare venues.
- **Live queries:** current best quotes, rolling spread/imbalance, crossed-market alerts, and ASOF enrichment of new trades.
- **Stress patterns:** sustained high update rates, extreme hot-key skew, sequence gaps followed by replay, many small columns, and high-rate state replacement in live operators.

```sql
SELECT t.symbol, t.ts, t.price,
       q.bid_price, q.ask_price
FROM trades AS t
ASOF LEFT JOIN quotes AS q
  ON t.symbol = q.symbol AND t.venue = q.venue
 AND q.ts <= t.ts
WHERE t.ts BETWEEN TIMESTAMP '2026-01-02 14:30:00Z'
               AND TIMESTAMP '2026-01-02 21:00:00Z';
```

## Infrastructure metrics

Canonical proposed DDL: [metrics](data-model.md#metrics).

- **Expected cardinality:** millions to hundreds of millions of active series in large multi-tenant deployments; selected label dimensions are typed `SYMBOL` columns and values such as host can be very high cardinality.
- **Ordering:** agents commonly buffer and send batches ordered within one series, but batches from many agents interleave.
- **Out of order:** retries, mobile/edge disconnection, remote-write buffering, and clock issues introduce late samples. Per-tenant lateness policies must be explicit.
- **Correction and deduplication:** `(tenant, series_id, sample_id)` identifies a logical sample. Conflicting replacements require explicit correction intent; system time retains when each accepted version became visible.
- **Common projections:** `ts`, value, tenant, metric, host, region, and series identity.
- **Common filters:** tenant, metric, host/region, event-time range, and value thresholds.
- **Common aggregations:** rate, delta, sum, min/max, quantiles where supported, downsampling, and group-by label/window.
- **Historical queries:** capacity trends, incident comparison, SLO analysis, and high-resolution drill-down.
- **Live queries:** dashboard panels, alert conditions, rolling rates, and continuously updated aggregates.
- **Stress patterns:** high-cardinality churn, synchronized scrape bursts, broad fan-out by label, sparse series, retention expiration, hot tenants, and slow dashboard subscribers.

```sql
SELECT region,
       time_bucket(INTERVAL '1 minute', ts) AS minute,
       avg(value)
FROM metrics
WHERE tenant = CAST('acme' AS SYMBOL)
  AND metric = CAST('cpu.utilization' AS SYMBOL)
  AND ts >= TIMESTAMP '2026-01-02 00:00:00Z'
GROUP BY region, time_bucket(INTERVAL '1 minute', ts);
```

## Structured events

Canonical proposed DDL: [structured events](data-model.md#structured-events).

- **Expected cardinality:** billions of events with high-cardinality event, trace, and host values; service and severity are usually lower cardinality.
- **Ordering:** order is approximate across producers; even one trace may arrive in a different order from its causal execution.
- **Out of order:** queues, agent buffering, retries, and clock skew can delay events by seconds to days. Event-time and received-time lag must be observable.
- **Correction and deduplication:** `(tenant, event_id)` identifies a logical event; an identical retransmission is idempotent and an explicit replacement operation creates a new system-time version. Redaction policy is a separate future specification.
- **Common projections:** event time, service, severity, trace ID, host, and body.
- **Common filters:** tenant, event-time range, service, severity, host, trace ID, and later text-search predicates if adopted.
- **Common aggregations:** counts and rates by service/severity/host, trace counts, error ratios, and anomaly inputs.
- **Historical queries:** incident timelines, trace correlation, deployment comparisons, error exemplars, and system-time audit of corrected metadata.
- **Live queries:** tail by service, error-rate panels, trace/event correlation, and threshold or pattern alerts.
- **Stress patterns:** incident-driven bursts, oversized bodies, adversarial trace/host cardinality, sparse optional fields, selective point lookup mixed with broad scans, and subscriber fan-out.

```sql
SUBSCRIBE SELECT ts, service, severity, trace_id, body
FROM structured_events
WHERE tenant = CAST('acme' AS SYMBOL)
  AND severity IN (CAST('error' AS SYMBOL), CAST('fatal' AS SYMBOL))
ORDER BY ts;
```

## Cross-workload semantic pressure

These workloads require more than raw append throughput. Implementations must keep retry identity separate from sort keys; retain event-time meaning across corrections; assign committed system-time versions; bound queueing and subscription memory; expose gaps and rejected data; and define when window results are provisional, corrected, or final. Any benchmark derived from these examples must state the exact generated distribution, skew, batch size, lateness, correction rate, durability mode, and query concurrency.
