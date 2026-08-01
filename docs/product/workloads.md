# Representative Workloads

The schemas and SQL below are **illustrative proposed contracts**, not implemented syntax. Names, types, keys, correction clauses, subscription syntax, and functions require later SQL and protocol specifications. Cardinalities describe design envelopes, not benchmark claims or guaranteed limits.

All tables designate one event-time column. System time is database-managed and distinct from fields such as `received_at`, which represent source or ingestion metadata.

## Trades

```sql
CREATE TABLE trades (
    venue          TEXT,
    symbol         TEXT,
    trade_id       TEXT,
    event_time     TIMESTAMP_NS EVENT_TIME,
    received_at    TIMESTAMP_NS,
    price          DECIMAL(20, 8),
    quantity       DECIMAL(20, 8),
    side           TEXT,
    conditions     ARRAY<TEXT>,
    correction_seq UINT32,
    PRIMARY KEY (venue, symbol, trade_id)
);
```

- **Expected cardinality:** tens of thousands of symbols across venues; billions to trillions of retained events in large deployments; `trade_id` is high cardinality.
- **Ordering:** feeds are usually close to event-time order per venue/symbol but can interleave across sessions, connections, and symbols.
- **Out of order:** network paths, venue retransmission, and feed arbitration can deliver older trades after newer ones. Policies use watermarks and allowed lateness rather than assuming monotonic timestamps.
- **Correction and deduplication:** `(venue, symbol, trade_id)` is the logical input identity; a greater `correction_seq` creates a new system-time version, while an identical retry is idempotent. Cancellation semantics need a future schema/SQL decision.
- **Common projections:** event time, symbol, price, quantity, side, venue, and notional (`price * quantity`).
- **Common filters:** symbol/venue sets, event-time ranges, price or size thresholds, side, and conditions.
- **Common aggregations:** volume, notional, VWAP, trade count, OHLC, realized volatility inputs, and time bars.
- **Historical queries:** backtests, session summaries, liquidity studies, trade replay, and “as known at system time” reconstruction.
- **Live queries:** rolling VWAP, per-symbol bars, unusual-size alerts, and historical results followed by committed updates.
- **Stress patterns:** bursts at market open or news events, hot symbols, tiny batches mixed with recovery bursts, late corrections to closed windows, and long scans concurrent with ingest.

```sql
SELECT symbol,
       tumble(event_time, INTERVAL '1 minute') AS minute,
       sum(price * quantity) / sum(quantity) AS vwap
FROM trades
WHERE event_time >= TIMESTAMP '2026-01-02 14:30:00Z'
GROUP BY symbol, minute;

SUBSCRIBE AFTER SNAPSHOT
SELECT symbol, hop(event_time, INTERVAL '1 second', INTERVAL '5 minutes'),
       sum(price * quantity) / sum(quantity) AS rolling_vwap
FROM trades
GROUP BY symbol, hop(...);
```

## Quotes

```sql
CREATE TABLE quotes (
    venue          TEXT,
    symbol         TEXT,
    channel_id     UINT16,
    sequence_no    UINT64,
    event_time     TIMESTAMP_NS EVENT_TIME,
    received_at    TIMESTAMP_NS,
    bid_price      DECIMAL(20, 8),
    bid_size       DECIMAL(20, 8),
    ask_price      DECIMAL(20, 8),
    ask_size       DECIMAL(20, 8),
    flags          UINT32,
    PRIMARY KEY (venue, channel_id, sequence_no)
);
```

- **Expected cardinality:** many more rows than trades; tens of thousands of symbols, many venues/channels, and extremely high sequence cardinality.
- **Ordering:** sequence numbers should be monotonic within a feed channel, while event time can tie or regress; cross-channel ordering is not assumed.
- **Out of order:** redundant feeds, packet recovery, and channel failover can reorder or replay quote messages. Sequence gaps must be observable rather than silently hidden.
- **Correction and deduplication:** the feed identity deduplicates exact retransmission. A venue correction or busted sequence is represented as a committed version or explicit action, never an in-place mutation with lost history.
- **Common projections:** best bid/ask, sizes, midpoint, spread, imbalance, venue, and event time.
- **Common filters:** symbol/venue, session and event-time range, crossed/locked states, spread thresholds, and flags.
- **Common aggregations:** time-weighted spread, depth imbalance, update rate, volatility inputs, and per-symbol/venue quality metrics.
- **Historical queries:** reconstruct top of book, ASOF-join trades with prevailing quotes, measure slippage, and compare venues.
- **Live queries:** current best quotes, rolling spread/imbalance, crossed-market alerts, and ASOF enrichment of new trades.
- **Stress patterns:** sustained high update rates, extreme hot-key skew, sequence gaps followed by replay, many small columns, and high-rate state replacement in live operators.

```sql
SELECT t.symbol, t.event_time, t.price,
       q.bid_price, q.ask_price
FROM trades AS t
ASOF LEFT JOIN quotes AS q
  ON t.symbol = q.symbol AND t.venue = q.venue
 AND q.event_time <= t.event_time
WHERE t.event_time BETWEEN :start AND :end;
```

## Infrastructure metrics

```sql
CREATE TABLE metrics (
    tenant_id      TEXT,
    metric_name    TEXT,
    labels         MAP<TEXT, TEXT>,
    series_hash    UINT128,
    event_time     TIMESTAMP_NS EVENT_TIME,
    received_at    TIMESTAMP_NS,
    value          FLOAT64,
    sample_id      TEXT,
    PRIMARY KEY (tenant_id, series_hash, event_time, sample_id)
);
```

- **Expected cardinality:** millions to hundreds of millions of active series in large multi-tenant deployments; label keys are moderate cardinality and label values can be very high cardinality.
- **Ordering:** agents commonly buffer and send batches ordered within one series, but batches from many agents interleave.
- **Out of order:** retries, mobile/edge disconnection, remote-write buffering, and clock issues introduce late samples. Per-tenant lateness policies must be explicit.
- **Correction and deduplication:** `sample_id` makes retries idempotent. Conflicting samples at the same series/event time require a defined version policy; system time retains when each accepted version became visible.
- **Common projections:** event time, value, selected labels, metric name, and series identity.
- **Common filters:** tenant, metric name, indexed label predicates, event-time range, and value thresholds.
- **Common aggregations:** rate, delta, sum, min/max, quantiles where supported, downsampling, and group-by label/window.
- **Historical queries:** capacity trends, incident comparison, SLO analysis, and high-resolution drill-down.
- **Live queries:** dashboard panels, alert conditions, rolling rates, and continuously updated aggregates.
- **Stress patterns:** high-cardinality churn, synchronized scrape bursts, broad fan-out by label, sparse series, retention expiration, hot tenants, and slow dashboard subscribers.

```sql
SELECT labels['region'] AS region,
       tumble(event_time, INTERVAL '1 minute') AS minute,
       avg(value)
FROM metrics
WHERE tenant_id = :tenant
  AND metric_name = 'cpu.utilization'
  AND event_time >= :start
GROUP BY region, minute;
```

## Structured events

```sql
CREATE TABLE events (
    tenant_id      TEXT,
    event_id       UUID,
    event_time     TIMESTAMP_NS EVENT_TIME,
    received_at    TIMESTAMP_NS,
    service        TEXT,
    environment    TEXT,
    severity       TEXT,
    trace_id       BYTES,
    attributes     MAP<TEXT, TEXT>,
    body            TEXT,
    revision       UINT32,
    PRIMARY KEY (tenant_id, event_id)
);
```

- **Expected cardinality:** billions of events with high-cardinality event, trace, request, user, host, and attribute values; service/environment/severity are usually lower cardinality.
- **Ordering:** order is approximate across producers; even one trace may arrive in a different order from its causal execution.
- **Out of order:** queues, agent buffering, retries, and clock skew can delay events by seconds to days. Event-time and received-time lag must be observable.
- **Correction and deduplication:** `(tenant_id, event_id)` identifies a logical event; repeated revisions are idempotent and a higher `revision` creates a new system-time version. Redaction or deletion policies are a separate future specification.
- **Common projections:** time, service, severity, trace ID, selected attributes, and body.
- **Common filters:** tenant, event-time range, service, environment, severity, trace ID, selected attribute predicates, and later text-search predicates if adopted.
- **Common aggregations:** counts and rates by service/severity, unique estimates, error ratios, latency histograms derived from attributes, and anomaly inputs.
- **Historical queries:** incident timelines, trace correlation, deployment comparisons, error exemplars, and system-time audit of corrected metadata.
- **Live queries:** tail by service, error-rate panels, trace/event correlation, and threshold or pattern alerts.
- **Stress patterns:** incident-driven bursts, oversized bodies or attribute maps, adversarial cardinality, sparse optional fields, selective point lookup mixed with broad scans, and subscriber fan-out.

```sql
SUBSCRIBE AFTER SNAPSHOT
SELECT event_time, service, severity, trace_id, body
FROM events
WHERE tenant_id = :tenant
  AND environment = 'production'
  AND severity IN ('error', 'fatal')
ORDER BY event_time;
```

## Cross-workload semantic pressure

These workloads require more than raw append throughput. Implementations must keep retry identity separate from sort keys; retain event-time meaning across corrections; assign committed system-time versions; bound queueing and subscription memory; expose gaps and rejected data; and define when window results are provisional, corrected, or final. Any benchmark derived from these examples must state the exact generated distribution, skew, batch size, lateness, correction rate, durability mode, and query concurrency.
