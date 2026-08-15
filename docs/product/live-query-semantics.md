# Live Query Semantics

> **Status: single-source core, durable views, and Protocol 1.1 delivery implemented; service
> integration incomplete.** Source-tagged Resume Token v2 issuance with v1 WAL compatibility,
> bounded register-before-boundary buffering,
> poll/acknowledge/resume, overflow, incremental windows, exact durable view recovery, and negotiated
> native subscription delivery are implemented. A plan-bound multi-tablet coordinator records one
> replayable delivery order over exact per-tablet positions. An already-lowered single-tablet
> physical plan now executes against the exact registered storage boundary and emits Protocol 1.1
> snapshot batches through READY. Single-source `SUBSCRIBE SELECT` now has exact bounded planning
> and schema-bound identity. Durable plan lookup and multi-tablet snapshot execution are not yet
> wired. The contract provides at-least-once external delivery, not exactly-once effects.

Eligible SQL and row visibility follow [SQL v1](sql-v1.md), the [data model](data-model.md), and the [consistency contract](consistency-and-durability.md).

## Handoff protocol

For a new subscription, ChronosDB logically performs these ordered steps:

1. Bind the query against a schema version and register subscription state.
2. Select a committed source snapshot position vector `C` (one position per relevant tablet; one scalar position on a single node).
3. Pin or buffer the committed change stream strictly after `C` before any handoff race can open.
4. Execute the historical query at exactly `C`.
5. Deliver the finite snapshot as `SNAPSHOT_ROW` records, followed by `SNAPSHOT_END` containing `C` and an initial resume token.
6. Deliver every matching committed change strictly after its component of `C`, ordered by the
   single coordinator's recorded admission order. This is a subscription delivery order, not a
   fictional total commit order across tablets.
7. Continue live delivery while retention, schema, resources, and client state permit.
8. Issue resumable checkpoints tied to the query-plan identity, schema identity, committed source position vector, and last deterministic delivery sequence.

Releasing the post-`C` pin before buffered changes are safely owned is forbidden. A disconnect before `SNAPSHOT_END` may replay the entire snapshot on registration retry. After a valid resume token, replay begins at or before the next unacknowledged sequence; duplicates are allowed, omission is not.

## Delivery records

- **Snapshot row:** one row of the finite result at `C`, tagged `SNAPSHOT_ROW`. Snapshot order follows SQL `ORDER BY` when present; otherwise order has no relational meaning even if transport ordinals are stable.
- **Change record:** a versioned envelope containing schema identity, sequence number, committed
  source position, operation, result key, and payload where applicable. The active request and its
  bound resume token carry subscription/query-plan identity; an explicit acknowledgement returns a
  new resumable checkpoint.
- **`UPSERT`:** replace or create the current result value for a result key. Reapplying the same or an older sequence is an idempotent consumer no-op.
- **`DELETE`:** remove the current result value for a result key. Deleting an absent key is an idempotent no-op.
- **Result key:** source logical row identity for row subscriptions, or the encoded grouping keys plus window bounds for aggregates.
- **Sequence number:** a monotonically increasing unsigned delivery ordinal within one query-plan/subscription history. It deterministically orders changes and deduplicates replay; it is not an event-time or database-wide commit position.

External delivery is at least once. Consumers obtain idempotent effects by persisting `(query_plan_identity, result_key, sequence_number)` with their output or by applying only a greater sequence per key. ChronosDB cannot claim exactly-once effects unless it transactionally controls the sink and acknowledgment.

## Time and window terms

A **watermark** is event-time progress, not commit visibility. For a window `[start, end)`, the configured **allowed lateness** defines the expected revision interval after `end`. A window is **finalized** when the effective watermark is at or beyond `end + allowed_lateness`. Finalization means no ordinary arrival is expected under the declared policy; it does not erase retained source history or forbid an accepted correction.

An emitted value is marked:

- `provisional` before finalization;
- `finalized` when the watermark crosses the finalization boundary; or
- `corrected` when an accepted late version, replacement, or tombstone changes a previously emitted value. A corrected already-finalized window remains finalized and carries an increased result revision.

If required source/operator state has expired, the server must either recompute from retained source data within an explicit bound or terminate with `LIVE_STATE_EXPIRED`; it cannot silently ignore the change.

## Event reactions

| Input | Required live behavior |
| --- | --- |
| Normal event | Apply after commit, update affected result keys/windows, and emit `UPSERT`/`DELETE` only if the result changes. |
| Late before finalization | Apply to the open window and emit a new provisional `UPSERT` with a greater sequence. |
| Late after an initial result | Recompute/update the affected result and emit `UPSERT` marked `corrected`; finalization status follows the watermark. |
| Correction | Remove the prior visible version's contribution and add the replacement's contribution, possibly across different windows if event time changed; emit each changed key. |
| Tombstone | Remove the prior visible contribution and emit a replacement aggregate `UPSERT` or result-key `DELETE`. |
| Replay after restart | Restore source/operator progress from durable state, replay committed changes in source order, and redeliver from the last safe checkpoint. Sequences and results must match the original logical history; an acknowledgment race may duplicate records. |

## Aggregate examples

All examples use half-open one-second windows and a deterministic event tie-break from the [data model](data-model.md).

- **VWAP:** state is exact `sum(price * size)` and `sum(size)` using sufficient decimal precision. Output is numerator/denominator; zero denominator yields NULL. Correction/tombstone subtracts the old contribution then adds the new one.
- **OHLC:** open/close are the first/last price by `(event_time, physical ordering key, row-version identity)`; high/low are extrema of visible prices. Removing an extremum or endpoint requires retained ordered state or exact recomputation, never an approximate patch.
- **Welford variance:** maintain `(count, mean, M2)` using a specified floating implementation; merges use Welford/Chan formulas. Replacement/removal uses a mathematically valid inverse or exact window recomputation. Numerical tolerance and reproducibility across parallel merge shapes remain a lower-level specification.
- **Count:** add one for a newly visible contributing row and subtract one for its tombstone/replacement removal. `COUNT(expr)` excludes NULL.
- **Sum:** add the new visible value and subtract the old value for correction/tombstone; integer/decimal overflow follows SQL error semantics, and floating NaN follows [SQL v1](sql-v1.md#deterministic-semantics).

Example logical stream for a one-second VWAP result key `(AAPL, 10:00:00Z)`:

```text
seq 41  UPSERT vwap=100.10 provisional
seq 42  UPSERT vwap=100.12 corrected, provisional   # late trade
seq 47  UPSERT vwap=100.12 finalized
seq 53  UPSERT vwap=100.09 corrected, finalized    # delayed correction
```

These are semantic examples, not measured output or a final wire encoding.

## Resume, backpressure, and lifecycle

A resume token is opaque, versioned, integrity-protected, and scoped to database/tablet epochs, query-plan identity, bound schema, source position vector, and safe sequence. A token is valid only while all required source changes and plan/schema semantics remain available.

- **Reconnect:** validate the token, restore the same plan identity, and redeliver from the safe sequence. The client deduplicates repeated records.
- **Expired token:** return `RESUME_TOKEN_EXPIRED` with the earliest available source boundary when safe to disclose. Never silently start at “now” or claim continuity.
- **Backpressure:** per-subscription inbound state, operator output, and socket buffers are bounded. Backpressure may pause that subscriber but cannot indefinitely stop tablet ingestion.
- **Overflow:** v1's required safe policy is disconnect-with-resume at the last retained safe token. Bounded spill or lossy sampling are post-v1 policies and require explicit opt-in; lossy output cannot claim gap-free delivery.
- **Cancellation:** idempotently stop new output, release buffers/pins, and return the last safe token if available. It does not roll back committed source writes.
- **Schema change:** v1 terminates an affected subscription at a committed boundary with `SCHEMA_CHANGED`. The old token cannot bind to a different plan; the client registers a new plan/snapshot. A future compatibility analysis may allow provably irrelevant additive changes.

Topology transitions, state-retention defaults, window trigger cadence, spill, and the complete
eligible incremental SQL subset remain deferred. Durable fingerprint-to-plan lookup, multi-tablet
snapshot execution, durable coordinator checkpoint installation, single-source SQL planning,
single-tablet plan-bound snapshot execution, multi-tablet delivery ordering, Resume Token v1/v2 bytes,
the Protocol 1.1 acknowledgement/checkpoint lifecycle, exact logical coordinator restoration, and
versioned coordinator checkpoint bytes are implemented. Topology-bound subscription retention and
verified physical WAL-prefix reclamation are implemented for fixed WAL-backed source sets; Raft
checkpoint/protocol integration, prefix mapping, and dynamic plan-owner retirement remain deferred.
Every later choice must preserve
[invariant 17](../architecture/invariants.md).
