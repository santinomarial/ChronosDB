# Data Model Contract

> **Status: specified; logical schema foundation implemented.** This document defines the logical
> contract. Logical type and schema identity/evolution rules are frozen by
> [ADR 0014](../adr/0014-logical-types-schema-identity-and-evolution.md), and ingestion bytes are
> frozen by [columnar-batch v1](../formats/columnar-batch-v1.md). CSEG and catalog encodings remain
> deferred to their roadmap phases and [accepted ADRs](../adr/README.md). The `chronos_schema`
> target implements the in-memory identity/type/schema/lineage subset; table storage remains absent.

## Core table concepts

An event table has typed columns with explicit nullability. `NOT NULL` rejects a row whose value is absent; nullable columns store SQL `NULL`, which is distinct from zero, an empty string, and NaN.

Each table declares:

- exactly one **event-time column**, used for event-time windows, ASOF relationships, lateness, and time partitioning;
- an optional **receive-time column**, supplied or assigned at ingress for operational lag analysis but never used as the commit order;
- hidden **system commit metadata**: a commit position, system timestamp mapping, row-version ordinal, operation kind, and stable row-version identity sufficient to reproduce snapshot visibility;
- a **physical ordering key**, whose prefix contains workload dimensions and event time and whose full value deterministically orders rows within a part;
- a **time partition expression** that maps event time to a bounded partition domain;
- a **tablet/sharding key** that places logical rows and all versions of the same deduplication identity on one tablet;
- a **deduplication key** defining logical row identity; omission means each accepted input row has a generated identity and cannot be corrected by key;
- a **retention policy** for event data;
- a **system-history retention** policy for superseded versions and tombstones;
- **allowed lateness**, used by live event-time operators and distinct from storage retention; and
- in a future cluster, a **replication factor** stored in cluster metadata rather than encoded into table rows or CSEG parts.

The sharding expression must be compatible with the deduplication key: all versions of one logical row must route to the same tablet. A table specification is invalid if it cannot guarantee that property. Physical ordering is an access-layout decision, not a uniqueness constraint.

The accepted [WAL v1](../formats/wal-v1.md) `record_sequence`, paired with its `wal_id`, supplies a
single-node physical total order for application entries. The accepted
[columnar append command](../architecture/columnar-ingestion.md#columnar-append-command-v1) encodes
the tablet, client-batch identity, digest, outcome, operation kind, and mutation needed to derive
the logical per-tablet commit position and replay this data model. WAL record sequence is not event
time, receive time, or a complete row-version identity by itself.

The illustrative DDL clauses below are part of the planned [SQL v1](sql-v1.md) contract. Exact catalog storage and CSEG encodings remain deferred.

## Initial logical types

All integer and fixed-size durable representations use explicit widths and byte order. SQL display is independent of the durable encoding.

| Type | Proposed logical semantics |
| --- | --- |
| `BOOL` | `TRUE`, `FALSE`, or `NULL`; no implicit integer conversion. |
| `INT8`, `INT16`, `INT32`, `INT64` | Signed two's-complement value ranges of the named width; arithmetic overflow is an error. |
| `UINT8`, `UINT16`, `UINT32`, `UINT64` | Unsigned value ranges of the named width; negative literals require an explicit valid cast and overflow is an error. |
| `FLOAT32`, `FLOAT64` | IEEE 754 binary32/binary64 values with the NaN and ordering rules in [SQL v1](sql-v1.md). |
| `DECIMAL(p, s)` | Exact signed base-10 value with precision `1..38`, scale `0..p`, and no silent rounding on ingest; values outside precision are errors. |
| `TIMESTAMP_NS` | Signed 64-bit nanoseconds from Unix epoch in UTC. It has no implicit session time zone. |
| `DATE` | Proleptic Gregorian calendar date represented independently of time zone. |
| `SYMBOL` | A typed categorical string. SQL equality, comparison, hashing, and output use its UTF-8 value; dictionary identifiers are table/part-local physical details and may never affect equality across dictionaries. |
| `STRING` | Length-delimited valid UTF-8 text; default comparison is binary Unicode-code-point byte order until collations are specified. |
| `BINARY` | Length-delimited uninterpreted bytes with lexicographic byte comparison. |
| `UUID` | A 128-bit UUID value with canonical textual input/output and lexicographic unsigned-byte ordering. |

The first typed-table implementation phase requires every type above except `FLOAT32`; financial schemas require `DECIMAL`, and observability schemas require `FLOAT64`. `FLOAT32` storage/execution may follow once scalar and codec correctness for the required set is established. Collations, time-zone-aware timestamps, arrays, maps, JSON, enums beyond `SYMBOL`, and wider integers are post-v1 extensions.

## Canonical proposed schemas

The clauses are intentionally explicit. Intervals shown are illustrative policy values, not universal defaults.

### Trades

```sql
CREATE TABLE trades (
    ts       TIMESTAMP_NS NOT NULL,
    recv_ts  TIMESTAMP_NS,
    symbol   SYMBOL NOT NULL,
    venue    SYMBOL NOT NULL,
    price    DECIMAL(20, 8) NOT NULL,
    size     DECIMAL(20, 8) NOT NULL,
    trade_id STRING NOT NULL,
    flags    UINT64 NOT NULL
)
EVENT TIME ts
ORDER KEY (symbol, venue, ts, trade_id)
PARTITION BY time_bucket(INTERVAL '1 day', ts)
SHARD KEY (symbol)
DEDUP KEY (symbol, venue, trade_id)
RETENTION INTERVAL '1825 days'
SYSTEM HISTORY RETENTION INTERVAL '365 days'
ALLOWED LATENESS INTERVAL '10 seconds';
```

### Quotes

```sql
CREATE TABLE quotes (
    ts        TIMESTAMP_NS NOT NULL,
    recv_ts   TIMESTAMP_NS,
    symbol    SYMBOL NOT NULL,
    venue     SYMBOL NOT NULL,
    bid_price DECIMAL(20, 8) NOT NULL,
    bid_size  DECIMAL(20, 8) NOT NULL,
    ask_price DECIMAL(20, 8) NOT NULL,
    ask_size  DECIMAL(20, 8) NOT NULL,
    sequence  UINT64 NOT NULL
)
EVENT TIME ts
ORDER KEY (symbol, venue, ts, sequence)
PARTITION BY time_bucket(INTERVAL '1 day', ts)
SHARD KEY (symbol)
DEDUP KEY (symbol, venue, sequence)
RETENTION INTERVAL '730 days'
SYSTEM HISTORY RETENTION INTERVAL '90 days'
ALLOWED LATENESS INTERVAL '5 seconds';
```

### Metrics

```sql
CREATE TABLE metrics (
    ts          TIMESTAMP_NS NOT NULL,
    recv_ts     TIMESTAMP_NS,
    tenant      SYMBOL NOT NULL,
    metric      SYMBOL NOT NULL,
    series_id   UUID NOT NULL,
    host        SYMBOL,
    region      SYMBOL,
    value       FLOAT64 NOT NULL,
    sample_id   UUID NOT NULL
)
EVENT TIME ts
ORDER KEY (tenant, metric, series_id, ts, sample_id)
PARTITION BY time_bucket(INTERVAL '1 hour', ts)
SHARD KEY (tenant, series_id)
DEDUP KEY (tenant, series_id, sample_id)
RETENTION INTERVAL '30 days'
SYSTEM HISTORY RETENTION INTERVAL '7 days'
ALLOWED LATENESS INTERVAL '2 minutes';
```

### Structured events

```sql
CREATE TABLE structured_events (
    ts          TIMESTAMP_NS NOT NULL,
    recv_ts     TIMESTAMP_NS,
    tenant      SYMBOL NOT NULL,
    event_id    UUID NOT NULL,
    service     SYMBOL NOT NULL,
    severity    SYMBOL NOT NULL,
    trace_id    BINARY,
    host        SYMBOL,
    body        STRING NOT NULL
)
EVENT TIME ts
ORDER KEY (tenant, service, ts, event_id)
PARTITION BY time_bucket(INTERVAL '1 day', ts)
SHARD KEY (tenant)
DEDUP KEY (tenant, event_id)
RETENTION INTERVAL '90 days'
SYSTEM HISTORY RETENTION INTERVAL '30 days'
ALLOWED LATENESS INTERVAL '10 minutes';
```

## Row and version semantics

- **Logical row identity:** the typed tuple produced by `DEDUP KEY`; it is stable across corrections and tombstones.
- **Physical row version:** an immutable committed representation of one logical identity at one system commit position. Multiple versions may share event time.
- **Replacement version:** a later committed version that supersedes an earlier visible version without changing logical identity. Its event time may be corrected.
- **Tombstone:** a version that makes the logical row absent from current snapshots at or after its commit position while preserving earlier system-time views until retention permits removal.
- **Snapshot visibility:** for boundary `C`, consider only versions committed at or before `C`; for each identity, expose the greatest commit position and suppress it if it is a tombstone.
- **Current view:** snapshot visibility at the query's newly acquired committed boundary.
- **System-time historical view:** the same rule at the committed boundary resolved by `FOR SYSTEM_TIME AS OF`.

Retention of current event data does not automatically authorize deletion of superseded versions. A version becomes compaction-eligible only when it is not visible to any active snapshot, retained system-time boundary, recovery/subscription position, backup, or other declared pin, and when both event and system-history retention permit removal. Compaction may relocate or re-encode eligible versions but cannot change the result at any retained boundary.

### Concrete correction timeline

| System event | Commit boundary | Event data | Visible result |
| --- | --- | --- | --- |
| Original trade accepted at 10:00:01 | `C=120` | identity `(XNYS, T-7)`, `ts=10:00:00`, price `100.00` | Current query at `C=120` returns `100.00`. |
| Query starts at 10:00:03 | snapshot `C=125` | correction not yet committed | Query returns original `100.00`. |
| Delayed correction accepted at 10:00:05 | `C=131` | same identity and event time, price `100.25` | New version supersedes the original. |
| Current query starts at 10:00:06 | snapshot `C=134` | both physical versions retained | Query returns `100.25`. |
| `FOR SYSTEM_TIME AS OF` resolves to `C=125` | historical snapshot `C=125` | correction is later than boundary | Query reproduces `100.00`. |

A later tombstone at `C=140` makes the current view empty for that identity, while a system-time query at `C=134` still returns `100.25` if history remains retained.

## Late-data classification

Classification uses event time relative to a tablet's event-time frontier and configured reorder horizon; it never changes system commit order.

- **In-order data:** at or beyond the active frontier. It follows the normal mutable-head append/build path.
- **Bounded out-of-order data:** behind the frontier but within the reorder horizon. It enters the active generation's logical reorder stage so sealing can produce sorted output. The committed version remains snapshot-visible while awaiting final ordering.
- **Outside the reorder horizon:** too old for the active generation. It enters a bounded delta-part build path and is flushed as a sorted immutable delta part, then merged by compaction. It is not discarded merely for being late.
- **Duplicate retransmission:** same logical identity and canonical payload within the supported deduplication/idempotency horizon. It is an idempotent no-op and returns the prior outcome.
- **Correction:** same logical identity with an explicitly authorized replacement operation and a new payload; it creates a physical version. Conflicting reuse without correction intent is an error.
- **Invalid event:** fails type, nullability, range, schema, event-time, size, identity, or authorization rules and is rejected without a visible version.

The reorder stage and delta builder must be query-visible through a stable snapshot after commit, bounded in memory, and owned by the tablet shard. This document does not prescribe their final in-memory containers, sort algorithm, or horizon values.

## Deferred parameters

Catalog encoding, generated-identity representation, correction/tombstone write syntax, default
retention and lateness, system timestamp assignment, CSEG type encodings, collation, partition
evolution, tablet hashing, and cross-tablet temporal coordination remain deferred. The initial WAL
application body and ingest dictionary scope are fixed by
[ADR 0015](../adr/0015-columnar-batch-v1-and-wal-append-command.md) and
[columnar-batch v1](../formats/columnar-batch-v1.md). Replication factor is unavailable until the
cluster phases.
