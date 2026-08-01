# ChronosDB

> **Pre-alpha — architecture phase.** ChronosDB currently consists of project and architecture documentation; no database engine, server, client, file format, or protocol is implemented yet.

ChronosDB is a greenfield, Linux-first distributed real-time analytical database planned primarily in C++23. It is intended to unite durable, low-latency ingestion of event-heavy data with historical columnar SQL, event-time-aware live analytics, system-time history, and resumable subscriptions—through purpose-built storage, query, networking, and replication subsystems rather than an existing database engine hidden behind a new interface.

## Intended workloads

- Financial market data: trades, quotes, order-book events, corrections, ASOF joins, real-time aggregates, and historical backtesting.
- Infrastructure observability: metrics, structured events, traces, high-cardinality labels, dashboards, and continuous aggregates.

Representative schemas and access patterns are documented in [Product workloads](docs/product/workloads.md).

## Historical-to-live differentiator

The signature planned capability is a gap-free historical-to-live query. A query will select a committed snapshot, return its historical results, and then stream committed changes strictly after the snapshot boundary. A deterministic resume token will allow reconnection at a committed boundary. The contract must prevent an omitted change during snapshot-to-stream handoff, but it will not claim general end-to-end exactly-once delivery: that requires transactionally controlling the external sink as well as the database.

## Intended architecture

```text
Clients
   │ native protocol / SQL
   ▼
epoll reactors ──bounded SPSC──► shard-owned tablets
                                      │
                         WAL / future Raft log
                                      │ commit
                         mutable columnar heads ──► live operators
                                      │ flush
                                      ▼
                             immutable CSEG parts
                                      │
                              manifest + compaction
                                      │
SQL ─► parse ─► bind ─► optimize ─► snapshot scans ─► vectorized results
```

The diagram is an accepted architectural direction, not a diagram of implemented components. See the [architecture overview](docs/architecture/overview.md) for boundaries and deferred decisions.

## Current status

Phase 0 is in progress: product semantics, invariants, durable-format contracts, protocol contracts, and architectural decisions must be specified before implementation begins. The present repository establishes the constitution, vision, workload models, architecture outline, invariants, non-goals, roadmap, and ADR process. It contains no implementation and publishes no benchmark results.

## Planned technical depth

The roadmap calls for a checksummed segmented WAL and idempotent recovery; append-only mutable columnar heads; immutable sorted CSEG parts; manifests, flush, checkpointing, indexes, delta parts, and compaction; a custom SQL front end and vectorized engine; epoll-first event-driven networking; live queries and incremental materialized views; system-time history; deterministic Raft and multi-Raft tablets; distributed execution; and later object-storage tiering. Fuzzing, differential testing, crash testing, sanitizers, deterministic simulation, and reproducible measurements are exit criteria rather than post-release decoration.

## Documentation

- [Documentation index](docs/README.md)
- [Architecture overview](docs/architecture/overview.md)
- [Non-negotiable invariants](docs/architecture/invariants.md)
- [Roadmap and phase gates](docs/roadmap.md)
- [Product workloads](docs/product/workloads.md)
- [ADR index and process](docs/adr/README.md)
