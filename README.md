# ChronosDB

> **Pre-alpha — architecture-phase implementation.** The accepted single-node foundation through
> Phase 10 is implemented and tested, including WAL/recovery, columnar storage, the supported SQL
> and vectorized-query surface, and a bounded native networking library. ChronosDB is not a
> production server: there is no packaged daemon, maintained TLS backend, distributed execution,
> replication, or Phase 11 live-query implementation.

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

Phases 0 through 10 have reached their documented development boundaries. The current tree
contains the checksummed WAL and recovery path, append-only mutable and immutable columnar storage,
manifest/checkpoint/compaction machinery, the explicitly supported SQL v1 and bounded vectorized
execution surface, and Protocol v1 with a portable client/session layer plus the Linux `epoll`
reactor. The networking layer is embeddable library code: plaintext is loopback-only and
`TLS_REQUIRED` fails closed because no maintained TLS record backend is implemented. Published
measurements are reproducible subsystem baselines, not production capacity or service-level claims.
See the [roadmap](docs/roadmap.md) and [Phase 10 exit review](docs/reviews/phase-10-network-review.md)
for the exact implemented and deferred boundaries.

## Build and test

Prerequisites and platform details are in the [building guide](docs/development/building.md). With
CMake 3.25+, Ninja, a C++23 compiler, and Git installed:

```sh
cmake --preset dev
cmake --build --preset dev
ctest --preset dev

build/dev/chronosctl version
build/dev/chronosctl version --json
```

Run the normal local format, build, test, and static-analysis sequence with `scripts/check.sh`.
Sanitizer and release commands are documented in the building guide.

## Planned technical depth

The roadmap calls for a checksummed segmented WAL and idempotent recovery; append-only mutable columnar heads; immutable sorted CSEG parts; manifests, flush, checkpointing, indexes, delta parts, and compaction; a custom SQL front end and vectorized engine; epoll-first event-driven networking; live queries and incremental materialized views; system-time history; deterministic Raft and multi-Raft tablets; distributed execution; and later object-storage tiering. Fuzzing, differential testing, crash testing, sanitizers, deterministic simulation, and reproducible measurements are exit criteria rather than post-release decoration.

## Documentation

- [Documentation index](docs/README.md)
- [Architecture overview](docs/architecture/overview.md)
- [Non-negotiable invariants](docs/architecture/invariants.md)
- [Roadmap and phase gates](docs/roadmap.md)
- [Building and testing](docs/development/building.md)
- [Development tooling](docs/development/tooling.md)
- [Common binary foundations](docs/learning/common-binary-foundations.md)
- [Durable POSIX I/O foundations](docs/learning/posix-io.md)
- [Product workloads](docs/product/workloads.md)
- [ADR index and process](docs/adr/README.md)
