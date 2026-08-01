# Product Vision

## Problem

Event-heavy systems need to ingest continuously, answer historical questions over large time ranges, and update live results as committed events arrive. Today those responsibilities are often split across a transactional ingest service, stream processor, analytical database, cache, and bespoke handoff logic. That fragmentation makes event-time corrections, durability boundaries, replay, and the transition from a historical answer to a live feed difficult to reason about.

ChronosDB aims to provide one purpose-built analytical engine for durable event ingestion, columnar historical queries, and committed live updates. It is a greenfield Linux-first systems project planned primarily in C++23, not a compatibility layer over an existing database.

## Primary users and workloads

The primary users are engineers and analysts operating event-driven systems where freshness, replayability, time semantics, and sustained analytical throughput matter:

- market-data engineers, quantitative researchers, and trading infrastructure teams working with trades, quotes, order books, corrections, ASOF relationships, and backtests;
- observability platform engineers and SRE teams working with metrics, traces, structured events, high-cardinality labels, dashboards, and continuous aggregates; and
- systems engineers who need explicit, testable storage, recovery, and delivery contracts rather than opaque best-effort behavior.

The initial workload definitions are in [workloads.md](workloads.md), with canonical typed table semantics in the [data-model contract](data-model.md).

## Why historical SQL and live analytics belong together

A live dashboard or signal normally starts with state derived from history and then incorporates new events. Running both halves in one engine allows them to share schema rules, expression semantics, event-time policies, committed ordering, and incremental operator state. Most importantly, it permits a snapshot and its continuation stream to be anchored to the same committed position. That eliminates an application-managed race in which changes can fall between a historical query and subscription setup.

This union does not make batch and streaming semantics identical. Historical scans can evaluate a stable snapshot over heads and immutable parts; live operators must also handle backpressure, late data, corrections, and replay. The architecture keeps those execution paths distinct while giving them a common commit boundary and query model.

## Two dimensions of time

**Event time** answers when an event happened in the source domain. Windows, OHLC bars, ASOF joins, trace timelines, and late-event policies depend on it. Producers can send event time out of order and may later correct it.

**System time** answers when a version became committed and visible in ChronosDB. It provides reproducible audit and “as known then” queries. A correction can retain the business meaning of the original event time while creating a new system-time version. Ingestion time may aid operations, but it is neither event time nor the visibility order.

## Differentiators

- **Gap-free historical-to-live queries:** acquire a committed snapshot, return its results, then stream changes after exactly that commit boundary. Versioned deterministic resume tokens identify restart positions.
- **Explicit temporal semantics:** typed relational tables designate one event-time column while retaining separate system commit history.
- **Storage shaped for events:** append-only mutable columnar heads for recent data and immutable, sorted, compressed CSEG parts for durable analytical scans.
- **Purpose-built systems depth:** custom durable formats, recovery, query processing, incremental execution, networking, and later consensus are developed and validated directly.
- **Measured architecture:** performance claims require reproducible benchmarks and published methodology; correctness contracts are not weakened to manufacture a number.

The historical-to-live contract prevents database-side gaps and supports deterministic replay. It does not by itself provide general end-to-end exactly-once effects in an external sink. That claim is possible only when the sink and acknowledgment protocol participate in a suitable transaction.

## Implementation principles

1. Validate a complete single-node storage and query engine before adding distribution.
2. Prioritize correctness, recoverability, and debuggability before performance and breadth.
3. Specify and version file formats and protocols before depending on them.
4. Preserve a clear durability and visibility boundary: acknowledged guarantees name a durability mode, and readers see only committed state.
5. Give each mutable tablet exactly one shard-worker owner; use bounded handoff queues and explicit backpressure.
6. Use immutable installation and replacement for durable parts; make recovery repeatable.
7. Use locks where they simplify cold control-plane code. Apply lock-free techniques only with a documented need and memory-ordering proof.
8. Keep hot paths allocation-conscious with purpose-specific allocation strategies, not one universal allocator.
9. Measure before optimizing, and test failure behavior as a first-class API contract.

## Single-node milestone

The single-node milestone succeeds when the following are true together:

- checksummed segmented WAL recovery, CSEG storage, manifests, checkpoints, flush, and compaction preserve the documented invariants under crash injection and corruption tests;
- typed tables with a designated event-time column support the documented core SQL subset through both a scalar reference engine and a validated vectorized engine;
- the epoll server provides bounded admission, explicit durability modes, and a versioned native protocol;
- historical queries and resumable live subscriptions perform a proven gap-free handoff at committed positions, including late events and corrections;
- incremental materialized views agree with full recomputation over supported queries;
- fuzzing, differential tests, sanitizers, fault injection, and reproducible benchmark harnesses run in the supported Linux environment; and
- measured results and resource limits are published without expanding the claims beyond the tested configurations.

Passing individual component tests or merely compiling does not satisfy this milestone.

## Distributed milestone

The distributed milestone succeeds when the single-node guarantees remain intact while:

- each tablet uses a deterministic Raft state machine and never exposes uncommitted log entries;
- a multiplexed physical log safely and fairly serves many logical Raft groups;
- membership changes, snapshot installation, recovery, leader changes, and rebalancing pass deterministic simulation and fault tests;
- read consistency is explicit (for example, linearizable or bounded-stale) and experimentally verified;
- distributed planning and execution preserve snapshot and system-time semantics across tablets; and
- the historical-to-live resume contract survives failover and topology changes without ambiguous boundaries.

Object-storage tiering is a later capability and is not required to call the replicated core correct unless its phase has also passed.

## Benchmark claims

ChronosDB will publish no estimated, extrapolated, or invented performance results. A claim must satisfy the full [benchmark publication contract](../benchmarks/benchmark-contract.md), including commit, hardware, software, configuration, durability, dataset, procedure, raw samples, statistics, and resources. Comparisons must use equivalent correctness and durability settings. Regressions and unfavorable results remain part of the record.
