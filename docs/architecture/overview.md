# Architecture Overview

This document records the accepted high-level architecture for a system that has not yet been implemented. It fixes component responsibilities and ordering constraints while leaving encodings, algorithms, and operational policies to specifications and ADRs created in the relevant [roadmap](../roadmap.md) phase. All components are governed by the [non-negotiable invariants](invariants.md) and the accepted [ADR index](../adr/README.md).

## Component map

```text
                                      control plane
                           schema catalog · placement · manifests
                                      │
clients ─► protocol ─► epoll reactors │
                           │           │
                           │ immutable decoded batches
                           ▼           ▼
                    bounded SPSC queues
                           │
                           ▼
                    shard workers ─── own ───► tablets
                           │                       │
              ┌────────────┼────────────┐          │ future
              ▼            ▼            ▼          ▼
       WAL / Raft log   mutable head   live operators
              │            │                 │
              │ commit     │ seal/flush      └──► subscription streams
              ▼            ▼
          checkpoint    CSEG parts ◄──── compaction
                              │
                           manifest
                              │
                              ▼
SQL ─► parser ─► binder ─► optimizer ─► snapshot ─► vectorized scans/operators
                              │
                              └──────── future distributed fragment planning

local hot storage ───────────────────── future tiering ─► object storage
```

Arrows express intended data or control flow, not implemented interfaces.

## Architectural planes

### Write plane

The write plane accepts versioned network frames, decodes them into immutable columnar batches, selects a tablet, and transfers each batch to the tablet's owning shard worker over a bounded single-producer/single-consumer queue. The reactor owns socket progress and admission; it does not modify tablet storage. [ADR 0004](../adr/0004-thread-ownership-and-ingress-concurrency.md) defines this ownership and handoff contract.

Exactly one shard worker owns a mutable tablet at a time. That worker validates schema, event-time, logical identity, and limits; serializes the operation to the single-node WAL or future tablet Raft log; crosses the requested durability boundary; marks the operation committed; and publishes fully initialized columnar rows. [ADR 0006](../adr/0006-wal-durability-and-group-commit.md) fixes the `ASYNC`, `LOCAL_SYNC`, and future `QUORUM_SYNC` acknowledgment modes. [ADR 0013](../adr/0013-wal-v1-format-and-recovery.md) and the [WAL recovery design](wal-recovery.md) now fix the single-node file/directory synchronization and acknowledgment ordering; defaults, group-commit parameters, and replicated persistence remain deferred.

The intended write flow is:

```text
network frame
  → decoded immutable batch
  → tablet routing
  → bounded reactor-to-shard queue
  → shard-owned validation
  → WAL or Raft-log append
  → durability boundary
  → commit
  → mutable head
  → live operators
  → eventual flush to CSEG
```

Commit precedes query visibility and live emission. The accepted
[columnar-ingestion design](columnar-ingestion.md) reserves bounded head/retry resources before WAL
admission, materializes after the requested WAL boundary, and atomically publishes the complete
batch before external logical success. Its recovery path replays durable-but-unpublished commands
into fresh state. No path may expose an uncommitted operation or a partially initialized row.

### Read plane

The custom query engine will implement the bounded [SQL v1 contract](../product/sql-v1.md), bind names and types against a versioned catalog, form a logical plan, optimize it, and lower it into physical vectorized operators. Before scanning data, it acquires a stable snapshot governed by the [consistency contract](../product/consistency-and-durability.md), containing the relevant schema versions, per-tablet committed positions, head generations, installed CSEG parts, and row-version visibility rule.

Scans combine visible mutable/sealed heads with immutable parts, use zone maps and sparse or optional secondary indexes to prune work, and feed bounded columnar vectors into operators. The planned scalar engine is a reference oracle before vectorization becomes authoritative.

The intended read flow is:

```text
SQL
  → parser
  → binder
  → logical plan
  → optimizer
  → physical plan
  → snapshot acquisition
  → head and CSEG scans
  → vectorized operators
  → result stream
```

### Live plane

The live plane consumes only committed tablet operations in log order. It updates supported incremental operators and materialized views, records sufficient progress to recover or replay, and emits changes through bounded subscription buffers. Slow consumers face a specified policy—backpressure up to a bound, spill if explicitly designed, or disconnect with a resumable position—but cannot stall ingestion indefinitely.

For a historical-to-live query, snapshot acquisition also registers or retains the continuation boundary. Historical results are evaluated at that snapshot; streaming then begins strictly after its commit position. The [live-query contract](../product/live-query-semantics.md) closes the race between those acts and specifies at-least-once change delivery. Resume tokens are opaque, versioned, integrity-protected representations of deterministic committed boundaries, not promises that an external consumer applied each result exactly once.

Event-time watermarks and allowed lateness control incremental window behavior. They do not determine database commit visibility. Corrections retain their source event-time meaning and create later system-time versions.

### Control plane

The control plane owns relatively cold metadata: schema and table definitions, partitioning, tablet ownership, configuration, admission policies, manifest coordination, background-job scheduling, and later cluster membership and placement. Locks are acceptable where they provide simple, auditable synchronization. Metadata transitions that affect durable recovery or read snapshots must be versioned and installed atomically.

## Storage components

### Mutable and sealed heads

A mutable head is an append-only, in-memory, columnar collection for one tablet's recent committed
versions. One shard worker performs mutation; readers acquire a stable published boundary or
generation. The writer initializes every selected column slot before publishing a new row count or
reference visible to readers. The accepted
[mutable-head publication contract](mutable-head-publication.md) fixes schema-bound generations,
stable storage, batch-atomic release/acquire publication, snapshot pins, and sealing ownership.
Concrete vector/allocator layouts remain implementation choices requiring evidence.
[ADR 0005](../adr/0005-columnar-heads-and-immutable-cseg-parts.md) fixes the head/part storage model.

When a head reaches a policy threshold, the owner seals it. A sealed head accepts no more rows, remains readable by active snapshots, and becomes flush input. New writes continue in a new mutable generation so durable I/O does not stop the shard.

### WAL

The accepted single-node [WAL v1 format](../formats/wal-v1.md) is a segmented, append-only sequence
of bounded versioned records with little-endian fields, protected framing, full-record CRC32C, and
WAL-wide sequence order. Its physical codec, minimal POSIX primitives, writer, locked discovery,
verification, explicit final-tail repair, deterministic replay interface, and existing-history
reopen path are implemented. A record never crosses a segment. The writer holds the WAL-directory
advisory lock, installs each segment through synchronized temporary file/rename/directory
boundaries, and synchronizes the prior segment before activating its successor. Existing-history
recovery preserves that lock and the recovered identity/sequence/offset. A bounded commit
coordinator now accepts concurrent producers, transfers all physical writer calls to one worker,
orders records by linearized admission, acknowledges `ASYNC` after complete write, and groups
`LOCAL_SYNC` requests behind covering synchronization frontiers. A subprocess crash harness checks
those acknowledgment frontiers against strict recovery, repair, rotation, reopening, and process
locking on real host files. The generic application envelope and first columnar application-kind
in-memory codec are implemented according to the
[accepted command contract](columnar-ingestion.md#columnar-append-command-v1); submission,
state-machine application, and retry-state integration remain unimplemented.

The [WAL recovery state machine](wal-recovery.md) verifies the complete physical history before
semantic preflight or replay. It can explicitly truncate only a narrowly defined incomplete suffix
of the highest active segment; bad checksums, discontinuities, and middle-of-log damage fail closed.
WAL v1 establishes physical order before durable CSEG installation covers operations. The columnar
logical mutation payload has an independent byte codec but no state-machine application path.
Deployment tuning of the implemented
group-commit limits, checkpoints, and old-segment removal remain future work.

In the distributed phase, each tablet's authoritative ordering is its committed Raft log. Many logical Raft groups will share a multiplexed physical log while preserving per-group ordering, durability, fairness, reclamation safety, and recovery identity. Reusing the single-node record codec may be desirable but is not yet decided.

### CSEG parts

CSEG is the planned public contract for versioned, immutable, sorted, compressed columnar parts. A part organizes rows into granules and independently checksummed column pages, with metadata sufficient for safe bounds validation and selective decoding. Fixed-width fields define byte order and encoding explicitly; native C++ object layouts are never serialized.

Parts are written to temporary identities, fully validated and made durable according to the installation protocol, then atomically referenced by a manifest version edit. After installation they are never changed in place. The exact file decomposition, codecs, sort key, encodings, footer layout, and compatibility policy are deferred to the CSEG v1 specification and ADRs.

### Manifest, flush, and checkpointing

The manifest is the authoritative versioned inventory of installed parts and relevant recovery metadata. It can refer only to completely installed durable parts. A flush converts a sealed head into one or more new CSEG parts, durably installs them, and applies a manifest version edit before a checkpoint allows covered WAL history to be reclaimed. Crashes at any step must recover to either the old complete state or the new complete state, and retry must be idempotent.

A checkpoint records the manifest generation and committed log coverage needed for recovery. It is not permission to delete data still reachable by an active reader, subscription, backup, or other declared retention owner.

### Compaction and indexes

Compaction reads immutable base and delta parts and writes new immutable parts. It installs additions and removals atomically in the manifest. For every supported snapshot and system-time rule, output must neither add, lose, nor duplicate visible logical rows. Obsolete files are reclaimed only after no active reader can reference their generations.

Zone maps and sparse indexes are part metadata used to prune granules/pages without changing truth. Optional secondary indexes may accelerate selected predicates but cannot be required for correctness. Late and out-of-order events may initially enter delta parts to avoid repeatedly rewriting primary sorted ranges; merge policy remains a measured design area.

## Query engine

ChronosDB plans a custom parser, binder, optimizer, and execution engine under [ADR 0008](../adr/0008-custom-sql-and-vectorized-execution.md). Binding assigns stable catalog identities and types; optimization must preserve SQL null, decimal, temporal, and system-time semantics; physical execution processes bounded column vectors rather than allocating per row. The scalar reference engine and differential tests provide an oracle for vectorized operators.

Parallel scheduling, spilling, memory accounting, adaptive behavior, join algorithms, and the precise supported SQL surface are deferred. Query resource use must eventually be admitted and bounded; cancellation must release snapshot pins and memory safely.

## Networking

Linux `epoll` is the first server backend under [ADR 0009](../adr/0009-network-reactor-strategy.md). Reactors use nonblocking sockets, bounded frame sizes, explicit connection state machines, and bounded queues to shard workers. Thread-per-connection is excluded. Network formats are versioned from their first implementation and all lengths, offsets, compression envelopes, and state transitions are validated before allocation or access.

An `io_uring` backend is optional and may be accepted only after the epoll path is correct, profiled, and reproducibly benchmarked. TLS and cryptography will use maintained external libraries behind a defined interface; ChronosDB will not implement cryptographic primitives.

## Future distribution: tablets and Raft

Tablets are the distribution and replication unit from the data model's beginning, but [ADR 0003](../adr/0003-single-node-first-development-order.md) defers replication until the single-node engine passes its gates. Under [ADR 0010](../adr/0010-tablets-raft-and-multiplexed-log-storage.md), each future tablet maps to one logical Raft group with deterministic state-machine application, while a small metadata group owns schemas, placement, membership, and cluster metadata. Readers may observe only committed and applied entries under an explicitly selected consistency level. Leader leases, read index, membership changes, snapshot transfer, and bounded-stale policies need lower-level ADRs and deterministic simulation.

Multi-Raft will multiplex many groups over shared threads, network connections, timers, and a physical log without conflating their logical indexes. Distributed queries will acquire compatible per-tablet snapshot boundaries and report consistency; rebalancing must preserve identities, resume positions, and retention pins.

## Future hot/cold tiering

Local storage remains the initial source of truth. A later tiering phase may move eligible immutable CSEG parts to object storage while retaining manifest identity, integrity validation, cache coherence, snapshot safety, and query observability. Object-store listings cannot be treated as the authoritative manifest. Upload/install ordering, local cache eviction, remote deletion, encryption, failure recovery, and interoperability exports remain deferred.

## Accepted direction and deferred design

The following are accepted project constraints:

- typed relational tables designate one event-time column and retain distinct system commit history;
- single-node correctness precedes distribution;
- one shard worker owns each mutable tablet, fed by bounded reactor-to-shard SPSC queues;
- recent data uses append-only columnar heads; durable analytical data uses immutable sorted CSEG parts;
- WAL/log commit gates visibility, and future Raft readers never see uncommitted entries;
- manifests atomically install complete parts; compaction creates replacement parts rather than editing in place;
- the query stack is custom and uses a scalar reference path before vectorized execution;
- networking is event-driven and epoll-first; and
- historical-to-live handoff is anchored to deterministic committed positions.

Deferred design areas include the server durability default and production tuning of the bounded
group-commit parameters; WAL application
record kinds and checkpoint/reclamation integration; row identities and correction syntax; CSEG
layout and codecs; head memory layout and publication ordering; manifest and garbage-collection
protocol; SQL grammar and type system; optimizer rules; subscription result/change model; watermark
finalization; scheduler and memory limits; authentication/TLS integration; Raft protocol details;
multi-Raft log layout; distributed snapshot coordination; and object-tier policy. The WAL v1
physical bytes, synchronization ordering, and recovery-tail classification are no longer deferred.
Each remaining area becomes accepted only through its phase artifacts, validation evidence, and any
required ADR.

The product scope and portability boundary are fixed by [ADRs 0001–0002](../adr/README.md). The dependency boundary is fixed by [ADR 0011](../adr/0011-dependency-and-build-versus-buy-policy.md), and [ADR 0012](../adr/0012-correctness-testing-and-performance-evidence.md) defines the evidence required before any component or optimization is accepted as correct or performant.
