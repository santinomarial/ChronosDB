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

Exactly one shard worker owns a mutable tablet at a time. That worker validates schema, event-time, logical identity, and limits; serializes the operation to the single-node WAL or tablet Raft log; crosses the requested durability boundary; marks the operation committed; and publishes fully initialized columnar rows. [ADR 0006](../adr/0006-wal-durability-and-group-commit.md) fixes the `ASYNC`, `LOCAL_SYNC`, and `QUORUM_SYNC` acknowledgment modes. [ADR 0013](../adr/0013-wal-v1-format-and-recovery.md) fixes the single-node file/directory synchronization ordering, while [ADR 0074](../adr/0074-quorum-sync-proof-boundary.md) fixes the internal replicated majority/apply proof. Defaults, native client exposure, and end-to-end replicated crash qualification remain deferred.

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
The implemented `chronos_head` boundary supplies one fixed-capacity generation with byte-per-row
validity/Boolean storage, stable fixed/variable arenas, an immutable publication pointer, and owning
snapshots. The `chronos_ingest` tablet owner adds bounded whole-batch rotation and one owning outer
publication for the active/sealed generation snapshots, applied position, and tablet retry table.
The blocking single-tablet append executor composes those publications with the global retry
directory and WAL coordinator. Bounded registered schema successors rotate generations without
mixing shapes, and retained-lineage recovery reconstructs fresh publications and retry state before
exposing the reopened writer. Routing/admission, retry retention, and flush handoff remain future
integration.
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
recovery preserves that lock and the recovered identity/sequence/offset. Manifest-checkpoint-aware
recovery also permits a proven covered prefix to be absent, validates the complete required suffix,
and reopens the writer at its exact global end. The live writer can separately revalidate and remove
only completely covered closed segments, followed by a WAL-directory sync; it never removes its
active highest segment. A bounded commit
coordinator now accepts concurrent producers, transfers all physical writer calls to one worker,
orders records by linearized admission, acknowledges `ASYNC` after complete write, and groups
`LOCAL_SYNC` requests behind covering synchronization frontiers. A subprocess crash harness checks
those acknowledgment frontiers against strict recovery, repair, rotation, reopening, and process
locking on real host files. The generic application envelope and first columnar application-kind
in-memory codec are implemented according to the
[accepted command contract](columnar-ingestion.md#columnar-append-command-v1). An already-routed
single-tablet execution path now submits that payload, waits for the exact requested durability
boundary, publishes rows/position/retry state, and commits the global retry pointer. Recovery
application is implemented for a caller-supplied retained linear schema lineage per tablet: it
preflights either the whole WAL or a Manifest-checkpoint suffix, restores exact durable tablet/retry
state, replays schema-bound uncovered rows and covered no-ops, rejects conflicts and first-time
schema regression, and exposes state plus the reopened writer only after full success.
Routing/admission, retry retention, and transport acknowledgment remain
unimplemented.

The [WAL recovery state machine](wal-recovery.md) verifies the complete physical history before
semantic preflight or replay. It can explicitly truncate only a narrowly defined incomplete suffix
of the highest active segment; bad checksums, discontinuities, and middle-of-log damage fail closed.
WAL v1 establishes physical order before durable CSEG installation covers operations. The columnar
logical mutation payload has an independent byte codec, a live in-memory application path, and a
retained-lineage fresh-state recovery path with a durable-prefix seed boundary. The Manifest-to-seed
startup owner is implemented around a caller-supplied retained catalog; durable catalog
reconstruction remains outside that path. Deployment tuning of the implemented group-commit limits
remains future work.

In the distributed phase, each tablet's authoritative ordering is its committed Raft log. Many logical Raft groups will share a multiplexed physical log while preserving per-group ordering, durability, fairness, reclamation safety, and recovery identity. Reusing the single-node record codec may be desirable but is not yet decided.

### CSEG parts

CSEG is the accepted public contract for versioned, immutable, sorted, compressed columnar parts.
The [CSEG v1 specification](../formats/cseg-v1.md) fixes one schema/tablet-bound file, canonical
granules, schema and system columns, independently checksummed PLAIN pages, bounded raw/Zstandard
storage, metadata integrity, physical row ordering, and compatibility behavior. The `chronos_cseg`
target now exposes the authoritative constants, nominal part identity, allocation-free checked
metadata/page layout planner, bounded raw/Zstandard page compression, and a checksummed borrowed
metadata-directory codec with exact schema binding. An identity-free physical-column view shares
the Columnar Batch v1 canonical buffer validator with deterministic PLAIN payload encoding and
borrowed schema-independent decoding. The composed page codec computes and verifies stored-byte
CRC32C before bounded compression-provider entry, borrows raw pages, and owns decompressed pages.
The part codec now owns exact canonical file images and provides prefix/exact borrowed structural
decoding that validates all page CRCs, bounded decompression, PLAIN payloads, and zero alignment.
The bounded full validator checks system-row values, recomputes event-time extrema, enforces exact
all-type physical ordering across granules, and composes exact schema/tablet binding. The projected
reader authenticates metadata once, then independently validates only requested user pages plus
the four mandatory system pages for each granule; lineage projection synthesizes canonical NULLs
for nullable successor-schema tails. Complete read-only inspection validates structural and all
schema-independent row semantics and returns an owned value-free report; `chronos-csegdump`
exposes that path without mutating the candidate file.

Parts are written to temporary identities, fully validated, and made durable according to the
accepted [installation protocol](manifest-installation-and-checkpointing.md), then atomically
referenced by a Manifest v1 generation. After installation they are never changed in place.
Sealed-head conversion, part/Manifest installation, and atomic database storage publication are
implemented; compaction and installed-file reclamation remain deferred.

### Manifest, flush, and checkpointing

The manifest is the authoritative versioned inventory of installed parts and relevant recovery
metadata. The accepted [Manifest v1 specification](../formats/manifest-v1.md) uses immutable
database-wide full generations with per-tablet schema/application boundaries, protected retry
outcomes, and one global WAL reclaim coordinate. It can refer only to completely installed durable
parts. A flush converts a sealed head into one or more new CSEG parts, durably installs them, and
publishes a manifest generation before a checkpoint allows covered WAL history to be reclaimed.
Crashes at any step recover to either the old complete state or the new complete state, and retry is
idempotent. The codecs, conversion/build/proof primitives, filesystem installation/recovery,
checkpoint-aware WAL lifecycle, bounded sealed-head scheduling, receipt-authorized TabletState
retirement, one-pointer head-to-part publication, and the end-to-end single-part durable flush
coordinator are implemented. Subprocess crash/reopen coverage now exercises every part and Manifest
write, file-sync, rename, and directory-sync boundary and proves repeated old-or-new selection.
Caller-catalog startup now composes selected Manifest/part validation, exact durable retry/tablet
seeding, required WAL suffix replay/reopen, recognized temporary cleanup, and one aggregate
Manifest/head publication behind a move-only lock-owning boundary.

A checkpoint records the manifest generation and committed log coverage needed for recovery. It is not permission to delete data still reachable by an active reader, subscription, backup, or other declared retention owner.

### Compaction and indexes

Compaction reads immutable base and delta parts and writes new immutable parts. It installs additions and removals atomically in the manifest. For every supported snapshot and system-time rule, output must neither add, lose, nor duplicate visible logical rows. Obsolete files are reclaimed only after no active reader can reference their generations.

Zone maps and sparse indexes are part metadata used to prune granules/pages without changing truth. Optional secondary indexes may accelerate selected predicates but cannot be required for correctness. Late and out-of-order events may initially enter delta parts to avoid repeatedly rewriting primary sorted ranges; merge policy remains a measured design area.

## Query engine

ChronosDB plans a custom parser, binder, optimizer, and execution engine under [ADR 0008](../adr/0008-custom-sql-and-vectorized-execution.md). Binding assigns stable catalog identities and types; optimization must preserve SQL null, decimal, temporal, and system-time semantics; physical execution processes bounded column vectors rather than allocating per row. The scalar reference engine and differential tests provide an oracle for vectorized operators. The first Phase 9 substrates provide identity-free canonical physical owners, explicit order-preserving selections, caller-bounded immutable chunks under [ADR 0020](../adr/0020-bounded-vector-chunk-representation.md), one shared query-wide memory/cancellation state under [ADR 0021](../adr/0021-query-resource-accounting-and-cooperative-cancellation.md), and an accounted pull lifecycle with allocation-free Boolean filtering under [ADR 0022](../adr/0022-pull-based-physical-operator-lifecycle.md). Stable column-subset projection can discard unused direct-owned buffers without changing row cardinality, selection, or credit ownership; global LIMIT truncates the selected prefix across chunks and eagerly releases unpulled sequential input. [ADR 0023](../adr/0023-bounded-physical-pipeline-plan.md) adds the first bounded immutable unary plan, exact physical-shape propagation, runtime source-shape enforcement, and composed scalar-model differential execution. [ADR 0024](../adr/0024-lifetime-pinned-vector-chunk-backing.md) permits the same chunks to borrow immutable physical views through one conservatively charged lifetime owner. [ADR 0025](../adr/0025-allocation-free-cseg-projected-read-planning.md) makes exact raw-versus-owned CSEG decoded-buffer work visible without heap allocation before decode, [ADR 0026](../adr/0026-pinned-in-memory-cseg-scan-source.md) uses those boundaries for a query-accounted, lifetime-pinned single-part CSEG source, and [ADR 0027](../adr/0027-snapshot-bound-cseg-images-and-part-lifetime-pins.md) loads a held snapshot's selected part through per-part reclamation pins and a storage-validated query adapter. [ADR 0028](../adr/0028-pruned-multi-part-snapshot-cseg-scan.md) composes the durable CSEG subset of one tablet from one exact aggregate epoch: canonical Manifest planning prunes disjoint files, authenticated CSEG metadata prunes disjoint granules before page work, and a query-accounted serial source emits selected granules in physical order. [ADR 0029](../adr/0029-query-accounted-mutable-head-scan-source.md) adds a single-generation mutable-head source that pins one acquire-observed publication and materializes race-safe byte-per-row head storage into bounded canonical query chunks. Both sources can opt into the shared row-version suffix described below. [ADR 0047](../adr/0047-exact-append-only-snapshot-tablet-scan.md) composes the exact durable, sealed-head, and active-head publications for the complete currently accepted append-only tablet multiset. Future correction/delete winner resolution remains unimplemented because those operation encodings and visibility rules are not yet accepted.

[ADR 0058](../adr/0058-shared-snapshot-publication-query-credit.md) charges one aggregate storage
publication per query across selected images, borrowed CSEG chunks, tablet children, and same-epoch
ASOF aliases while keeping independently owned buffers locally accounted.

[ADR 0059](../adr/0059-bounded-physical-strategy-selection.md) owns each exact checked pipeline with
authoritative finite sort estimates, prefers in-memory sort when admitted, otherwise requires an
exact stage-indexed external-sort capability and runtime directory, and selects bounded parallel
source composition only under an explicit whole-pipeline order-independence proof and lower
deterministic work cost. The snapshot connector supports optimizer-selected external SQL ordering
but deliberately keeps each complete tablet as one source.

The accepted Phase 9 boundary is closed by a reproducible full-plan differential oracle spanning
base, aggregate, LATEST, and ASOF pipelines, exact ordered output, system-time ties, changing batch
widths, and matched runtime failures. This validates the complete current append-only path without
inventing correction/delete semantics or treating future asynchronous and parallel provider work as
already implemented.

Accounted column output now supports arbitrary source order/duplicates, typed constants, and bounded
checked expression programs. One exact single-source, nonaggregate bound-SELECT subset lowers
WHERE, ordered projection, fixed-width scalar expressions, STRING/SYMBOL casts and ASCII case
output, borrowed text comparisons/NULL predicates, and LIMIT into the unary pipeline under
[ADRs 0035](../adr/0035-bounded-checked-vector-expression-programs.md),
[0036](../adr/0036-bound-select-to-physical-pipeline-lowering.md), and
[0037](../adr/0037-fixed-width-vector-casts-and-scalar-functions.md), and
[0038](../adr/0038-borrowed-variable-width-vector-materialization.md), and
[0039](../adr/0039-borrowed-text-predicate-vector-kernels.md). One fixed-state streaming global
aggregate stage now consumes accounted chunks and emits a canonical one-row COUNT/SUM/AVG/MIN/MAX/
variance result under [ADR 0040](../adr/0040-streaming-ungrouped-vector-aggregates.md). Bound-SQL
global aggregates now lower WHERE, direct or computed arguments, the fixed-state aggregate stage,
final expressions, and LIMIT under
[ADR 0041](../adr/0041-bound-global-aggregate-physical-lowering.md). Grouped/dynamic aggregate
state has finite query-accounted exact fixed/variable keys under
[ADR 0042](../adr/0042-query-accounted-bounded-grouped-aggregates.md), and bound single-source GROUP
BY lowering is connected under
[ADR 0043](../adr/0043-bound-grouped-aggregate-physical-lowering.md). Canonical query-accounted
open addressing now replaces linear group lookup while preserving exact collision checks and
floating grouping equivalence under
[ADR 0050](../adr/0050-canonical-query-accounted-group-hashing.md). A bounded physical sort now
retains accounted input, stably orders explicit all-type keys, and gathers one independent canonical
output under [ADR 0044](../adr/0044-query-accounted-bounded-physical-sort.md). CSEG and mutable-head
sources can now append the same checked WAL ID/record sequence/row ordinal/operation suffix under
[ADR 0045](../adr/0045-shared-vector-row-version-suffix.md): CSEG borrows mandatory authenticated
system pages while head scans materialize accounted canonical buffers. Exact bounded SQL ORDER BY
lowering under [ADR 0046](../adr/0046-exact-bounded-sql-order-by-lowering.md) now prepares aliases
and non-projected keys, appends DEDUP/group and commit-position ties, sorts before LIMIT, and removes
every hidden column. Base schemas without a DEDUP KEY remain unsupported until their authoritative
generated logical identity is exposed.
[ADR 0051](../adr/0051-exact-bounded-latest-by-physical-lowering.md) adds exact bounded LATEST BY:
the bound timestamp is prepared before WHERE, complete physical and row-version winner ties are
explicit sort keys, adjacent groups compact allocation-free, and all helper columns remain hidden.
[ADR 0052](../adr/0052-query-accounted-bounded-asof-join.md) adds the storage-independent physical
ASOF primitive: two finite accounted inputs, exact SQL equality/time/physical-version selection,
canonical left null extension, and an explicit match bit for later joined identity.
[ADR 0053](../adr/0053-checked-left-deep-asof-physical-plan.md) composes those primitives in a
finite immutable left-deep plan with exact preparation, binary-output, and final-pipeline shape
handoffs plus fail-closed sibling ownership.
[ADR 0054](../adr/0054-bound-asof-select-physical-lowering.md) lowers bound source-aware join
expressions, post-join filtering and aggregation, and exact joined ORDER BY identities into that
plan while widening ASOF LEFT right-source nullability.
[ADR 0055](../adr/0055-snapshot-bound-multi-source-asof-instantiation.md) binds every SQL source of
that plan, in source order, to complete tablet publications from one held aggregate database epoch;
partial construction owns and releases all earlier sources, pins, and query credit on failure.
[ADR 0056](../adr/0056-shared-query-credit-and-bounded-parallel-scheduling.md) adds last-owner shared
query credit and a bounded unordered merge for independent pipelines. Each worker owns a whole
thread-affine pipeline, a fixed ring release/acquire-publishes complete accounted chunks, and
terminal failure cancels and joins every sibling before returning a deterministic status. SQL
ordering remains the responsibility of explicit complete physical keys, never queue arrival.
[ADR 0057](../adr/0057-bounded-checksummed-external-sort.md) adds a finite external-sort baseline:
contiguous stable in-memory runs use an ephemeral versioned and per-row-checksummed format, while a
bounded pull merge preserves exact physical comparisons and cross-run ties under explicit memory,
record, run-count, and disk quotas. Temporary files are caller-namespaced, exclusively created, and
removed on completion or ownership unwinding; they are not durable or recoverable state.
[ADR 0048](../adr/0048-snapshot-tablet-physical-pipeline-instantiation.md) now validates a lowered
plan's complete schema and optional suffix input, loads one held snapshot's durable images, composes
every current source, and instantiates the checked pipeline without collapsing SQL diagnostics.
Future correction/delete version resolution remains separate work. Variable-width aggregate
extrema now use exact unsigned byte order and reserve-before-copy query accounting under
[ADR 0049](../adr/0049-query-accounted-variable-width-extrema.md). The bounded selector described
above chooses the accepted sort and source-merge implementations without rewriting this graph;
statistics derivation, adaptive behavior, and additional join algorithms remain deferred.
Implemented reservations and accounted chunks provide the admission/ownership invariant, but
future operators must reserve every retained allocation and release snapshot pins and memory by
cooperative cancellation unwinding.

## Networking

Linux `epoll` is the first server backend under [ADR 0009](../adr/0009-network-reactor-strategy.md). Reactors use nonblocking sockets, bounded frame sizes, explicit connection state machines, and bounded queues to shard workers. Thread-per-connection is excluded. Network formats are versioned from their first implementation and all lengths, offsets, compression envelopes, and state transitions are validated before allocation or access.

The implemented portable Protocol v1 frame under [ADR 0060](../adr/0060-native-protocol-v1-framing.md)
uses a fixed checksummed header and an independently checksummed payload. It validates the header
and finite body requirement before payload allocation. The implemented payload registry covers
handshake, ingest, durability acknowledgement, query, cancellation, errors, bounded query-result
batches, and terminal completion with exact connection-state validation.

Portable bounded connection buffers retain partial input and immutable short-write output. A
fixed-capacity SPSC ring then transfers complete owned tasks from exactly one reactor to exactly one
shard; release publication and acquire consumption cover initialization, while the reverse pair
covers safe cell reuse under [ADR 0063](../adr/0063-bounded-reactor-shard-spsc-routing.md).

The Linux owner under [ADR 0064](../adr/0064-bounded-linux-epoll-reactor.md) contains descriptors and
epoll types behind a portable PIMPL. It applies finite admission/deadlines, reads before simultaneous
half-close cleanup, routes only matching active responses, and detaches work before dropping late
An `eventfd` wakeup transfers shard-response availability to the blocked reactor without polling;
finite event and I/O budgets preserve fairness. Accepted sockets require `TCP_NODELAY`, a measured
decision that prevents separately owned result and terminal frames from incurring delayed-ACK
latency. The portable client session enforces the same partial-I/O and request lifecycle contracts.

An optional Linux liburing readiness pilot now exists behind explicit portable backend selection.
It reuses the proven epoll connection engine and therefore preserves its partial-I/O, cancellation,
and shutdown state. It is not yet a full socket-operation io_uring backend and has no comparison or
performance claim. TLS and cryptography use maintained external libraries behind defined
interfaces; ChronosDB does not implement cryptographic primitives.

Under [ADR 0066](../adr/0066-authentication-and-tls-integration-boundary.md), plaintext is confined
to loopback, a borrowed authenticator attaches stable principal identity to shard work, and
`TLS_REQUIRED` fails startup until the maintained TLS backend exists.

## Distribution foundations: tablets and Raft

Tablets are the distribution and replication unit from the data model's beginning. Under [ADR
0010](../adr/0010-tablets-raft-and-multiplexed-log-storage.md), each tablet maps to one logical Raft
group, while a small metadata group owns schemas, placement, nodes, leader hints, retention, and
cluster metadata. The deterministic Raft and bounded Multi-Raft logical cores, versioned/checksummed
metadata commands with committed application and retained-log recovery,
full-state physical record codec, segmented append/sync/recovery owner, distributed aggregate
primitives, and safe movement state machine are implemented. Readers may observe only committed and
applied entries under an explicitly selected consistency level. The tablet state machine now
decodes committed COLUMNAR_APPEND entries, publishes them in group-index order, persists the applied
index afterward, and reconstructs fresh memory from a complete retained committed log. In-memory
tablet commit positions
now distinguish a WAL identity from a Raft group/index identity, while frozen CSEG/Manifest v1
writers reject Raft identities rather than aliasing them into WAL fields. Production timers,
transport, a replicated durable-row/application-snapshot format, snapshot installation,
membership protocol, read index/staleness proof, and a packaged cluster runtime remain
unimplemented. The
single-thread-affine durable runtime already accepts bounded operation batches, persists every
state-changing transition under one local sync, and exposes outbound messages only afterward.

Multi-Raft will multiplex many groups over shared threads, network connections, timers, and a
physical log without conflating their logical indexes. The physical-log boundary now supplies
versioned shared segments, append/sync frontiers, rotation, locked bounded recovery, and explicit
incomplete-tail repair; batching, reclamation, and runtime transport integration remain.
Distributed queries will acquire compatible per-tablet snapshot boundaries and report consistency;
rebalancing must preserve identities, resume positions, and retention pins.

## Hot/cold tiering foundation

Local storage remains the initial source of truth. The implemented logical tiering coordinator moves
eligible immutable identities through verified idempotent object upload, a caller-owned atomic
manifest-install callback, bounded content caching, and range reads. Object-store listings are not
metadata truth. Manifest v1 has no cold-location fields, so production manifest persistence, safe
local/remote deletion, crash recovery, S3 transport, encryption, and Arrow/Parquet exports remain
deferred.

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
group-commit parameters; additional WAL application record kinds; row identities and correction
syntax; additional CSEG encodings; manifest-generation and installed-part garbage collection; SQL
grammar and type system; statistics derivation, relational rewrites, and join costing; subscription
result/change model; watermark finalization; parallel tablet morsels, worker-pool admission, and
production memory-limit tuning; authentication/TLS
integration; Raft protocol details; multi-Raft log layout; distributed snapshot coordination; and
object-tier policy. The WAL v1
physical bytes, synchronization ordering, recovery-tail classification, CSEG v1 codecs, and
Manifest v1/checkpoint installation design are no longer deferred.
Each remaining area becomes accepted only through its phase artifacts, validation evidence, and any
required ADR.

The product scope and portability boundary are fixed by [ADRs 0001–0002](../adr/README.md). The dependency boundary is fixed by [ADR 0011](../adr/0011-dependency-and-build-versus-buy-policy.md), and [ADR 0012](../adr/0012-correctness-testing-and-performance-evidence.md) defines the evidence required before any component or optimization is accepted as correct or performant.
