# Feature Completion Pass Review

## Repository state

- **Starting HEAD:** `7c46d1427a0c0b1cc4cfbe7a864b140b7ae17a0b` (`build: close phase 10 Linux portability gate`).
- **Starting tree:** `main...origin/main` with two pre-existing untracked headers:
  `include/chronos/live/subscription.hpp` and `include/chronos/live/resume_token.hpp`.
- **Concurrent owner activity:** while this pass was running, the repository owner created and
  pushed `f35c140`, `30ba6bd42f84aae821f426cc194f344b32114de9`, and
  `034273eb74b0040820be4e82587797c77b217fac`, committing the Phase 11–17, runtime, integration, and
  documentation files produced during the pass. The agent did not run `git commit` or `git push`.
- **Reviewed ending HEAD:** `034273eb74b0040820be4e82587797c77b217fac`. The final uncommitted tree
  contains the last Raft safety fixes, documentation reconciliation, and formatting corrections.

This is a truthful feature-architecture checkpoint, not a declaration that Phases 11–17 have passed
their full roadmap exit gates or that ChronosDB is a production three-node database.

## Implemented phase slices

### Phase 11 — live subscriptions and materialized views

`chronos_live` implements:

- Resume Token v1 with fixed versioned bytes, HMAC-SHA256, constant-time MAC comparison, and bound
  database/subscription/plan/schema/tablet/WAL/sequence identity;
- single-source register-before-boundary handoff, snapshot-phase buffering, live transition,
  at-least-once poll, acknowledgment checkpoints, retained-suffix resume, cancellation, and bounded
  fail-closed overflow that never rejects an already committed source change;
- removable count, sum, min, max, VWAP, OHLC, and Welford population/sample variance state; and
- tumbling/sliding window materialized state, watermark finalization, corrections/tombstones,
  revisions, consecutive committed progress, and finite row/window bounds;
- plan-bound single-tablet snapshot execution, schema-bound `SUBSCRIBE SELECT` identity, and a
  durable exact-SQL registry that reprepares and verifies executable fingerprints after restart;
- canonical multi-tablet delivery order, exact logical checkpoint/restore, and frozen checksummed
  checkpoint-generation bytes; and
- lock-owning, exact-next-generation filesystem installation, fail-closed latest selection, and a
  durable coordinator owner that publishes retention frontiers only after synchronized install;
  plus exact multi-tablet historical execution through one global physical pipeline, started
  directly from a durably recovered executable without mutable-manager escape. Snapshot teardown
  abandons state without token allocation, while client cancellation still returns a safe token;
  and committed schema incompatibility has a distinct terminal phase, precise Protocol 1.1 reason,
  invalidated resume state, and durable checkpoint/reopen representation; and a bounded reactor-
  facing service owns SQL validation, snapshot/READY/live/ack/cancel/resume transitions, exact
  response-ring backpressure retry, disconnect cleanup, and resumable shutdown drain.

The source-retention boundary now component-wise intersects storage/Raft safety with every durable
plan frontier and validates committed placement epochs and local replica membership before invoking
a source-specific batch reclaimer. Logical subscription positions are not fabricated into physical
WAL offsets.

Still incomplete for production deployment: source-specific WAL/Raft physical prefix reclamation
and dynamic plan-owner retirement remain coupled to the Phase 15 physical-log lifecycle.

### Phase 12 — performance architecture and io_uring

Portable `ReactorBackend` selection retains epoll as the reference. The opt-in Linux/liburing owner
now submits accept, receive, send, and response-wakeup operations directly through io_uring while
preserving the portable connection state, bounded buffers, shard queues, and partial-I/O semantics.
One operation per connection and CQE-before-reclamation ownership keep native references bounded;
unsupported builds, kernels, or host policies fail explicitly without fallback.
`ThreadPlacement` adds optional CPU and NUMA hooks; empty configuration is correctness-neutral and
unsupported NUMA/portable affinity fails explicitly. Parallel query workers accept one exact
placement per selected worker and use an all-worker startup gate: no pipeline executes unless every
placement succeeds, and concurrent failures resolve by lowest worker ordinal.

A focused Ubuntu 24.04/GCC/liburing 2.5 production build passed with warnings as errors. Focused
Linux 6.12 tests passed for fragmented accept/read, handshake/query routing, response wakeup,
ordered result/terminal sends, and shutdown. Dense non-null identity timestamp filters now dispatch
between exact scalar, AVX2, and AArch64 NEON kernels; NEON differential tests ran locally and AVX2
passed an x86_64 warnings-as-errors compile-only check. No epoll/io_uring or SIMD comparison, NUMA
experiment, allocation profile, or performance campaign ran.

### Phase 13 — system-time history and corrections

The existing SQL parser/binder/executor already carried `FOR SYSTEM_TIME AS OF` into a snapshot
provider. `TemporalSnapshotProvider` now supplies real committed logical history for one exact
schema: atomic commit batches, distinct event/receive/system time, original/correction/replacement/
tombstone versions, current and historical winner selection, copied stable snapshots, finite state,
and precise history-expired failure.

CSEG v1 remains correctly frozen to `APPEND_ROWS`. Temporal Mutation Command v1, CSEG v2, and
Manifest v2 now durably encode, validate, install, select, resolve, and recover WAL/Raft-neutral
version histories without reinterpreting v1 bytes. The WAL startup owner restores every selected
distinct-table tablet under one global checkpoint, exact-verifies routed overlap, applies only each
tablet's suffix, and retains all providers/generation/locks until complete unpublished recovery.
Same-table multi-tablet routing fails explicitly because command v1 lacks tablet identity. Vector
publication now has a bounded query-accounted scalar-snapshot source that copies current/as-of
winners into canonical owned chunks. Direct vector winner resolution/lowering, Raft/mixed-source
composition, v1 migration, and authorized compaction/retention integration remain, so the complete
Phase 13 exit is not claimed. In-memory compaction now keeps the global time-index predecessor needed
at the exact retained boundary, rejects both frontier regressions, and reports exact removals; this
does not yet authorize durable part replacement or reclamation.

### Phase 14 — deterministic Raft

`RaftNode` implements follower/candidate/leader roles, term/vote/log/snapshot/commit/applied state,
runtime-triggered elections, log freshness, RequestVote, heartbeat/AppendEntries, log match,
conflict hints/rewind, next/match indexes, majority commit with current-term restriction, stale-term
rejection, leader demotion, bounded proposals, and committed-unapplied exposure. Transitions carry
an explicit persist-before-send state copy.

The focused minimum gate passes for 3-node election, one committed command, leader loss, replacement
leader, a second command, stale-leader rejection, and restarted follower catch-up. Joint membership,
two-stage snapshot installation, and a current-term quorum read barrier are implemented. Disk
persistence fault matrices, runtime timers, network encoding, production tablet read integration,
snapshot transfer bytes, and randomized simulation remain incomplete.

### Phase 15 — Multi-Raft tablets and metadata

`MultiRaftRuntime` multiplexes bounded logical groups on one owner with group-tagged messages,
node-global physical persistence sequences, persist-before-send batches, independent application,
and reopen state. The checksummed Multiplexed Raft Persistent-State Record v1 encodes group identity,
term, vote, logical log, commit/applied indexes, manifest generation, and part-set checksum without
native struct serialization.

`MetadataStateMachine` applies nodes, schema identities, tablet placement/replicas/epochs/leader
hints, and retention only at consecutive committed metadata-group indexes. Focused tests cover
different leaders, group isolation, one-node loss, reopen, metadata order, and record corruption.

No segmented file/fsync owner, batching worker pool, fairness policy, physical-log recovery scan,
tablet state-machine adapter, membership protocol, or QUORUM_SYNC exists. QUORUM_SYNC is not aliased
to LOCAL_SYNC or exposed.

### Phase 16 — distributed query and rebalancing

The query target implements event-time tablet pruning, explicit read-consistency enum values,
mergeable COUNT/SUM/MIN/MAX/Welford partial state, bounded MPMC exchange, backpressure, cancellation,
duplicate detection, worker failure, and a coordinator that refuses partial success.

`TabletMovement` enforces add target as learner, bounded checksummed retryable snapshot transfer,
catch-up through the snapshot index, placement-epoch-checked target promotion, and only then source
removal. Corrupt, gapped, or conflicting retry chunks fail closed.

Subsequent work added proof-bound dispatch/response protocols, authenticated TCP/mTLS clients and
servers, compatible pinned multi-tablet scheduling, whole-query cancellation/deadlines, finite
explicit rebinding, authenticated leader hints, durable physical movement ownership, joint Raft
membership coordination, and reader-pinned source retirement/reclamation. A focused real-mTLS gate
now returns the identical aggregate before and after a learner-first movement. General vector-plan
grouping/order/top-N/LIMIT, automatic metadata acquisition, a packaged process runtime, remote CSEG
execution in that gate, and broad failure/measurement evidence remain incomplete.

### Phase 17 — object storage and interoperability

`chronos_tiering` defines S3-compatible immutable `put_if_absent`/`stat`/`get_range` semantics and a
deterministic memory implementation. A subsequent production libcurl backend adds SigV4,
TLS-by-default, finite timeouts and response bounds, checksum metadata, conditional immutable PUT,
exact retry verification, and exact range responses. `TieredPartManager` checks SHA-256 content,
verifies remote metadata, calls the atomic manifest installer before allowing local release, rejects
part/key identity conflicts, caches bounded complete objects with eviction, and supports
authenticated range reads for larger objects. A smoke test exposed and fixed a 32-bit constant-
expression overflow that made the default 4 GiB object limit zero.

Manifest v1/v2 bytes remain unchanged. A subsequent Cold Location Manifest v1 codec binds bounded
object keys and deployment store identity to exact Manifest v2 part length/SHA-256 without trusting
listings. A dedicated locked storage owner now exact-readback installs synchronized immutable
add-only generations and recovers the highest consecutive generation without fallback, always
binding it to an exact Manifest v2 value. Credential refresh/provider policy, automatic retry/backoff,
multipart upload, aggregate cold/base publication, safe deletion, CSEG pre-upload validator
connection, cache concurrency, and Arrow/Parquet import/export remain incomplete.

## End-to-end integration state

`chronos_feature_smoke_tests` connects committed metadata, temporal visibility, live handoff,
distributed partial aggregation, a committed single-group Raft command, verified object upload,
manifest callback, cache/range read, and byte-identical result in one process. Separate deterministic
tests cover the requested 3-node Raft failover and Multi-Raft different-leader cases.

A packaged daemon and the requested real three-process/socket workflow do not exist. A later focused
gate uses real mutual-TLS query sockets around the complete movement state machine, but simulates the
externally committed promotion/removal milestones and deterministic worker aggregates. It does not
start three server processes, execute SQL through the native protocol, kill a process, apply a Raft
command to mutable/CSEG storage, or query a real remote CSEG. Those remain high-priority integration
and hardening tasks, not passed checks.

## Public APIs and formats

Important new public targets are `chronos::live`, `chronos::runtime`, `chronos::raft`, and
`chronos::tiering`; `chronos::query` gained temporal/distributed APIs and `chronos::network` gained
explicit backend selection.

Important APIs include `SubscriptionManager`, `WindowedMaterializedView`,
`IncrementalAggregateSet`, `TemporalSnapshotProvider`, `RaftNode`, `MultiRaftRuntime`,
`MetadataStateMachine`, `TabletMovement`, `BoundedExchange`, `DistributedAggregateCoordinator`,
`ObjectStore`, `TieredPartManager`, `Reactor`, and `apply_current_thread_placement`.

New bytes are limited to authenticated Resume Token v1 and Multiplexed Raft Persistent-State Record
v1, specified under `docs/formats/`. WAL v1, Columnar Batch v1, CSEG v1, Manifest v1, and native
Protocol v1 bytes were not changed. No Raft wire, distributed exchange wire, correction/CSEG v2, or
cold Manifest v2 bytes are claimed.

## Checks actually performed

Targeted configure/build commands used the existing `dev` preset and `--parallel 4`:

- `cmake --preset dev`
- `cmake --build --preset dev --target chronos_live_tests --parallel 4`
- `cmake --build --preset dev --target chronos_query_tests --parallel 4`
- `cmake --build --preset dev --target chronos_raft_tests --parallel 4`
- `cmake --build --preset dev --target chronos_tiering_tests --parallel 4`
- `cmake --build --preset dev --target chronos_network_tests chronos_runtime_tests --parallel 4`
- `cmake --build --preset dev --target chronos_feature_smoke_tests --parallel 4`
- Ubuntu 24.04/GCC 13: `chronos_network` with `CHRONOS_ENABLE_IO_URING=ON` and warnings as errors.

Focused executions passed:

- `chronos_live_tests`: 10 tests;
- `chronos_query_tests --gtest_filter=TemporalSnapshotTest.*:DistributedQueryTest.*`: 4 tests;
- `chronos_raft_tests`: 12 tests after metadata integration and final hostile-input audit fixes;
- `chronos_tiering_tests`: 11 tests;
- `chronos_network_tests`: 31 tests, including the backend-selection test;
- `chronos_runtime_tests`: 1 test; and
- `chronos_feature_smoke_tests`: 1 test.
- Linux 6.12/liburing 2.5 `IoUringReactorTest.*`: 2 focused tests.

The final C++ tree passed the repository-pinned clang-format 18 check. Full-suite, sanitizer, fuzz,
broader cross-compiler/Linux parity, benchmark, profile, and chaos checks were deliberately not run.

## Known risks and limitations

### Correctness

- The feature graph is not service-integrated; several APIs accept already-committed/validated data
  and rely on an absent adapter to preserve that precondition.
- Temporal corrections are not durable and are not resolved in vector CSEG/head scans.
- The distributed implementation covers numeric global aggregate state, not arbitrary plans,
  grouping, order, top-N, limits, or exchange retries.
- The movement state machine is not the Raft membership protocol itself.
- Cold upload does not independently parse/validate the candidate as CSEG before upload.
- Raft now prevalidates malformed higher-term messages and divergent matching-term entries, but
  snapshot boundaries, response-state combinations, and membership still need broader model evidence.

### Concurrency

- Live/materialized-view, Multi-Raft, metadata, movement, and tiering owners are intentionally
  single-thread-affine but are not yet scheduled by production worker pools.
- BoundedExchange and MemoryObjectStore use mutexes but have no TSan evidence in this pass.
- io_uring protocol cancellation, forced in-flight shutdown, and close/completion races lack broad
  Linux and TSan evidence beyond the focused clean-shutdown lifecycle.
- Cache/tiering catalog access is single-owner and unsafe for concurrent query access without the
  planned owner/locking integration.

### Durability

- No persistent materialized-view checkpoint, temporal correction log/part, Raft segmented writer,
  metadata command codec/file, durable movement receipt, Manifest v2 cold location, or cache index.
- Multiplexed records are codecs only; no append/sync/recovery/reclamation owner exists.
- QUORUM_SYNC is unavailable.
- The in-memory object store explicitly makes no remote durability claim; production S3 semantics
  have not been exercised.

### Performance

- Full-state Raft persistence is intentionally simple and may amplify writes.
- Ordered maps/multisets and copied snapshots favor correctness over throughput.
- Live fan-out, temporal history, exchange, cache, and movement have no scale measurements.
- The io_uring socket backend may be slower than epoll; no comparison was run.
- Beyond the timestamp range kernel, no SIMD/NUMA optimization, allocation profile, flame graph, or
  tail-latency campaign ran.

## Deferred validation and recommended Phase 18 order

The exact subsystem/category ledger is
[`deferred-validation.md`](../development/deferred-validation.md). Recommended order:

1. Audit/fix deterministic Raft transition safety, add persistent file owner and crash recovery,
   then run bounded deterministic simulation before building more distribution on it.
2. Connect committed Raft application to tablet ingest/head/CSEG state and metadata command codecs;
   only then define and test QUORUM_SYNC.
3. Accept/version correction WAL and CSEG v2 plus Manifest v2 cold descriptors; implement recovery,
   compaction, and scalar/vector temporal equivalence.
4. Build real coordinator/worker fragment and exchange protocols with consistency proofs, then wire
   safe Raft membership and durable snapshot movement.
5. Add production S3 and Arrow/Parquet providers with dependency/security/compatibility review.
6. Build the packaged three-node daemon/service adapter and run the complete small end-to-end path.
7. Run full compiler/Debug/Release/install suites, then ASan/UBSan/TSan, fuzz/corruption/crash,
   deterministic/chaos campaigns, SQL differential tests, and only afterward benchmarks/profiling/
   epoll-io_uring/SIMD/NUMA comparison and final tuning.
