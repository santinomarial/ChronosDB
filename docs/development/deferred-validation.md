# Deferred Validation Ledger

> **Purpose:** execution plan for Phase 18. Nothing listed here has passed merely because it is
> listed. This ledger records work deliberately not performed during the feature-completion pass.

## Cross-cutting

- Full Debug and Release builds and the complete 900+ test repository suite after the final diff.
- Full unit, integration, regression, API, dead-code, documentation-reconciliation, install/export,
  and external-consumer review for every new public target.
- ASan, UBSan, and TSan matrices; GCC, Clang, AppleClang, Linux, macOS, x86-64, and arm64 validation.
- Sustained fuzzing, property testing, deterministic allocation-failure sweeps, corruption testing,
  crash testing, long-duration soak, chaos testing, and reproducible fault campaigns.
- Full benchmark suite, allocation profiling, CPU/I/O profiles, flame graphs, p50/p95/p99/p99.9,
  throughput, recovery/failover time, and final performance tuning. No performance result is claimed.

## Phase 11 — live subscriptions and materialized views

- Commits injected at every historical-to-live handoff step across real snapshot execution.
- Multi-tablet registration/merge ordering, topology epochs, retention pins, restart recovery, and
  durable materialized-view checkpoints.
- Native-protocol subscription messages, partial delivery, disconnect/reconnect, duplicate replay,
  schema-change termination, cancellation races, and network backpressure integration.
- Full unit/property coverage for token hostile sizes/versions, retention expiry disclosure,
  subscriber fan-out, slow consumers, window expiration, negative event times, overflow, NaN,
  floating reproducibility, integer/decimal overflow, VWAP zero weight, OHLC endpoint removal,
  Welford inverse/merge shapes, and recomputation equivalence.
- ASan/UBSan/TSan, token/window fuzzing, restart/crash testing, state corruption, fan-out/latency/
  memory benchmarks, ingestion-impact measurement, and API review.

## Phase 12 — io_uring, SIMD, affinity, NUMA, and allocation

- Linux liburing configure/build/install and unsupported-kernel, queue-full, timeout, cancellation,
  shutdown, response wakeup, partial read/write, connection churn, and sanitizer parity tests.
- Replace or validate the readiness pilot with a full socket-operation io_uring backend before any
  production-backend claim; run equal-semantics epoll/io_uring comparison with unfavorable runs.
- AVX2, AVX-512, and ARM NEON kernel implementations/comparisons beyond existing portable vector
  operators; scalar/vector equivalence under every supported CPU feature set.
- Reactor/query/shard affinity integration, NUMA provider and local allocation policy, invalid CPU/
  topology behavior, Linux/macOS portability, TSan, NUMA experiments, and allocation cleanup based
  on profiles rather than speculation.

## Phase 13 — system-time history and corrections

- Accepted WAL application encoding and CSEG v2 system columns/operation codes for corrections,
  replacements, tombstones, receive time, and system commit time; golden/corruption/migration tests.
- Mutable-head and CSEG vector visibility resolution, physical-plan integration, current/as-of
  scalar-vector differential SQL, flush/restart recovery, compaction equivalence, active-snapshot
  pins, tombstone/history retention, and audit interfaces.
- Generated multi-version property models, timestamp ties/boundaries, late corrections, crash/
  corruption testing, SQL differential testing, storage amplification, scan/compaction benchmarks,
  and retention sensitivity.

## Phase 14 — deterministic Raft

- Extend the implemented hostile higher-term/payload-identity regression checks into exhaustive
  persistence-before-response, committed-log overwrite, sequence-exhaustion, response-state, and
  snapshot-boundary properties.
- Persistent file owner, vote/log fsync ordering, crash/restart at every transition, idempotent
  recovery, application to tablet state, snapshot creation/install, log compaction, and read index.
- Membership protocol, leader leases if ever proposed, real transport framing/versioning, timer
  runtime, disk-error behavior, and storage fault injection.
- Exhaustive bounded schedules, long randomized deterministic simulation with trace replay/shrink,
  partitions, loss/duplication/reordering, clock changes, leader churn, disk failures, ASan/UBSan/
  TSan, fuzzing, model checking, commit/catch-up/snapshot benchmarks, and API review.

## Phase 15 — Multi-Raft tablets and metadata

- Extend the implemented segmented node-level writer, rotation, complete recovery scan, explicit
  tail repair, corruption rejection, and caller-batched sync with injected I/O failures, per-group
  reclamation/checkpointing, process-crash testing, and metrics.
- Put `DurableMultiRaftRuntime` behind a bounded asynchronous worker pool; batch transport and
  application alongside its implemented persistence release, and prove fairness/no starvation
  under hot/cold skew.
- Encode/decode metadata commands through the metadata Raft group; durable schemas, nodes, tablet
  placement, membership, leader hints, retention, cluster epochs, and metadata snapshots.
- Apply committed tablet commands to existing ingestion/table-state machinery; prove uncommitted
  invisibility. Implement and validate true QUORUM_SYNC before exposing the mode.
- Thousands-of-groups simulation, one-node loss, group lifecycle, persisted reopen, noisy-neighbor,
  TSan/chaos, physical amplification, group density, memory, and tail benchmarks.

## Phase 16 — distributed query and rebalancing

- General coordinator/worker physical fragments for existing vector plans, projection/filter/scan
  serialization, bounded framed exchanges, partial message I/O, grouping-state codecs, ordering,
  top-N, LIMIT, cancellation, retries, and coordinator/worker failure cleanup.
- Real leader-linearizable read index, bounded-stale proof, local-eventual routing, compatible
  multi-tablet snapshots, routing/placement epochs, and no silent consistency downgrade.
- Shard-key pruning and statistics in addition to event-time pruning; multi-node scalar/distributed
  differential SQL and partial-aggregation equivalence.
- Integrate movement with joint/safe Raft membership, resumable durable snapshot files, manifest/
  CSEG install, source/target/leader failures, source switching, bandwidth limits, and stale metadata.
- Partitions, duplicate/lost exchanges, skew, chaos, movement at every state, foreground interference,
  scale-out/exchange/coordination/failover benchmarks, and sanitizer/fuzz/property coverage.

## Phase 17 — object storage and interoperability

- Production S3-compatible HTTP/authentication backend, pinned dependency record, credentials,
  timeouts, retry/backoff, multipart upload, conditional immutable put, eventual listing behavior,
  restore, remote deletion, encryption boundary, and object-store fault injection.
- Manifest v2 cold-location descriptor and atomic old/new recovery; CSEG validation before upload,
  safe local deletion, snapshot/compaction pins, cache concurrency, crash/restart, cache index
  recovery, remote corruption, and page-range checksum integration.
- Arrow IPC and Parquet import/export providers, schema/logical-type mapping, fixtures, round trips,
  dependency/SBOM review, and explicit proof that CSEG remains the primary store.
- Cache-hit, upload/download/range scan/restore/request-cost/egress/foreground-impact profiles;
  eviction/property/fuzz/corruption tests; Linux/macOS and install/export validation.

## End-to-end integration

- Packaged three-node daemon and service adapter connecting native protocol, auth, ingest, WAL/Raft,
  mutable heads, flush/CSEG/manifest, SQL execution, live delivery, metadata routing, failover,
  movement, and object storage.
- Execute the complete requested three-node scenario with real sockets/processes and retained logs:
  create table, ingest, historical SQL, vector distributed aggregate, subscribe/update, leader kill,
  failover ingest/query, movement/query, tier/query, restart, and result reconciliation.
- Full reproducible demo, operations/runbooks, observability, backup/restore, security review, and
  final documentation/API/dead-code reconciliation.
