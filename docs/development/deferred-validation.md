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

- Multi-tablet coordinator checkpoints: broader hostile corruption and allocation sweeps,
  filesystem fault injection at every write/sync/rename cut point, process crash/reopen,
  token/checkpoint race scheduling, obsolete-generation reclamation, cross-compiler golden
  verification, and retained-state size/recovery measurements.
- Subscription plan identity: cross-process golden vectors, forced-allocation/provider-failure
  sweeps, hostile SQL/catalog limits, registry crash/fault cut points, planner-upgrade compatibility,
  obsolete-definition reclamation, schema migration, and compatibility review when the supported
  incremental SQL surface expands.
- Plan-bound subscription snapshot execution: forced-allocation sweeps, cancellation at every
  pull/END_STREAM/READY transition, multi-chunk socket backpressure, reactor worker dispatch, and
  real concurrent publication scheduling. Exercise every global operator over multi-tablet source
  vectors and extend the exact snapshot boundary to Raft-backed publications.
- Commits injected at every historical-to-live handoff step across real snapshot execution.
- Cross-tablet-owner wiring around the implemented canonical vector/admission-order coordinator,
  topology transitions, multi-plan retention registration/retirement, service SQL/plan-to-input
  suffix replay, concurrent post-checkpoint source-log replay, physical WAL/Raft coordinate mapping
  behind the implemented topology-bound deletion authority, schema migration, process restart
  integration, real-socket reactor/service threading, and obsolete-generation reclamation.
- Real-socket Protocol 1.1 partial delivery, disconnect/reconnect, duplicate replay, schema-change
  termination and coordinator replacement, cancellation races, allocation faults, checkpoint
  minor-0/minor-1 mixed-version peers, and sustained network backpressure integration around the
  implemented subscription messages and lifecycle.
- Full unit/property coverage for token hostile sizes/versions, retention expiry disclosure,
  subscriber fan-out, slow consumers, window expiration, negative event times, overflow, NaN,
  floating reproducibility, integer/decimal overflow, VWAP zero weight, OHLC endpoint removal,
  Welford inverse/merge shapes, and recomputation equivalence.
- ASan/UBSan/TSan, token/window fuzzing, restart/crash testing, state corruption, fan-out/latency/
  memory benchmarks, ingestion-impact measurement, and API review.

## Phase 12 — io_uring, SIMD, affinity, NUMA, and allocation

- Linux liburing install/export and broader unsupported-kernel, queue-full, timeout, cancellation,
  shutdown-race, response-wakeup, partial-write, connection-churn, and sanitizer parity tests beyond
  the focused Ubuntu build and fragmented socket lifecycle executed during the feature pass.
- Run an equal-semantics epoll/io_uring comparison with unfavorable runs before considering any
  default-backend change or relative performance claim.
- x86 runtime differential evidence for the implemented AVX2 timestamp filter; AVX-512 and
  additional AVX2/ARM NEON kernels; forced CPU-feature fallback matrices; scalar/SIMD equivalence
  under every supported compiler/CPU feature set; and comparative threshold/throughput benchmarks.
- Reactor/shard owned-thread affinity integration; query-worker real CPU-set coverage; NUMA provider
  and local allocation policy; invalid/offline CPU and topology behavior; broader Linux/macOS
  portability; TSan; NUMA experiments; and allocation cleanup based on profiles rather than
  speculation.

## Phase 13 — system-time history and corrections

- Exercise Temporal Mutation Command v1 with golden fixtures, fuzzing, hostile length/count and
  nested-batch corruption matrices, mixed versions, allocation failure, and cross-compiler bytes.
- Mixed `COLUMNAR_APPEND`/temporal recovery dispatch and application checkpoints; Manifest-backed
  multi-part/vector winner resolution beyond the implemented single-lineage scalar reference,
  strict metadata, full-part, bounded semantic-validation, and projected-granule paths;
- Manifest v2 expanded hostile decode, v1 migration, object-store installation, application
  recovery/publication, authorized compaction, and source-specific reclamation beyond the
  implemented local admission/install/selection and generation-pinned loading paths; live-executor
  fault injection plus crash and migration tests.
- Expand CSEG v2 projected-reader and semantic validation with golden bytes, hostile metadata/page
  matrices,
  WAL/Raft source-lineage cases, allocation failure, fuzzing, cross-version conversion, and
  cross-compiler fixtures after the accepted temporal registry and layout planner.
- Direct mutable-head/CSEG vector winner resolution and physical-plan lowering beyond the
  implemented scalar-snapshot vector source; full logical-type/allocation/cancellation matrices and
  current/as-of scalar-vector differential SQL; many-tablet/skew/allocation/crash coverage beyond
  the implemented distinct-table WAL checkpoint composition; versioned same-table tablet routing;
  Raft/mixed-source recovery composition, compaction equivalence, active-snapshot pins,
  durable tombstone/history retention authority, CSEG/Manifest replacement/reclamation, and audit
  interfaces beyond the implemented monotonic in-memory retention frontier.
- Generated multi-version property models, timestamp ties/boundaries, late corrections, crash/
  corruption testing, SQL differential testing, storage amplification, scan/compaction benchmarks,
  and retention sensitivity.

## Phase 14 — deterministic Raft

- Extend the implemented hostile higher-term/payload-identity regression checks into exhaustive
  persistence-before-response, committed-log overwrite, sequence-exhaustion, response-state, and
  snapshot-boundary properties.
- Persistent file owner, vote/log fsync ordering, crash/restart at every transition, idempotent
  recovery, application to tablet state, snapshot creation/install, and log compaction. Extend the
  implemented read barrier through production transport and tablet snapshot acquisition.
- Membership protocol, leader leases if ever proposed, real transport framing/versioning, timer
  runtime, disk-error behavior, and storage fault injection.
- Exhaustive bounded schedules, long randomized deterministic simulation with trace replay/shrink,
  partitions, loss/duplication/reordering, clock changes, leader churn, disk failures, ASan/UBSan/
  TSan, fuzzing, model checking, commit/catch-up/snapshot benchmarks, and API review.

## Phase 15 — Multi-Raft tablets and metadata

- Extend the implemented segmented node-level writer, rotation, complete recovery scan, explicit
  tail repair, corruption rejection, and caller-batched sync with injected I/O failures, per-group
  reclamation/checkpointing, process-crash testing, and metrics.
- Exercise the v1.1 snapshot membership checkpoint with golden minor-0/minor-1 fixtures,
  mixed-version processes, hostile voter counts, snapshot-install crash points, and reclamation.
- Connect the implemented two-stage Raft snapshot request/completion boundary to versioned tablet
  and metadata snapshot bytes, resumable transfer, manifest installation, and process-crash tests.
- Extend the implemented one-worker bounded durable Multi-Raft FIFO and ordered owning observations
  with allocation/worker-start/I/O failure injection, reactor continuations, observation
  deadlines/coalescing, timer batching, thread placement, and measured group-aware
  fairness/no-starvation under hot/cold skew.
- Carry the implemented group-scoped read-barrier operation through authenticated production
  transport, request deadlines/coalescing, apply waiting, and exact tablet snapshot acquisition.
- Extend the implemented metadata Raft codec/application/reopen path with complete schema
  definitions, cluster epochs, metadata application snapshots, golden fixtures,
  fuzzing, crash injection, and large-catalog limits/measurements.
- Extend the implemented committed-only tablet command application and full retained-log rebuild
  with crash injection around publication/applied-index persistence. Extend the implemented durable
  application-snapshot creation/compaction plus prefix/suffix recovery with mismatch/fault matrices,
  obsolete-file and physical-log reclamation; version CSEG/Manifest row identities for Raft source/
  group positions, and cover query row-version columns and compaction migration. Carry the implemented joint-
  membership quorum-sync/application receipt through authenticated transport, protocol negotiation,
  explicit configuration identity, metrics, timeouts, and minority-loss crash reconciliation before exposing
  the client mode.
- Thousands-of-groups simulation, one-node loss, group lifecycle, persisted reopen, noisy-neighbor,
  TSan/chaos, physical amplification, group density, memory, and tail benchmarks.

## Phase 16 — distributed query and rebalancing

- The canonical fixed-width ungrouped aggregate exchange frame, exact codec, aligned in-memory
  state admission, and constant-storage fragmented/coalesced read plus short-write ownership are
  implemented. Contiguous per-tablet sequence admission, bounded bit-exact retry history, terminal
  closure, and first-failure arbitration are also implemented. The current projected Float64
  aggregate path has canonical snapshot/route/proof/projection/event-filter fragment request bytes
  plus an exact Raft-group-scoped executable dispatch envelope. Runtime binding now exact-matches
  admission, committed placement, destination schema, and one pinned Raft-backed Manifest v2
  generation before constructing that envelope.
  General physical pipeline stage/expression serialization, worker execution, socket integration
  and connection backpressure, grouping-state codecs, reconnect/resend protocol, ordering, top-N,
  LIMIT, cancellation, durable retries, and broader coordinator/worker failure cleanup remain.
- Carry the implemented proof-bound leader-linearizable/bounded-stale/local-eventual admissions
  through compatible pinned multi-tablet snapshots, protocol/carrier integration, and leader or
  placement changes during long scans.
- Shard-key pruning and statistics in addition to event-time pruning; multi-node scalar/distributed
  differential SQL and partial-aggregation equivalence.
- Carry the implemented authenticated remote-action receiver bytes and atomic current-leader-term
  admission fence plus the exact response and bounded sender retry state machine through a
  maintained TLS/socket carrier; carry the implemented nonblocking completion-to-response adapter
  through connection write ownership, carrier deadlines, connection-level leader refresh, and
  automatic metadata apply scheduling; extend exact remote
  duplicate delivery beyond the deterministic filesystem/runtime coverage;
  complete physical Manifest/CSEG
  handoff and response routing around the implemented Raft-completed RTAS, composed mixed-generation
  ready/later-phase checkpoint reconciliation, durable chunk owners, and published-ownership-gated
  terminal physical-receipt reclamation; compose the implemented committed-placement-authorized
  source-retirement Manifest builder, exact durable installer, and reader-pinned publication proof
  with exact reader-pinned source-part reclamation and authority-bound restart reconstruction; add
  durable completion state, old-generation reclamation, and broader filesystem crash/fault injection;
  source/target/metadata-leader failures, source
  switching, bandwidth limits, and stale routing.
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
