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
- Extend the implemented metadata Raft codec/application/reopen path and complete-schema-definition
  entry, complete table-policy command, and owning deterministic recovery projection with cluster
  epochs, metadata application snapshots,
  golden fixtures, fuzzing, allocation/crash injection, policy-transition matrices, and
  large-catalog limits/measurements.
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
  generation before constructing that envelope. The first worker path reproves local route,
  placement, barrier, schema, group, and durable snapshot authority, resolves logical winners from
  validated generation-pinned temporal parts, and emits a filtered terminal Float64 partial.
  The authenticated TCP server/client and pinned multi-tablet retry scheduler now provide bounded
  socket integration, whole-query deadlines, and local cancellation for this aggregate path.
  General physical pipeline stage/expression
  serialization, connection pooling/multiplexing, grouping-state codecs, ordering, top-N, LIMIT,
  remote worker interruption, durable retries, and broader coordinator/worker failure cleanup
  remain.
- Proof-bound leader-linearizable/bounded-stale/local-eventual admissions now remain attached through
  compatible pinned multi-tablet snapshots and protocol/carrier scheduling. Whole-query replacement
  now validates fresh caller-proved authority, identical logical shape, nonregressing generation,
  and a finite budget. Unavailable workers can now publish an authenticated advisory leader/epoch
  from a committed metadata-provider boundary. Add automatic metadata acquisition and broader
  leader/placement-change integration during long scans without silent downgrade.
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
  The focused real-mTLS two-tablet query/movement/query gate passes, but uses deterministic worker
  aggregates and externally committed milestone simulation rather than a packaged multi-process
  cluster or remote installed-CSEG scan.

## Phase 17 — object storage and interoperability

- Live AWS/MinIO/LocalStack validation for the libcurl SigV4 backend; authenticated-proxy/live
  CONNECT qualification,
  fleet-scale jitter distribution/recovery and clock-step/skew simulation, high-concurrency
  multipart stress and live throughput tuning, live concurrent-writer qualification,
  timeout/TLS/partial-response faults, eventual
  listing behavior, restore, conditional-delete races, live SSE-S3/SSE-KMS and bucket-policy
  qualification, and broader object-store fault injection. Durable cold-history discovery now completes
  interrupted remote reclamation before reader admission with focused evolving-catalog, bound,
  metadata-mismatch, deletion, and idempotent-retry coverage; subprocess crash points,
  multiple-object partial failures, and live-provider qualification remain deferred.
  Bounded replay-safe retries, capped backoff, per-attempt signing, explicit provider refresh after
  401/403, transient-service recovery, and exact attempt exhaustion have focused local coverage.
  Delta-seconds Retry-After parsing and the configured ceiling have focused local coverage.
  Strict IMF-fixdate, RFC 850, and asctime Retry-After parsing with calendar/weekday validation has
  focused local coverage under the same ceiling.
  Per-store atomic bounded retry jitter has focused deterministic floor/ceiling and invalid-bound
  coverage; statistical distribution and fleet recovery simulation remain deferred.
  Two independent signed clients racing one key have focused equal-body convergence and unequal-body
  single-winner/no-overwrite coverage; multi-process and live-provider races remain deferred.
  Ambient-proxy exclusion and secret-bearing explicit-proxy rejection have focused local coverage.
  Explicit SSE-S3/SSE-KMS upload headers, exact HEAD verification, malformed configuration, and
  fail-closed stored-mode mismatch have focused local coverage. SSE-C, DSSE-KMS, encryption context,
  Bucket Keys, and live KMS identity normalization remain deferred.
  The explicit immutable environment provider has focused standard-variable, signed-request,
  incomplete-value, secret-redaction, and fail-closed refresh coverage.
  The ordered provider chain has focused absence-only fallback, stable precedence, pinned refresh,
  outage-stop, and invalid-composition coverage. The explicit container workload provider has focused
  cache/refresh, Authorization, strict JSON/expiration, signed-S3, TLS-default, and redaction coverage;
  live ECS/EKS and token-file rotation remain deferred.
  The explicit IMDSv2-only instance provider has focused token/role/credential sequencing, cache,
  forced refresh, link-local authority, role/path, and token-TTL coverage; live EC2 IPv4/IPv6,
  hop-limit, throttling, and metadata-disable qualification remain deferred.
  Bounded parallel multipart creation, encoded upload IDs, signed parts, conditional completion,
  exact final verification, strict HTTP-200 embedded-error rejection, and abort after part/completion
  failure have focused local coverage. A two-worker overlap barrier and sorted completion test cover
  the bounded scheduler; TSan, high-part-count stress, completion races, abort failure, and bucket
  lifecycle cleanup remain deferred.
- Subprocess/power-loss coverage for the implemented durable component and pair-commit installers;
  TSan coverage for the implemented atomic shared pair publisher; broader snapshot/compaction pins,
  remote corruption, and
  page-range checksum integration. Exact schema/source-bound Manifest-v1 CSEG validation now runs
  before upload and has focused corrupt-bytes and wrong-WAL tests proving zero remote mutation.
  Mutex-linearized cache concurrency has focused eight-reader eviction and exact-byte coverage;
  TSan, adversarial scheduling, duplicate-download accounting, and latency evidence remain deferred.
  The cache is intentionally volatile rather than a second durable index; exact catalog restoration,
  empty-cache restart, verified demand rebuild, and all-or-nothing metadata mismatch have focused
  coverage. Subprocess restart and large-catalog/object-store fault matrices remain deferred.
  Source-general local reclamation has focused WAL-owned remote validation, deletion, tier-aware
  restart, and remote-query coverage in addition to the existing Raft/pin/corruption/retry cases;
  mixed-source batches and subprocess crash points remain deferred.
- Independent Arrow IPC/Parquet fixtures and multi-version compatibility; hostile compression-ratio
  and allocator-failure injection; Linux package qualification and broader SBOM automation for the
  implemented optional provider. Every current logical type, schema rejection, corruption, file
  limits, round trips, dependency ownership, and the CSEG boundary have focused coverage.
- Cache-hit, upload/download/range scan/restore/request-cost/egress/foreground-impact profiles;
  eviction/property/fuzz/corruption tests; Linux/macOS and install/export validation.

## End-to-end integration

- Single-node owner crash/fault injection at root, Raft, catalog projection, WAL creation/replay,
  coordinator start/drain, and shutdown boundaries; table creation fault injection after every
  schema/policy/placement proposal, application, catalog rebuild, and tablet publication; all
  incomplete/divergent prefix matrices; concurrent/stale DDL and identity generation;
  corrupt/unknown-table WAL; schema-evolution and multi-tablet recovery; concurrent ingest/query
  shutdown; Manifest/CSEG composition; subprocess lock/restart tests; ASan/UBSan/TSan; Linux/macOS
  persistence, installation/export, API, metrics, and startup/large-catalog profiling.
- Tablet-state vector pipeline allocation fault injection, concurrent sealed/active publication
  schedules, schema-evolution generations, large multi-tablet sets, every physical operator, full
  scalar/vector SQL differential testing, ASan/UBSan/TSan, and generation-count/chunk-size profiles.
- Database Bootstrap v1 golden fixture and fuzz corpus; allocation and syscall-fault injection;
  subprocess crashes after intent write/sync, each directory creation/sync, rename, and final root
  sync; concurrent multi-process creation; large-root scaling; Linux filesystem/power-loss and macOS
  persistence qualification; install/export and public API review.
- Compose the packaged `chronosd` lifecycle with a durable service adapter connecting native
  protocol, auth, ingest, WAL/Raft, mutable heads, flush/CSEG/manifest, SQL execution, live delivery,
  metadata routing, failover, movement, and object storage; then run it as three processes.
- Configured `chronosd` Linux subprocess execution in CI, daemon ingest over real sockets, corrupt
  root/WAL/Raft startup cases, signals during ingest/query, queue saturation with multi-frame
  responses, cancellation, concurrent clients, TLS/auth configuration, secure UUID entropy-failure
  injection, metrics, privilege dropping, service-manager packaging, ASan/UBSan/TSan, and sustained
  load. The Linux-only test now covers CREATE/query/restart/query; this macOS run verified daemon
  build and durable root creation before the expected Linux-reactor rejection.
- Native ingest service adapter allocation/fault injection, event-time and ancestor-schema retry
  policy, authorization, cancellation during WAL wait, concurrent requests/shutdown, queue-worker
  integration, daemon subprocess/restart, ASan/UBSan/TSan, and throughput/latency profiles. Focused
  coverage now proves LOCAL_SYNC application, exact WAL acknowledgement, positionless matching
  retry, routing-envelope retention, and malformed-request conversion.
- Native vector query adapter allocation/fault injection, response-limit boundary/property tests,
  source-free SELECT, ASOF and Manifest/CSEG composition, streaming response backpressure,
  cancellation, concurrent query/DDL/shutdown, every logical cell type, scalar/vector differential
  coverage, ASan/UBSan/TSan, and latency/memory profiles. Focused coverage now proves table-wide
  count encoding, bounded overflow, and described zero-row completion.
- Native CREATE TABLE secure system-identity source integration, entropy failure injection,
  duplicate/nil generator tests, DDL response boundary tests, client retry identities, fully
  complete duplicate-request behavior, concurrent/stale DDL, authorization, ALTER/DROP/rename,
  crash matrices through protocol dispatch, and subprocess restart qualification. Focused coverage
  now proves injected identities, canonical durable completion fields, and immediate queryability.
- SQL INSERT columnar materialization allocation-failure sweeps, every logical type and integer
  boundary, hostile maximum-width variable values, exact preflight byte accounting before allocation,
  codec round trips, fuzz/property coverage, and ASan/UBSan. Focused coverage now proves schema-order
  transposition, schema pinning, typed NULL preservation, scalar round trips, and row-limit rejection.
- Execute the complete requested three-node scenario with real sockets/processes and retained logs:
  create table, ingest, historical SQL, vector distributed aggregate, subscribe/update, leader kill,
  failover ingest/query, movement/query, tier/query, restart, and result reconciliation.
- Full reproducible demo, operations/runbooks, observability, backup/restore, security review, and
  final documentation/API/dead-code reconciliation.
