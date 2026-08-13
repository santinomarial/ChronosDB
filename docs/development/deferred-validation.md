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
- Committed columnar-batch vector source: forced-allocation sweeps, all logical types, cancellation
  at every column, hostile chunk limits, large batches, scalar/vector differential coverage, and
  memory/allocation profiles. Focused coverage now proves bounded canonical slicing and a checked
  row-preserving filter/projection pipeline. The plan-bound evaluator now constructs one
  deterministic key and bounded result payload per committed append and fits the existing live
  coordinator/protocol contract. A bounded service fan-out now routes applied table/tablet/WAL
  matches and contains evaluator/publication failure as explicit continuity loss. Forced failures
  across multi-chunk result collection, broad type matrices, incremental stateful-plan routing, and
  broader daemon delivery matrices remain integration work.
- Single-node applied-append observation: the native ingest and SQL INSERT product paths now share
  one database-owned executor seam, and focused coverage proves that one applied mutation notifies
  while its matching retry does not. Fixed multiple-plan fan-out, evaluator/publication metrics,
  and continuity-loss containment are now composed at the service boundary. Dynamic plan
  registration/retirement, disconnect races, replay/startup interleavings, and sustained observer
  cost remain integration and Phase 18 work. Coordinator failure containment overflows old
  sessions/tokens before allowing a fresh snapshot.
- Write-synchronous live checkpointing now prevents an online acknowledged append from outrunning
  configured coordinator recovery. Broader checkpoint latency/failure profiling, batched group
  installation, and exact post-checkpoint WAL replay are deferred; no asynchronous relaxation is
  safe until replay and retention are composed.
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
- The single-plan service runtime now composes the stable pre-open observer address, durable fan-out,
  historical/live lifecycle, acknowledgement, and shutdown over dedicated bounded queues. Daemon
  plan registry/key configuration and real reactor queue multiplexing now exist for one configured
  row-preserving plan. Multi-plan routing, key rotation, hostile startup namespaces, and Linux
  subprocess execution across disconnect/backpressure variants remain integration work.
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

- Raft Transport Envelope v1 cross-compiler golden fixtures, hostile length/count/reserved-byte
  matrices, sustained fuzzing, allocation-failure sweeps, carrier-wide admission schedules,
  runtime poll composition across inbound/outbound/timer/durable completion, mixed-version processes,
  and
  carrier-integrated duplicate/loss/reorder/partition simulation. Focused coverage round-trips every
  current message, including an actual conflict-repair response, rejects damage, unknown kinds,
  route mismatch and bounds, and exercises the header-first exact-allocation reader and owned
  short-write cursor across fragmentation and coalescing. Authenticated principal/source
  authorization, exact local routing, asynchronous durable admission, and post-sync response
  encoding have focused coverage. A persistent real mutual-TLS inbound session now authenticates,
  reads fragmented exact frames, pauses for durable completion, and publishes complete results.
  A bounded TCP listener/poll owner now admits those sessions and retains result-ready connections
  until explicit pickup. Stable connection IDs and external readiness/closure driving now permit one
  outer poll table; overload/churn and unified runtime polling remain deferred.
  Each admitted inbound message now returns its exact ordered post-message group observation for
  timer rearming; high-contention observation ordering remains part of cluster stress validation.
  A persistent peer-authenticated outbound session bounds FIFO frames/bytes, retains short writes,
  and drains complete originals for duplicate-safe reconnect retry. A fixed-capacity peer pool
  preflights every destination and aggregate queue bound before routing, and removes failed carriers
  only through a complete retry-frame handoff.
  One exact-route TCP attempt now retains retry bytes across connect/timeout and transfers descriptor
  ownership with the borrowing TLS carrier. Connection churn, multi-address policy, and multi-peer
  scheduling remain deferred. One immutable peer now has exact-deadline capped exponential
  reconnect and lossless failed-frame custody. A fixed multi-peer manager now owns route validation,
  descriptor interests, pool installation, failure recycling, and fresh-result backpressure.
- Long seed matrices, exhaustive bounded schedules, chunk/dependency trace shrinking, timer/clock
  changes, physical segmented-log syscall faults, and automated randomized membership/snapshot/read-
  barrier generation. The bounded simulator now has focused explicit and seeded coverage for
  partition, delay/reordering, duplicate/loss, crash/restart, atomic persistence failure,
  application, joint membership, local compaction, exact replay, safety-model comparison, and
  deletion shrinking (eight seeds and 4,000 generated actions in the focused test).
- Extend the implemented hostile higher-term/payload-identity and pre-observation term/position/
  response-state regression checks into exhaustive persistence-before-response, committed-log
  overwrite, sequence-exhaustion, response-state, and snapshot-boundary properties.
- Persistent file owner, vote/log fsync ordering, crash/restart at every transition, idempotent
  recovery, application to tablet state, snapshot creation/install, and log compaction. Extend the
  implemented read barrier through production transport and tablet snapshot acquisition.
- Membership protocol, leader leases if ever proposed, completion-pipe saturation/shutdown races,
  timerfd integration, disk-error behavior, and storage fault injection. The asynchronous durable
  owner now supplies a portable coalescing completion descriptor. A bounded generation-tagged timer
  scheduler now emits exact election/heartbeat actions and rejects stale completion rearming; its bounded driver
  submits two-operation action/observation batches through the asynchronous durable owner and
  retains complete post-sync results for routing.
  Timer and transport owners now expose their exact earliest monotonic deadlines; clock-change and
  high-cardinality deadline-scan validation remain deferred.
  Runtime-lifetime FIFO submission identities now order timer and multi-connection inbound results;
  exhaustion and high-contention mixed-producer ordering remain deferred.
  Inbound disconnect now retains already admitted durable work through result pickup; exhaustive
  disconnect timing, descriptor-pressure, and crash matrices remain deferred.
  Outbound terminal events now immediately retain whole retry frames and enter capped reconnect;
  partial-write terminal matrices and reconnect-storm stress remain deferred.
  Outbound Raft TLS now begins with client-write readiness; handshake fragmentation/failure and
  reconnect matrices remain deferred.
  One bounded Raft transport runtime now composes durable wakeups, exact deadline-clamped polling,
  inbound/outbound readiness, FIFO activity/results, retry-safe routing, and application pickup.
  Result-ring saturation, mixed external completion producers, many-group/peer skew, storage stalls,
  disconnect/reconnect storms, and deterministic multi-node production-carrier faults remain.
- Broad leader-churn and partition matrices, semantic/chunk trace shrinking, clock changes, physical
  disk failures, ASan/UBSan/TSan, fuzzing, independent model checking, commit/catch-up/snapshot
  benchmarks, and API review.

## Phase 15 — Multi-Raft tablets and metadata

- Extend the implemented segmented node-level writer, rotation, complete recovery scan, explicit
  tail repair, corruption rejection, caller-batched sync, and anchored all-group physical-prefix
  reclamation with injected I/O failures, process-crash testing, asynchronous scheduling, and
  metrics.
- Exercise the v1.1 snapshot membership checkpoint with golden minor-0/minor-1 fixtures,
  mixed-version processes, hostile voter counts, snapshot-install crash points, and reclamation.
- Connect the implemented two-stage Raft snapshot request/completion boundary to versioned tablet
  and metadata snapshot bytes, resumable transfer, manifest installation, and process-crash tests.
- Extend the implemented one-worker bounded durable Multi-Raft FIFO and ordered owning observations
  with allocation/worker-start/I/O failure injection, reactor continuations, observation
  deadlines/coalescing, timer batching, thread placement, and measured group-aware
  fairness/no-starvation under hot/cold skew. The worker-affine application extension seam now
  initializes, prepares/completes each durable batch before publication, and shuts down on that
  owner. Its concrete bounded tablet application owns recovery, touched-group application,
  applied-index persistence, pinned snapshots, latest receipts, and weakly owned exact
  group/leader-term/index receipt completions on the worker. The nonblocking replicated ingest
  operation now exact-validates proposal persistence, applied receipts, retry outcomes, and
  protocol-v2 acknowledgement projection. Reactor tasks now preserve exact negotiated protocol
  context, and a bounded coordinator now derives routes from committed placement/binding metadata,
  validates complete active-schema authority before and after observation, verifies ordered stable
  local leadership, and owns fair multi-request polling, exact cancellation, deadlines,
  correlation, and metrics. A flat bounded extension set composes exact
  application owners with ordered callbacks, reverse partial-failure cleanup, and direct-child
  identity proof. The concrete metadata extension now performs pre-admission retained/snapshot
  recovery, touched-group application, durable applied-index advancement, and immutable catalog
  snapshot publication on that worker. The metadata log now durably binds placed tablets to their
  Raft groups through additive entry type 4 and Snapshot minor 1; add administrative legacy binding
  backfill. An owning replicated runtime now fixes group/application identity and address-stable
  create/reopen/shutdown ordering. A bounded queue-facing service now preserves negotiated tasks,
  exact cancellation, one retained response, response-wakeup reporting, and admission-close/drain;
  a database-root owner now reconstructs resident tablet applications from committed global
  metadata and explicit group membership before reopening the asynchronous runtime. A strict
  bounded group/voter deployment parser and secure packaged daemon loading/routing now exist. A
  strict authenticated peer route/identity parser and exact certificate/address/node authority also
  exist, and an owning transport runtime plus packaged daemon poll thread composes them with TLS
  contexts and randomized timers; add in-memory credential loading, remote leader redirection,
  metadata/tablet snapshot process recovery,
  joint-reconfiguration restart matrices, broader queue/disconnect failure matrices, long-running
  hook watchdog evidence, and TSan scheduling coverage.
- Carry the implemented group-scoped read-barrier operation through authenticated production
  transport, request deadlines/coalescing, apply waiting, and exact tablet snapshot acquisition.
- Extend the implemented metadata Raft codec/application/reopen path and complete-schema-definition
  entry, complete table-policy command, and owning deterministic recovery projection with cluster
  epochs and snapshot transfer/reclamation around the implemented exact-entry Metadata Application
  Snapshot v1 codec, locked storage, install-before-Raft compaction, and snapshot-plus-suffix
  recovery. Add golden fixtures, fuzzing, allocation/crash injection, policy-transition matrices, and
  large-catalog limits/measurements.
- Extend the implemented committed-only tablet command application and full retained-log rebuild
  with crash injection around publication/applied-index persistence. Extend the implemented durable
  application-snapshot creation/compaction plus prefix/suffix recovery with mismatch/fault matrices,
  obsolete-file and physical-log reclamation fault matrices and scheduling; version CSEG/Manifest
  row identities for Raft source/group positions, and cover query row-version columns and
  compaction migration. Protocol 2.0 now negotiates the implemented joint-membership
  quorum-sync/application receipt without changing v1 bytes. Carry that request through the
  authenticated replicated service owner, enable advertisement only there, and add explicit
  configuration identity, metrics, cancellation/timeouts, mixed-version fixtures/fuzzing, and
  minority-loss crash reconciliation before deployment exposure.
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
  socket integration, whole-query deadlines, and local cancellation for this aggregate path. A
  production receiver service now acquires one request-local owning Manifest/schema/placement/
  group/barrier context and invokes the proof-revalidating real-CSEG worker. General physical
  pipeline stage/expression serialization, connection pooling/multiplexing, multi-key/non-FLOAT64
  grouping-state codecs and coordination, ordering, top-N, LIMIT, remote worker interruption,
  durable retries, and broader coordinator/worker failure cleanup remain. A distinct fixed frame
  now canonically carries one nullable FLOAT64 group key plus one mergeable aggregate partial,
  including exact signed-zero/NaN group equivalence. Its fixed reader and move-only write cursor now
  cover every fragmentation boundary, coalesced suffix ownership, sticky damage, and short writes.
  A distinct terminal-only frame closes an empty tablet stream without inventing a SQL NULL group.
  Its separate fixed reader and move-only cursor own every terminal fragmentation boundary,
  coalesced successor bytes, sticky damage, and checked short writes without introducing an
  implicit stream discriminator.
  A bounded single-owner coordinator now enforces contiguous per-tablet grouped sequences, exact
  canonical retry history, empty-tablet terminals, first-failure arbitration, all-tablet closure,
  and cross-tablet nullable-FLOAT64 group merging. Sender/coordinator integration remains
  incomplete. A distinct exact grouped-intent envelope now binds one
  projected key index around the existing snapshot/route/proof-bound Fragment v1 bytes; schema/type
  authority binding now reuses the complete aggregate binder and proves the grouped key against the
  same pinned schema. A distinct group-scoped executable dispatch now binds that intent to its
  nonnil Raft group without changing ungrouped bytes, and the authority binder can now package its
  exact validated owned values directly into that dispatch without a second caller-side join. The
  grouped worker now reuses every local
  authority gate, resolves real temporal CSEG winners, emits canonical terminal partials, and uses
  the terminal-only value for empty selected input. Distinct exact grouped request/response codecs
  now carry the canonical dispatch and discriminate one correlated partial, empty terminal, or
  failure without changing ungrouped transport bytes. Fixed-storage request/response readers and a
  move-only validated write cursor now own fragmented/coalesced reads and short writes at those
  bounds. An authenticated receiver now authorizes the source, validates the complete bounded
  contiguous worker stream, contains worker failures, and publishes only an all-encoded response
  vector. A production request-local service adapter now acquires coherent owning Manifest/schema/
  placement/group/barrier authority and invokes the real-CSEG grouped worker. A bounded
  mutual-TLS client/server pair now owns authentication-before-bytes, ordered multi-response short
  I/O, finite deadlines, and terminal-only client publication on an already-connected socket. A
  deadline-bound outbound TCP composite now owns validation-before-connect, exact route identity,
  nonblocking completion, and TLS-before-descriptor teardown. Inbound listener/server ownership and
  sender/coordinator plus packaged grouped execution remain incomplete. A move-only service owner
  keeps the worker and authenticated receiver at stable addresses and proves the complete in-process
  canonical request-to-real-CSEG response path.
- Proof-bound leader-linearizable/bounded-stale/local-eventual admissions now remain attached through
  compatible pinned multi-tablet snapshots and protocol/carrier scheduling. Whole-query replacement
  now validates fresh caller-proved authority, identical logical shape, nonregressing generation,
  and a finite budget. Unavailable workers can now publish an authenticated advisory leader/epoch
  from a committed metadata-provider boundary. Add automatic metadata acquisition and broader
  leader/placement-change integration during long scans without silent downgrade.
- Packaged leader-linearizable construction now derives active schema, placement, group bindings,
  exact IPv4-or-DNS/TLS routes, correlated quorum barriers, and one compatible Manifest epoch from
  committed owners before creating the TCP lifecycle. Packaged bounded-stale construction applies
  the same gates to stable same-term leader/follower observations. Fresh bounded DNS acquisition,
  ordered unique IPv4 candidates, and finite candidate rotation under the existing sender retry
  budget have focused parser, route, and real-mTLS refused-address coverage. Native protocol/process
  integration, stale metadata refresh, live DNS churn/failure, resolver-latency and cache policy,
  IPv6, allocation/cancellation fault injection at every construction boundary, and broad
  multi-node failure matrices remain deferred.
- Raft Observation Transport v1 now carries one exact source/target/group/correlation request and
  one complete bounded ordered observation or failure response. The authenticated receiver checks
  trust, principal/source authority, and local target before invoking an embedding-owned durable
  observation service. Request/success/failure codec, corruption/version/bound rejection, trust
  ordering, service failure, and exception containment have focused coverage. Header-first bounded
  readers now reject declared lengths before response allocation, preserve coalesced suffixes, and
  pair with move-only validated short-write ownership. A maintained outbound mTLS attempt now
  authenticates and authorizes the exact target before writing, applies handshake/exchange
  deadlines, and exact-matches response route/group/correlation. A nonblocking TCP owner now binds
  the authentication address to the route, proves connect completion, applies its own deadline, and
  closes in TLS-before-descriptor order. An accepted-socket mTLS session authenticates the client
  before reading, invokes the receiver once, and owns the complete response under exact deadlines.
  A dedicated bounded TCP server now owns listener/TLS lifetime, finite per-poll admission, stable
  TLS-before-descriptor connection records, metrics, and deterministic shutdown. A single-node
  acquisition owner now rotates a bounded ordered address snapshot across one finite retry/backoff
  budget without changing request authority. A selected leader/follower owner now fans out both
  authenticated acquisitions, cancels the survivor on failure, and publishes only a complete
  same-term stable-membership pair. Selected nodes now resolve through canonical committed metadata
  into bounded numeric/DNS address snapshots with exact TLS contexts before polling. A canonical
  batch owner now starts every selected group pair before blocking, waits on the global earliest
  deadline, cancels all survivors on failure, and publishes only a complete group-sorted vector. Add
  A placement-backed constructor now derives every planned group, prefers an eligible coordinator
  follower or the lowest nonleader replica, resolves unique targets once, and assigns overflow-safe
  correlations before I/O. A service lifecycle now retains the plan and pinned Manifest snapshot,
  acquires the complete remote authority batch, binds it through the metadata barrier, and transfers
  directly into the TCP query owner with cross-phase cancellation and metrics. A production inbound
  owner now composes the authenticated receiver, request-local authority provider,
  proof-revalidating worker, and bounded mTLS server; a real loopback request returns the exact
  installed-CSEG aggregate. Add alternate-follower policy, broader cancellation/allocation faults,
  moved/multi-tablet remote CSEG gates, and real multi-process validation.
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
- Extend the packaged `chronosd` authenticated Raft peer lifecycle with remote leader routing,
  remote fragments and a globally atomic cross-group policy beyond the implemented applied
  read-barrier vector, live delivery, flush/CSEG/Manifest,
  failover/movement, and object storage; then run it as three processes.
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
  source-free SELECT, streaming response backpressure,
  cancellation, concurrent query/DDL/shutdown, every logical cell type, scalar/vector differential
  coverage, ASan/UBSan/TSan, and latency/memory profiles. Focused coverage now proves table-wide
  count encoding, bounded overflow, described zero-row completion, and whole-table ASOF execution
  over one mixed Manifest/CSEG/head epoch before and after restart. Whole-table ASOF allocation
  injection, fuzzing, large tablet/source counts, and profiles remain deferred.
- Compose `FOR SYSTEM_TIME AS OF` with the accepted Temporal Mutation Command v1 and
  CSEG/Manifest v2 recovery owners. Native admission now fails closed before current-snapshot
  execution, and focused coverage proves a historical aggregate cannot silently return present
  rows. Mixed current/temporal command dispatch, history retention, restart, and protocol-level
  historical result coverage remain deferred.
- Native CREATE TABLE system-identity entropy failure injection, duplicate/nil generator tests, DDL
  response boundary tests, client retry identities, fully complete duplicate-request behavior,
  concurrent/stale DDL, authorization, ALTER/DROP/rename, crash matrices through protocol dispatch,
  and subprocess restart qualification. The common Linux/macOS system UUID adapter is now shared by
  WAL, daemon bootstrap, and default native DDL/DML; focused coverage proves nonnil generation,
  injected identities, canonical durable completion fields, and immediate queryability.
- SQL INSERT columnar materialization allocation-failure sweeps, every logical type and integer
  boundary, hostile maximum-width variable values, exact preflight byte accounting before allocation,
  codec round trips, fuzz/property coverage, and ASan/UBSan. Focused coverage now proves schema-order
  transposition, schema pinning, typed NULL preservation, scalar round trips, and row-limit rejection.
- Native SQL INSERT client-supplied durable retry identity, multi-tablet event-time/shard routing,
  concurrent INSERT/query/shutdown schedules, identity-source collision/exhaustion faults, response
  boundary tests, every logical type, authorization, sustained head rotation with Manifest/CSEG
  flush, real-socket daemon execution, and ASan/UBSan/TSan. Focused coverage now proves LOCAL_SYNC
  WAL acknowledgement, immediate vector-query visibility, and restart recovery for one local tablet.
- Empty Manifest namespace initialization syscall-fault injection at every create/write/read/sync/
  rename boundary, subprocess crash images, concurrent process attempts under the aggregate root
  lock, permission/ownership qualification, hostile namespace matrices, Linux power-loss evidence,
  and full Manifest-aware single-node startup. Focused filesystem coverage now proves exact
  create/reopen, identity binding, temporary cleanup, and missing-lock fail-closed behavior; focused
  owner coverage proves live lock exclusion and WAL-before-Manifest shutdown ordering.
- Manifest-aware single-node startup/flush covered-segment reclamation, selected-identity
  corruption/namespace races, missing catalog bindings, multi-tablet/schema-evolution flush,
  background/abrupt-stop checkpoint scheduling, compression policy, forced WAL segment reclamation,
  legacy migration crash points, allocation/entropy faults, subprocess restarts, metrics, and
  ASan/UBSan/TSan. Focused owner
  coverage now routes empty and WAL-backed restarts through aggregate recovery and proves a live
  generation-2 CSEG flush, shutdown generation-3 checkpoint through record 2, and exact record-3
  suffix recovery. Native unary and ASOF queries count the complete mixed CSEG/head epoch before and
  after restart; distributed query sources remain uncomposed here.
- Execute the complete requested three-node scenario with real sockets/processes and retained logs:
  create table, ingest, historical SQL, vector distributed aggregate, subscribe/update, leader kill,
  failover ingest/query, movement/query, tier/query, restart, and result reconciliation.
- Full reproducible demo, operations/runbooks, observability, backup/restore, security review, and
  final documentation/API/dead-code reconciliation.
