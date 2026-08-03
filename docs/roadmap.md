# Roadmap and Phase Gates

ChronosDB has established the initial Phase 0 architecture baseline and has begun Phase 1. Phase 1A,
the build and tooling foundation, and Phase 1B, the portable binary foundations, are implemented.
Local verification is recorded per change; the Linux compiler/CI matrix remains the reference for
portable support and is not implied by a macOS-only local run. This status does not mean that the
complete Phase 1 gates have passed. Unimplemented portions of Phase 1 and all later phases remain
planned work, not implemented functionality or delivery commitments. The Phase 2–3
[WAL v1 format](formats/wal-v1.md), [recovery design](architecture/wal-recovery.md), and
[ADR 0013](adr/0013-wal-v1-format-and-recovery.md) are accepted design artifacts. The pure in-memory
WAL physical codec, fixtures, tests, fuzz target, minimal blocking POSIX file/directory primitives,
the segmented writer, bounded commit coordinator, locked discovery and verification, explicit
final-tail repair, replay-sink passes, existing-history reopen path, and read-only inspector now
exist. Coordinator metrics exist; no application-kind codec, process-kill crash-image harness, or
server-wide operational metrics/export path exists yet.
Work should proceed in order unless an accepted ADR explains why a limited dependency must move
earlier.

No phase passes because its code merely compiles. A phase passes only when its artifacts are reviewed, its applicable [invariants](architecture/invariants.md) have executable evidence, required tests pass in supported configurations, failure paths are exercised, and unresolved risks are recorded. Measurement gates establish reproducible baselines; they do not authorize unsupported performance claims.

## Phase 0 — Architecture and specifications

- **Scope:** establish product vocabulary, workload models, engineering priorities, component boundaries, invariants, non-goals, roadmap, and decision process; identify the first durable-format and protocol specifications required by implementation.
- **Explicit non-scope:** source code, build files, dependencies, CI, generated artifacts, benchmark results, and claims that any engine component exists.
- **Required artifacts:** repository constitution; documentation index; product vision/workloads; architecture overview/invariants/non-goals; glossary; this roadmap; ADR policy and template; tracked unresolved design questions for Phase 1–2 planning.
- **Correctness exit gate:** documents agree on commit, snapshot, temporal, ownership, durability, and historical-to-live semantics; accepted decisions are distinguished from deferred design; all documentation links resolve.
- **Testing exit gate:** automated or scripted documentation checks validate required files, links, and formatting when such tooling is introduced; for the initial bootstrap, equivalent commands and manual coverage review are reported.
- **Measurement exit gate:** no performance measurement is required; no benchmark or platform-performance claim is permitted.

## Phase 1 — Build and common foundations

- **Implementation status:** Phase 1A provides the C++23 target graph, pinned test/benchmark
  dependencies, presets, install/export rules, formatting, static analysis, sanitizer workflows,
  CI configuration, and the version-reporting proof executable. Phase 1B adds status/result values,
  byte views, checked unsigned arithmetic, bounded little-endian binary I/O, portable incremental
  CRC32C, unit/property-style tests, an optional ByteReader fuzz target, local-only microbenchmarks,
  and a learning document. The minimal durable POSIX file/directory layer adds explicit-offset
  transfer loops, synchronization, no-replace rename, and advisory locking with deterministic
  syscall injection. Time, general identity, logging, and broader test utilities remain
  unimplemented. The phase exit gates below remain unchanged and have not been declared complete.

- **Scope:** reproducible C++23 build profiles; foundational error/result, byte, checksum, file-I/O, time, identity, logging, and test utilities; sanitizers and initial Linux CI policy.
- **Explicit non-scope:** WAL record semantics, storage engines, SQL, networking, custom allocators without a measured user, and production dependencies not covered by policy/ADR.
- **Required artifacts:** build/toolchain specification; dependency policy; coding/test conventions; platform support matrix; common-library interfaces; structured diagnostics; unit tests; CI and sanitizer configurations; learning document for common foundations.
- **Correctness exit gate:** byte and integer handling is endian-explicit and bounds-checked; ownership/error contracts are documented; public headers are self-contained; untrusted input helpers cannot invoke undefined behavior.
- **Testing exit gate:** clean supported builds; unit/property tests; formatting/static analysis; AddressSanitizer, UndefinedBehaviorSanitizer, and applicable ThreadSanitizer smoke tests; compiler/configuration matrix passes.
- **Measurement exit gate:** record build time, binary size, and microbenchmark harness overhead only as reproducible baselines; no database performance claim.

## Phase 2 — WAL record codec

- **Implementation status:** the WAL v1 physical directory/segment/record format, integrity scopes,
  limits, application envelope, and compatibility policy are accepted. The pure in-memory segment
  header, record header, and complete-record codec plus identities, physical positions, checked
  layout calculations, golden fixtures, tests, and fuzz target are implemented. Every kind-specific
  logical application body remains unimplemented.
- **Scope:** complete the kind-specific logical operation payload specification and implement the
  accepted versioned, checksummed WAL framing and typed payloads using fixed-width encodings.
- **Explicit non-scope:** segment files, sync/acknowledgment policy, recovery across segments, mutable-head application, Raft, and compression unless justified by the record specification.
- **Required artifacts:** the accepted WAL v1/ADR/learning documents; accepted kind-specific
  application payload specification; encoder/decoder; golden fixtures; fuzz targets; and executable
  corruption/error taxonomy.
- **Correctness exit gate:** a decoder validates framing and integrity before unsafe allocation/access, rejects unsupported versions/flags, round-trips all valid records, and never serializes native object representation; invariant 10 and 14 obligations for records are met.
- **Testing exit gate:** unit, property, golden, truncation, bit-flip, hostile-length, cross-endian fixture, and coverage-guided fuzz tests pass under sanitizers.
- **Measurement exit gate:** publish reproducible codec throughput, latency, allocation, and size baselines across representative record/batch sizes with checksum cost isolated; correctness checks remain enabled.

## Phase 3 — Segmented WAL and recovery

- **Implementation status:** segment naming/lifecycle, install and synchronization order,
  acknowledgment eligibility, recovery classification, explicit tail repair, semantic preflight, and
  replay ordering are accepted and implemented at the physical layer. The reusable POSIX
  operations, process lock, creation and existing-history owners, segment installation,
  append/sync frontiers, terminal failure state, rotation, discovery, whole-log verification,
  explicit repair, preflight/replay, reopen, and inspection tool have deterministic tests and
  syscall injection where mutation occurs. Bounded concurrent admission, one-worker ordering,
  `ASYNC`/`LOCAL_SYNC` completion, group commit, graceful drain, terminal propagation, and coordinator
  metric snapshots are implemented with deterministic tests. The crash-image/process-kill harness,
  application-kind semantics, and server-wide operational metrics/export path do not exist.
- **Scope:** implement WAL v1 segment lifecycle, append/grouping, explicitly named durability modes,
  acknowledgment boundaries, rotation, torn-tail handling, ordered replay, and idempotent
  single-node recovery.
- **Explicit non-scope:** CSEG/head state, checkpoint format and old-segment removal, replication,
  multiplexed multi-Raft storage, and asynchronous durability modes without precise loss contracts.
- **Required artifacts:** the accepted WAL storage/recovery specification and ADR; append/replay APIs;
  fault-injecting storage boundary; crash harness; operational diagnostics; and implementation
  evidence mapped to the WAL v1 test contract.
- **Correctness exit gate:** acknowledged writes satisfy their named crash envelope; log order is stable; corruption fails closed at a known boundary; repeated recovery is idempotent; no required segment is reclaimed.
- **Testing exit gate:** deterministic fault injection for short writes, sync errors, rotation, truncation, corruption, process kill, and recovery-during-recovery; model comparison and sanitizer/fuzz suites pass.
- **Measurement exit gate:** characterize append/ack latency distributions and throughput by durability mode, batch size, group-commit setting, and storage device; record sync counts and recovery time separately.

## Phase 4 — Columnar batches and mutable heads

- **Scope:** typed immutable input batches; null/variable-width representation; append-only tablet heads; sealing; single shard-worker ownership; stable reader boundaries; idempotent ordered replay into heads.
- **Explicit non-scope:** durable columnar parts, SQL execution, secondary indexes, general lock-free containers, live subscriptions, and a universal allocator.
- **Required artifacts:** batch/head specifications; ownership and memory-ordering design; shard-affinity contract; append/scan APIs; reference model; tests and learning document.
- **Correctness exit gate:** rows become visible only when all columns are initialized; readers retain valid storage; committed replay order and batch identities are preserved; sealing cannot lose or duplicate rows.
- **Testing exit gate:** property/differential tests across types and nulls; forced allocation failures; deterministic reader/writer interleavings; ThreadSanitizer plus address/undefined sanitizers; replay and retry tests pass.
- **Measurement exit gate:** measure append/scan rate, allocation count, memory overhead, seal cost, and cache behavior across batch widths, variable-length distributions, and reader concurrency.

## Phase 5 — CSEG v1

- **Scope:** specify and implement immutable sorted CSEG parts, granules, checksummed column pages, metadata, supported encodings/compressors, safe readers/writers, and inspection tooling.
- **Explicit non-scope:** manifests, flush orchestration, compaction, remote objects, format v2 speculation, and indexes beyond metadata required by v1.
- **Required artifacts:** normative CSEG v1 specification; format ADRs; golden/corrupt fixtures; reader, writer, validator, and inspector; compatibility policy; fuzz targets; learning document.
- **Correctness exit gate:** every page and interpretation-critical metadata region has integrity coverage; parsing is bounds-safe; installed bytes are immutable; sort/type/null semantics round-trip exactly; unsupported encodings fail clearly.
- **Testing exit gate:** golden, property, round-trip, cross-version rejection, truncation/splice/bit-flip, decompression-bomb limit, fuzz, and sanitizer tests pass across all encodings.
- **Measurement exit gate:** report size, encode/decode throughput, selective-read cost, allocations, and compression tradeoffs on declared datasets without choosing defaults solely from one workload.

## Phase 6 — Manifest, flush, and checkpointing

- **Scope:** manifest generations/version edits; atomic durable part installation; sealed-head flush; checkpoint/log coverage; startup reconciliation; safe temporary-file handling.
- **Explicit non-scope:** compaction, delta parts, distributed metadata, object storage, and aggressive garbage collection beyond proven safe ownership.
- **Required artifacts:** installation/manifest/checkpoint specifications and ADRs; flush coordinator; recovery state machine; fault-injection matrix; observability; learning document.
- **Correctness exit gate:** a manifest references only complete durable parts; recovery yields the old or new complete state at every crash point; checkpointing cannot discard uncovered WAL; flush/recovery are idempotent; active readers keep storage alive.
- **Testing exit gate:** crash after every write/sync/rename/edit/checkpoint action; missing/corrupt/orphan file cases; repeated recovery; concurrent snapshot/flush tests; sanitizer and filesystem fault tests pass.
- **Measurement exit gate:** measure flush throughput, foreground interference, manifest growth, startup/replay time, sync amplification, and temporary/durable space amplification.

## Phase 7 — Sparse indexes, out-of-order delta parts, and compaction

- **Scope:** zone maps, sparse indexes, optional scoped secondary indexes; delta parts for late/out-of-order versions; selection and merge policy; atomic compaction installation; safe reclamation.
- **Explicit non-scope:** indexes required for correctness, arbitrary in-place updates, distributed compaction, object tiering, and undocumented history loss.
- **Required artifacts:** index and delta specifications; compaction/version-resolution ADRs; planner/executor; reference merger; retention and reclamation contract; learning document.
- **Correctness exit gate:** pruning has no false negatives; compaction preserves visible rows and system-time versions under retention rules; immutable inputs never change; reclamation waits for readers.
- **Testing exit gate:** generated overlapping ranges, duplicates, corrections, tombstones if adopted, boundary values, all snapshot positions, crash installation, concurrent scans, and index corruption are differentially checked against uncompacted scans.
- **Measurement exit gate:** quantify pruning effectiveness, read/write/space amplification, compaction debt, foreground tail latency, late-data sensitivity, and index build/storage cost under declared skew.

## Phase 8 — SQL parser, binder, and scalar reference engine

- **Scope:** specify a typed analytical SQL subset; custom lexer/parser; catalog binding; scalar expression/relational reference execution; event-time and system-time query syntax required by initial workloads.
- **Explicit non-scope:** full SQL compliance, vectorization, cost-based optimization, distributed SQL, unsupported mutation syntax, and streaming syntax beyond contracts scheduled later.
- **Required artifacts:** grammar and semantic specification; type/null/decimal/temporal rules; AST and bound-plan interfaces; scalar engine; diagnostic catalog; golden and differential tests; learning document.
- **Correctness exit gate:** binding is schema-version stable; invalid/ambiguous queries fail deterministically; scalar results match specified null, numeric, temporal, ASOF, aggregation, and snapshot semantics.
- **Testing exit gate:** parser/AST fuzzing, grammar goldens, type-error cases, metamorphic and reference-model comparisons, random small databases, schema-change races, and sanitizers pass.
- **Measurement exit gate:** track parse/bind/reference execution baselines and memory limits to detect pathologies; scalar speed is not a product performance target.

## Phase 9 — Vectorized execution and parallel scheduling

- **Scope:** bounded vectors, vectorized scans/expressions/aggregates/joins, physical planning, memory accounting, cancellation, parallel scheduling, and spill for explicitly supported operators.
- **Explicit non-scope:** distributed fragments, GPU novelty, unbounded query memory, or optimizer rules lacking semantic/differential validation.
- **Required artifacts:** vector/physical-operator contracts; scheduler and memory ADRs; cost/selection rules; profiles; benchmark suites; operator learning documents.
- **Correctness exit gate:** all supported physical plans match the scalar engine for values, errors, ordering guarantees, snapshots, and system-time visibility; cancellation and failures release resources and pins.
- **Testing exit gate:** randomized plan differential tests, forced batch boundaries/spills/allocation failures, scheduler interleavings, race/sanitizer runs, and resource-limit tests pass.
- **Measurement exit gate:** publish reproducible operator and end-to-end profiles with CPU, memory, I/O, batch width, parallelism, skew, spill, and tail latency; optimizations cite evidence.

## Phase 10 — epoll server and native protocol

- **Scope:** versioned native protocol; framing, handshake, errors, ingest/query request lifecycle; nonblocking epoll reactors; bounded connection/queue admission; reactor-to-shard SPSC routing; authentication/TLS integration boundary.
- **Explicit non-scope:** thread-per-connection, custom TLS/cryptography, `io_uring`, unbounded frames/queues, distributed routing, and protocol claims beyond tested versions.
- **Required artifacts:** protocol v1 and state-machine specifications; epoll design/ADRs; server/client test implementation; limits and backpressure policy; packet fixtures/fuzzers; operations and learning documents.
- **Correctness exit gate:** malformed peers cannot trigger undefined behavior or unbounded allocation; disconnect/cancel semantics are deterministic; acknowledgment names durability mode; reactor ownership and queue memory ordering are proven; overload is explicit.
- **Testing exit gate:** protocol fuzzing, partial read/write, slowloris, connection churn, queue saturation, shard stall, cancellation, TLS-boundary tests if enabled, and sanitizers pass.
- **Measurement exit gate:** characterize connections, batch/request latency, throughput, CPU, allocations, fairness, and overload behavior across frame and queue sizes on declared Linux configurations.

## Phase 11 — Subscriptions and incremental materialized views

- **Scope:** committed change model; gap-free snapshot-to-stream handoff; deterministic versioned resume tokens; bounded subscriber policies; supported incremental operators; materialized-view progress/recovery and late-event corrections.
- **Explicit non-scope:** unqualified end-to-end exactly-once claims, unlimited retention, every SQL operator, cross-cluster delivery, and external-sink transactions not explicitly integrated.
- **Required artifacts:** subscription/delivery protocol; resume-token and retention specifications; handoff ADR; watermark/lateness and view-correction contracts; incremental engine; recovery metadata; learning documents.
- **Correctness exit gate:** snapshot plus continuation omits no post-boundary committed change; resume yields the defined suffix or precise expiry/incompatibility error; views equal full recomputation; slow subscribers cannot indefinitely block ingestion.
- **Testing exit gate:** commits injected at every handoff step, disconnect/retry/restart, duplicate delivery, token tamper, retention expiry, late corrections, subscriber fan-out, stalled consumers, and differential recomputation pass.
- **Measurement exit gate:** measure handoff delay, update latency, operator state, replay/recovery cost, fan-out scaling, memory bounds, and ingestion impact under lateness and slow-consumer distributions.

## Phase 12 — Performance engineering and io_uring comparison

- **Scope:** profile verified single-node paths; remove measured bottlenecks; establish reproducible benchmark governance; compare an optional `io_uring` prototype with epoll under equal semantics.
- **Explicit non-scope:** weakened durability/checksums/visibility, selective publication of favorable runs, a mandatory `io_uring` migration, distribution, and novel allocators or lock-free rewrites without evidence.
- **Required artifacts:** benchmark specification/harness/datasets; hardware and run manifests; profiles; regression thresholds; optimization ADRs where architecture changes; epoll/`io_uring` comparison report.
- **Correctness exit gate:** optimized and reference paths pass identical invariant, crash, corruption, and differential suites; all benchmark modes name their durability and consistency guarantees.
- **Testing exit gate:** reproducibility across repeated clean runs; performance regression tests with noise policy; sanitizer/fault suites remain green; `io_uring` failure/cancel paths receive parity testing if retained.
- **Measurement exit gate:** report distributions and resources, not a single peak; retain unfavorable results; adopt `io_uring` only if an important tested workload shows durable end-to-end benefit that justifies complexity.

## Phase 13 — System-time history

- **Scope:** formal bitemporal row-version model; SQL system-time clauses; history retention; correction/cancellation semantics; compaction and index support; audit visibility.
- **Explicit non-scope:** general distributed transactions, legal/compliance certification, retroactive mutation of immutable history, and distribution before the model is validated locally.
- **Required artifacts:** temporal model and SQL specification; retention/GC ADR; storage and query changes; migration plan; bitemporal oracle; learning document.
- **Correctness exit gate:** event time and commit system time remain distinct; every supported as-of query returns the model's version; compaction/retention never remove a still-promised version; corrections are auditable.
- **Testing exit gate:** generated multi-version histories, ties/boundaries, late corrections, restarts, compaction, retention pins, and scalar/vector differential queries pass against the bitemporal model.
- **Measurement exit gate:** quantify history space/write amplification, as-of scan cost, compaction overhead, and retention-policy sensitivity on declared version distributions.

## Phase 14 — Deterministic Raft

- **Scope:** implement a deterministic Raft core for one logical group: elections, replication, commit, membership protocol as scoped by ADR, snapshots, read consistency mechanisms, and simulated transport/storage/time.
- **Explicit non-scope:** multi-group multiplexing, production network integration, distributed queries, hidden third-party Raft implementation, and serving uncommitted or merely appended entries.
- **Required artifacts:** protocol/state-machine specification and ADRs; deterministic core; simulator/model checker harness; persistent-state formats; snapshot/install contract; safety/liveness test corpus; learning document.
- **Correctness exit gate:** election safety, log matching, leader completeness, committed-entry durability, deterministic apply, membership safety, and read contracts hold; uncommitted entries are never visible.
- **Testing exit gate:** exhaustive bounded schedules where feasible; randomized long simulations with partitions, delay, duplication, crashes, disk faults, clock changes, snapshots, and membership; trace shrinking and reference/model comparison pass.
- **Measurement exit gate:** measure simulation coverage/rate, message and fsync cost, commit latency, recovery/catch-up, and snapshot transfer in stated topologies; performance cannot override safety.

## Phase 15 — Multi-Raft tablets

- **Scope:** map tablets to Raft groups; multiplex logical records over physical logs, threads, timers, and connections; lifecycle, placement, snapshot transfer, fairness, and safe per-group reclamation.
- **Explicit non-scope:** globally ordered logs, cross-tablet atomic transactions, distributed query execution, automatic rebalancing beyond scoped placement mechanics, and conflating physical offsets with logical indexes.
- **Required artifacts:** tablet/multi-Raft architecture and log format; scheduling/fairness/reclamation ADRs; placement metadata; production integration; deterministic cluster harness; operations and learning documents.
- **Correctness exit gate:** each tablet preserves independent ordered commit and identity through crash/leadership changes; multiplexed storage cannot cross-apply or prematurely reclaim another group's entries; snapshots and resume positions remain unambiguous.
- **Testing exit gate:** thousands of simulated groups, hot/cold skew, group creation/deletion, physical-log corruption, node loss, snapshot/install, leadership churn, starvation, and recovery are checked against per-group models.
- **Measurement exit gate:** characterize groups per node, scheduling fairness, physical-log amplification, commit tails, catch-up, snapshot bandwidth, memory, and noisy-neighbor behavior.

## Phase 16 — Distributed query execution and rebalancing

- **Scope:** distributed planning/fragments/exchanges; compatible multi-tablet snapshot acquisition; explicit linearizable and bounded-stale reads; tablet movement, routing epochs, and failure retry.
- **Explicit non-scope:** general cross-tablet write transactions, silent consistency downgrade, unlimited shuffle, and topology changes that invalidate tokens without an explicit error/mapping protocol.
- **Required artifacts:** distributed query and consistency specifications; exchange protocol; snapshot-coordination and rebalancing ADRs; planner/scheduler; fault recovery; observability and learning documents.
- **Correctness exit gate:** results correspond to the declared distributed snapshot/consistency level; retries do not duplicate result fragments beyond contract; rebalancing preserves committed data, identities, retention pins, and query/subscription boundaries.
- **Testing exit gate:** differential single-node/distributed queries; partitions, node/leader loss, skew, exchange duplication/loss, movement at each state, stale routing, cancellation, and deterministic fault simulations pass.
- **Measurement exit gate:** measure scale-out efficiency, exchange bytes, coordination latency, skew/straggler impact, movement duration, foreground interference, and consistency-level costs in declared topologies.

## Phase 17 — Object-storage tiering and interoperability

- **Scope:** immutable-part upload/install/cache/eviction; remote integrity and retry; authoritative manifest references; safe remote deletion; selected documented import/export or ecosystem formats.
- **Explicit non-scope:** treating bucket listings as metadata truth, mutating remote parts in place, claiming object storage has local-disk latency, custom cloud APIs when standard clients suffice, and compatibility claims without fixtures.
- **Required artifacts:** tiering lifecycle and failure specification; object identity/cache/deletion ADRs; manifest extensions; backend abstraction and supported implementation; interoperability schemas/fixtures; operations and learning documents.
- **Correctness exit gate:** local/remote transitions retain identical logical bytes and snapshot visibility; retries are idempotent; cache eviction and remote deletion respect all references; partial uploads are never installed.
- **Testing exit gate:** eventual-listing behavior, timeout, retry, corruption, partial/multipart upload, credential failure, cache loss, concurrent queries/compaction, restore, and import/export round trips pass with fault-injected backends.
- **Measurement exit gate:** report upload/download, cache-hit, scan, restore, request-cost, egress, and foreground-impact profiles by object size and access distribution.

## Phase 18 — Release hardening

- **Scope:** close supported-feature correctness gaps; upgrades and compatibility; security review; operations, backup/restore, observability, packaging; long-duration reliability; reproducible release and benchmark evidence.
- **Explicit non-scope:** last-minute feature expansion, unsupported-platform parity claims, unresolved data-loss risks, or relabeling pre-alpha components as production-ready based only on elapsed time.
- **Required artifacts:** release criteria and support matrix; threat model; upgrade/rollback and backup/restore procedures; format/protocol compatibility matrix; runbooks; SBOM/dependency review; fuzz/crash/soak reports; published benchmark methodology and known limitations.
- **Correctness exit gate:** no unresolved severity-one correctness/recoverability issue; supported upgrades and rollback boundaries preserve data; backup/restore meets its contract; all applicable invariants have traceable automated evidence.
- **Testing exit gate:** full CI/sanitizer/static-analysis suites; sustained fuzzing; crash, corruption, deterministic cluster, upgrade, rollback, backup, restore, network fault, and long-duration soak tests pass on the declared support matrix.
- **Measurement exit gate:** release candidates meet predeclared regression budgets for representative workloads and publish reproducible latency/throughput/resource results with durability and consistency modes; limitations and variance are included.
