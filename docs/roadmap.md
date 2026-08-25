# Roadmap and Phase Gates

ChronosDB has implemented correctness-first subsystem slices through the Phase 8 scalar SQL oracle
and the accepted Phase 9 vectorized query, resource-control, physical-planning, spill, and bounded
scheduling boundary.
That statement does not declare every Phase 1–8 exit gate complete: each section below records its
remaining implementation, integration, measurement, or platform evidence. Phase 1A/1B provide the
build/tooling and portable binary foundations but the broader Phase 1 utility surface remains
partial. Phases 2–3 provide the accepted WAL v1 codec, segmented writer, bounded commit coordinator,
locked recovery/reopen path, inspector, crash harness, and checkpoint-aware reclamation. Phase 4
provides schemas, canonical columnar ingestion bytes, bounded mutable heads/tablet publication, and
the single-tablet execution/recovery path. Phase 5 provides the complete CSEG v1 in-memory and
inspection surface. Phase 6 provides Manifest v1 installation, flush/checkpoint coordination,
aggregate publication, startup recovery, and WAL-prefix reclamation around a caller-supplied retained
catalog. Phase 7 provides the accepted append-only pruning, delta-planning, compaction, publication,
and pin-aware part-reclamation boundary. Phase 8 provides the pure in-memory parser, binder, and
scalar reference engine behind an abstract snapshot provider; it is not a production storage adapter
or vector engine. Local verification is recorded per change, while the declared Linux compiler/CI
matrix remains the portability reference and reviewed hardware/device campaigns remain separate
evidence.
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
  a bounded structured JSON diagnostic encoder/writer, the common nonnil system UUID generator,
  and a learning document. The minimal durable POSIX file/directory layer adds explicit-offset
  transfer loops, synchronization, no-replace rename, and advisory locking with deterministic
  syscall injection. A typed injectable wall/monotonic time source now replaces the first ad hoc
  subsystem clock seam and supplies default diagnostic timestamps. The OS-backed UUID generator
  now has a typed injectable entropy boundary with bounded nil/error tests and direct qualification
  of the production Linux partial-read/error completion loop; broader durable identity
  allocation/collision policy, logging sinks and rotation/collection, and broader test utilities
  remain incomplete. The phase exit gates below remain unchanged and have not been declared
  complete. A clean-revision local Apple arm64 baseline now records the common benchmark target's
  build time, artifact sizes, and harness-iteration proxy; Linux x86-64 and cross-toolchain
  repetition remain separate evidence.

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
  layout calculations, golden fixtures, tests, and fuzz target are implemented. The generic
  application-envelope API and first kind-specific logical body codec are implemented independently
  of physical WAL submission.
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
  metric snapshots are implemented with deterministic tests. The subprocess crash harness exercises
  real host files across installation, append/sync, grouped acknowledgment, rotation, corruption,
  repair, reopen, and locking. The first application-kind byte semantics and an already-routed live
  single-tablet submission/publication path are implemented. Retained-lineage recovery application
  for that first kind is implemented. The server-wide operational metrics/export path does not
  exist.
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

- **Design status:** logical types and stable identities, immutable schema versions and initial
  evolution, columnar-batch v1 bytes, the first WAL append command, ordered replay/retry semantics,
  mutable-head publication, snapshots, and sealing/handoff are accepted specifications. The
  `chronos_schema` identity/type/immutable-schema/lineage/projection foundation and the
  `chronos_columnar` borrowed/owned immutable vector and schema-shaped batch foundation plus the
  pure in-memory Columnar Batch v1 codec and `chronos_ingest` `COLUMNAR_APPEND` v1 command/digest
  codec are implemented with golden, property, corruption, fuzz, benchmark, sanitizer,
  installation, and external-consumer coverage. A bounded correctness-first live retry reservation
  directory adds deterministic model and concurrency tests. The `chronos_head` single-generation
  primitive adds fixed capacity, pre-WAL reservation, batch-atomic publication, owning snapshots,
  hidden row-version identity, sealing, concurrency/property tests, and focused microbenchmarks.
  The bounded tablet owner adds a live tablet retry table, whole-batch generation rotation,
  sealed-generation backpressure, and one outer generation/rows/position/retry publication with
  deterministic concurrency tests and focused microbenchmarks. The blocking single-tablet executor
  composes canonical command encoding, global retry reservation, bounded WAL admission, exact
  durability completion, tablet publication, and global outcome commit; integration tests cover
  both durability modes, retry outcomes, admission backpressure, and accepted-WAL failure, and a
  real-filesystem microbenchmark keeps the full execution work enabled. The retained-lineage
  recovery owner composes whole-WAL preflight/replay with fresh global retry and tablet state,
  schema-bound generation switching, exact ancestor retry no-ops, conflict failure, deterministic
  repeat recovery, and continued writer sequencing. It has real-WAL integration, hostile semantic
  classification, installation/external-consumer coverage, and unique/retry-heavy recovery
  microbenchmarks. Tablet preparation additionally enforces intra-batch and visible-generation
  APPEND_ROWS logical-key uniqueness with exhaustive typed, replay, property, and benchmark
  evidence. Deterministic allocation-failure sweeps cover retry reservation, mutable-head and tablet
  preparation, deduplication work, and rotation rollback, while the expected post-WAL in-memory
  publication path observes zero allocations. Mutable-head microbenchmarks now cover publication,
  checked borrowed scans, sealing, retained-memory counters, and zero-/8-/64-byte string values
  across 64, 1,024, and 65,536 rows. Benchmark-only scoped instrumentation reports regular
  allocation calls and requested bytes without including paused arena construction. Coordinated
  1-/2-/4-reader cases scan the same pinned buffers at the two larger row counts and two string
  widths. The executor matrix additionally covers correctness-guarded steady matching retries at
  64, 1,024, and 65,536 rows without a second WAL result, plus real-WAL 50/50 and 10/90
  first-attempt/retry operation ratios at 64 and 1,024 rows. Retry retention and routing/admission
  remain unimplemented; bounded flush scheduling/publication is now implemented in Phase 6.
  Dedicated retry-directory cases cover matching lookup at
  64, 4,096, and 65,536 committed entries plus 1-/2-/4-thread contention on the two larger
  populations, with construction allocation-call/requested-byte counters. Hardware cache profiles,
  true retained allocator/RSS measurement, and the wider end-to-end Phase 4 benchmark matrix remain
  unimplemented.

- **Scope:** typed immutable input batches; null/variable-width representation; append-only tablet heads; sealing; single shard-worker ownership; stable reader boundaries; idempotent ordered replay into heads.
- **Explicit non-scope:** durable columnar parts, SQL execution, secondary indexes, general lock-free containers, live subscriptions, and a universal allocator.
- **Required artifacts:** batch/head specifications; ownership and memory-ordering design; shard-affinity contract; append/scan APIs; reference model; tests and learning document.
- **Correctness exit gate:** rows become visible only when all columns are initialized; readers retain valid storage; committed replay order and batch identities are preserved; sealing cannot lose or duplicate rows.
- **Testing exit gate:** property/differential tests across types and nulls; forced allocation failures; deterministic reader/writer interleavings; ThreadSanitizer plus address/undefined sanitizers; replay and retry tests pass.
- **Measurement exit gate:** measure append/scan rate, allocation count, memory overhead, seal cost, and cache behavior across batch widths, variable-length distributions, and reader concurrency.

## Phase 5 — CSEG v1

> **Current status:** the normative v1 byte contract and format decision are accepted. The focused
> `chronos_cseg` target now provides authoritative constants, nominal `PartId`, allocation-free
> checked metadata/page layout planning, boundary/property tests, self-contained public headers,
> bounded deterministic raw/Zstandard page compression, checked metadata encoding, allocation-
> bounded borrowed metadata decoding, explicit outcomes, exact schema binding, hostile integrity/
> registry/cross-field tests, an independent metadata golden, metadata decoder fuzzing and
> microbenchmarks, and package install/export coverage. The shared identity-free physical-column
> validator plus deterministic PLAIN payload encoding and borrowed schema-independent decoding add
> all-type/property/corruption tests, decoder fuzzing, encode/decode microbenchmarks, and installed-
> consumer coverage. Stored-page composition now adds deterministic descriptor-ready encoding,
> CRC-before-provider decoding, allocation-free raw views, owned bounded Zstandard output, golden/
> corruption/property tests, fuzzing, microbenchmarks, and installed-consumer coverage. Canonical
> owned part composition and borrowed prefix/exact structural decoding now validate every page and
> alignment byte, with complete-file golden/corruption/property tests, fuzzing, microbenchmarks,
> and installed-consumer coverage. Bounded full validation now covers system-row semantics, exact
> event-time extrema, strict cross-granule ordering for every logical type including null/IEEE
> edge cases, and exact schema/tablet binding with hostile/property/sanitizer evidence.
> Metadata-authenticated projected granule reading now validates only requested user pages plus all
> system pages, supports nullable-tail lineage projection, and has hostile/property/fuzz/benchmark
> and installed-consumer coverage. Complete read-only inspection, deterministic descriptor-only CLI
> output, hostile/subprocess tests, installation/external-consumer coverage, and the CSEG learning
> document now complete the required Phase 5 implementation artifacts. Phase-gate evidence remains
> subject to the declared CI platform and benchmark methodology rather than this status summary.

- **Scope:** specify and implement immutable sorted CSEG parts, granules, checksummed column pages, metadata, supported encodings/compressors, safe readers/writers, and inspection tooling.
- **Explicit non-scope:** manifests, flush orchestration, compaction, remote objects, format v2 speculation, and indexes beyond metadata required by v1.
- **Required artifacts:** normative CSEG v1 specification; format ADRs; golden/corrupt fixtures; reader, writer, validator, and inspector; compatibility policy; fuzz targets; learning document.
- **Correctness exit gate:** every page and interpretation-critical metadata region has integrity coverage; parsing is bounds-safe; installed bytes are immutable; sort/type/null semantics round-trip exactly; unsupported encodings fail clearly.
- **Testing exit gate:** golden, property, round-trip, cross-version rejection, truncation/splice/bit-flip, decompression-bomb limit, fuzz, and sanitizer tests pass across all encodings.
- **Measurement exit gate:** report size, encode/decode throughput, selective-read cost, allocations, and compression tradeoffs on declared datasets without choosing defaults solely from one workload.

## Phase 6 — Manifest, flush, and checkpointing

> **Current status:** [Manifest v1](formats/manifest-v1.md),
> [ADR 0017](adr/0017-manifest-generations-installation-and-checkpoints.md), and the
> [installation/recovery architecture](architecture/manifest-installation-and-checkpointing.md) are
> accepted. They freeze immutable full-generation bytes and names, per-tablet/retry recovery state,
> independently durable part/manifest installation, atomic head replacement, and checkpoint-aware
> WAL suffix/reclamation ordering. The `chronos_manifest` target now provides the nominal database
> identity, authoritative constants, descriptor value model, checked canonical layout planner, and
> pure owned-encode/borrowed-decode Manifest v1 codec with hostile/property/fuzz/benchmark evidence.
> Exact retained-catalog binding and the Phase 6 add-only generation-transition validator are also
> implemented. Exact final/temporary basename formatting and parsing are implemented with
> property, installation, and external-consumer coverage. Installed CSEG images now have complete
> in-memory filename, length, header, content, schema, WAL-identity, and record-extrema binding.
> A locked, descriptor-relative filesystem owner now installs immutable CSEG parts with exact
> prevalidation/readback, file-sync, no-replace rename, directory-sync, poisoning, and metrics.
> It also strictly reconciles both locked namespaces, rejects malformed/nonregular entries and
> generation gaps, retains orphan finals, and durably removes only recognized temporaries.
> Exact next-generation installation now revalidates the selected predecessor, add-only/catalog
> transition, and every referenced final CSEG before canonical readback, file sync, no-replace
> rename, directory sync, and durability-boundary metrics. Read-only recovery selection now owns
> and validates only the highest generation, exact
> database/WAL/catalog context, every referenced final CSEG, and reports orphan/temporary entries
> without mutation. WAL recovery now accepts that external checkpoint context, verifies present
> covered headers and the exact coordinate, tolerates only covered prefix gaps, and preflights and
> replays the complete required suffix without changing WAL v1. Mutable recovery can apply the same
> proof for authorized tail repair, temporary cleanup, startup barriers, and exact writer reopening.
> The live writer now fully validates and synchronously removes only checkpoint-covered closed
> segments, preserves the active segment, poisons on cleanup failure, and exposes durability-boundary
> metrics. A pure sealed-head converter now preflights one pinned generation, applies the exact CSEG
> physical ordering through the validator's shared comparator, plans canonical granules, materializes
> every user/system page, exact-decodes and fully validates its output, and returns an install-ready
> descriptor, WAL identity, and immutable image. Its deterministic/golden/boundary tests include the
> real durable part-installation path, and flush-specific raw/Zstandard benchmarks are registered.
> A pure checked generation builder now validates that image against the retained schema, derives
> exact per-record row counts from CSEG system pages, requires matching retry outcomes, preserves
> all predecessor state and the reclaim checkpoint, inserts the new tablet/part/retries canonically,
> and self-validates the encoded add-only transition. Golden/property/hostile tests include the real
> part-then-manifest installation and recovery-selection path, with builder microbenchmarks and
> installed-consumer coverage. A read-only checkpoint builder now revalidates the candidate and all
> referenced parts, integrity/preflight scans the WAL suffix, proves exact first-applied user/system
> rows and protected retry outcomes, recognizes zero-row exact duplicates, and advances only the
> longest globally consecutive covered prefix. Hostile multi-tablet-gap, truncation, unsupported,
> digest/content/boundary, deterministic-property, benchmark, sanitizer, and installed-consumer
> coverage exercise that boundary. One aggregate release/acquire publication owner now retains the
> exact selected Manifest and live head pins, refreshes monotonic tablet epochs, and substitutes
> exact newly selected parts for their covered sealed heads with deterministic interleaving,
> lifetime, hostile, sanitizer, benchmark, and installed-consumer evidence. Successful replacement
> now issues a non-forgeable exact receipt; idempotent TabletState consumption release-publishes a
> smaller sealed set and releases its rotation backpressure while old snapshots retain their pins.
> A fixed-capacity MPSC/single-consumer handoff now reserves before topology mutation, owns exact
> immutable pins, preserves reservation order and retry age, rejects rotation before WAL when full,
> and releases capacity only for an exact post-publication receipt. A single-threaded durable flush
> coordinator now composes queue acquisition, deterministic conversion, part and Manifest
> installation, restart-style durable-generation resume, aggregate replacement, TabletState
> retirement, and receipt-gated completion with fail-closed post-Manifest handling and metrics.
> A subprocess SIGKILL matrix now stops after every part/Manifest write, file sync, rename, and
> directory sync, then proves recognized-temporary cleanup, orphan retention, complete old-or-new
> selection, and byte-identical repeated recovery. Columnar WAL recovery can now restore exact
> Manifest-derived tablet/retry/schema boundaries, verify covered suffix commands as no-ops, apply
> only uncovered rows, and reopen at the verified global end; the owning Manifest startup
> composition now selects/validates durable state, derives those seeds, cleans recognized
> temporaries, and returns one aggregate publication while retaining Manifest-before-WAL lock
> ownership. Its explicit startup policy can revalidate and synchronously remove only closed WAL
> segments covered by the selected checkpoint, converging when cleanup is repeated. The process
> crash matrix now kills after every covered-prefix unlink and the following WAL-directory sync,
> then proves every surviving namespace subset reopens and converges. Persistent catalog
> reconstruction and service activation remain. An installed reproducible flush harness now
> preserves real CSEG/Manifest/WAL images and raw samples while measuring durable flush throughput,
> concurrent publication interference, Manifest growth, repeated startup/WAL replay, sync
> amplification, and temporary/durable space amplification under an executable correctness gate.
> Reviewed platform/device campaigns remain before declaring the measurement gate complete.

- **Scope:** manifest generations/version edits; atomic durable part installation; sealed-head flush; checkpoint/log coverage; startup reconciliation; safe temporary-file handling.
- **Explicit non-scope:** compaction, delta parts, distributed metadata, object storage, and aggressive garbage collection beyond proven safe ownership.
- **Required artifacts:** installation/manifest/checkpoint specifications and ADRs; flush coordinator; recovery state machine; fault-injection matrix; observability; learning document.
- **Correctness exit gate:** a manifest references only complete durable parts; recovery yields the old or new complete state at every crash point; checkpointing cannot discard uncovered WAL; flush/recovery are idempotent; active readers keep storage alive.
- **Testing exit gate:** crash after every write/sync/rename/edit/checkpoint action; missing/corrupt/orphan file cases; repeated recovery; concurrent snapshot/flush tests; sanitizer and filesystem fault tests pass.
- **Measurement exit gate:** measure flush throughput, foreground interference, manifest growth, startup/replay time, sync amplification, and temporary/durable space amplification.

## Phase 7 — Sparse indexes, out-of-order delta parts, and compaction

> **Status: first compaction decision accepted.** ADR 0018 freezes the initial append-only boundary:
> unchanged CSEG v1 rows, deterministic complete-tuple merge, independent full-row equivalence,
> fresh output identities, Manifest v1 atomic replacement, and conservative retention of inputs.
> A separate allocation-free Manifest transition validator now enforces the exact one-tablet
> replacement authorization while preserving checkpoint, tablet, retry, and unrelated-part state.
> An independent bounded streaming oracle now fully validates both CSEG sets, rejects duplicate
> cross-part physical tuples, and compares every user/system cell without trusting hashes or
> metadata totals. The bounded reference merger independently stable-sorts validated row references,
> emits one canonical fresh CSEG v1 part, and requires that oracle to accept its owned output before
> returning. The compaction-specific Manifest builder and storage authority now install the fresh
> output first, reread both final sets for another complete equivalence proof, and reuse the existing
> atomic Manifest directory-sync boundary while retaining every input final. A separate release/
> acquire publication path selects the complete durable replacement without changing live heads;
> held predecessor snapshots retain their exact Manifest bytes and input descriptors. The
> single-threaded coordinator now composes authoritative input reread, merge, both durable installs,
> reload, publication, and exact durable-successor resumption. A subprocess SIGKILL matrix covers
> every output/Manifest write, readback, sync, rename, directory sync, and publication boundary and
> proves equivalent old-or-new recovery with conservative input retention.
> ADR 0019 now accepts the remaining correctness boundary: authenticated CSEG event-time zone maps
> and granule sparse entries with scan fallback, rebuildable base/delta planning hints, deterministic
> bounded selection, and exact per-part-pin-gated final-part reclamation. The owned event-time
> part/granule pruning plan, deterministic no-false-negative oracle, rebuildable base/delta
> classification, resource-bounded overlap selection, move-only retirement records, per-part weak-pin gate,
> namespace revalidation, idempotent unlink/directory-sync path, controlled pin tests, process-crash
> matrix, and focused benchmarks are implemented. Durable secondary sidecars and old-Manifest floors
> stay deferred. Full workload benchmark campaigns and reviewed performance evidence remain before
> declaring the Phase 7 measurement gate complete.

- **Scope:** zone maps, sparse indexes, optional scoped secondary indexes; delta parts for late/out-of-order versions; selection and merge policy; atomic compaction installation; safe reclamation.
- **Explicit non-scope:** indexes required for correctness, arbitrary in-place updates, distributed compaction, object tiering, and undocumented history loss.
- **Required artifacts:** index and delta specifications; compaction/version-resolution ADRs; planner/executor; reference merger; retention and reclamation contract; learning document.
- **Correctness exit gate:** pruning has no false negatives; compaction preserves visible rows and system-time versions under retention rules; immutable inputs never change; reclamation waits for readers.
- **Testing exit gate:** generated overlapping ranges, duplicates, corrections, tombstones if adopted, boundary values, all snapshot positions, crash installation, concurrent scans, and index corruption are differentially checked against uncompacted scans.
- **Measurement exit gate:** quantify pruning effectiveness, read/write/space amplification, compaction debt, foreground tail latency, late-data sensitivity, and index build/storage cost under declared skew.

## Phase 8 — SQL parser, binder, and scalar reference engine

- **Implementation status:** complete for Phase 8. The focused `chronos_query` target and bounded
  SQL v1 lexer own normalized token text and exact byte/line/column spans,
  handles the specified quoting, comments, binary/numeric forms, operators, identifier folding,
  and reserved words, and reports stable lexical diagnostic codes under explicit input/token
  limits. The owned, bounded parser implements the complete read-only SELECT family, including
  expression precedence, typed literal/CAST shapes, system-time, LATEST BY, ASOF joins, grouping,
  ordering, limits, EXPLAIN, and SUBSCRIBE syntax. An immutable catalog snapshot now retains exact
  schema versions, canonical quoted/unquoted table identities, and a generation across live
  lineage changes. The SELECT binder pins that snapshot, resolves exact table/column identities,
  applies the v1 implicit-conversion and aggregate/grouping rules, expands stars deterministically,
  and records typed expression, projection, and ORDER BY alias bindings without executor-side name
  resolution. Canonical literal parsing now validates exact UTC nanoseconds, Gregorian dates,
  bounded nanosecond intervals, numeric ranges, and lowercase canonical UUIDs during binding. The
  scalar oracle now has an owned all-logical-type value model, checked copying from canonical
  physical cells, SQL NULL/NaN equality, and deterministic total ordering. Its bounded expression
  evaluator uses only bound identities and implements three-valued predicates, checked signed and
  unsigned arithmetic, IEEE floating behavior, explicit numeric/temporal casts, IN/BETWEEN,
  COALESCE, ABS, ASCII case mapping, epoch-aligned time buckets, aliases, and aggregate overrides.
  Its dependency-free exact decimal path uses checked widened intermediates for arithmetic,
  rescaling, division, remainder, unary operations, and explicit integer/IEEE conversions. Bound
  LATEST/ASOF plans
  now carry exact key/source/time identities and reject non-canonical temporal-join conditions. An
  immutable scalar snapshot/provider boundary validates exact schemas, logical/version identities,
  commit boundaries, types, and nullability before relational execution. The bounded relational
  oracle now executes system-time snapshot resolution, LATEST, ASOF/ASOF LEFT, WHERE, projection,
  deterministic ORDER BY tie-breaking, and LIMIT. Grouped and global COUNT, exact widened SUM,
  AVG, MIN/MAX, and population/sample variance implement the frozen NULL, NaN, empty-input, and
  final-overflow rules. The bounded owned statement AST/parser now covers the canonical CREATE
  TABLE policy clauses and finite multi-row INSERT VALUES surface. CREATE TABLE binding rejects
  invalid role, key, partition, nullability, and interval relationships, retains normalized policy
  durations, and materializes an initial `TableSchema` only from caller-allocated durable
  identities. INSERT binding pins the target schema, resolves explicit columns to schema ordinals,
  enforces lossless assignment and non-null/default rules under row/value limits, and materializes
  source-free constant expressions into complete schema-ordinal scalar rows. EXPLAIN now emits a
  versioned stable logical/scalar-physical plan description without snapshot access, while EXPLAIN
  ANALYZE executes once and reports measured scalar operator-work counters with the underlying
  result. SELECT/CREATE/INSERT parser and binder fuzz targets, deterministic expression/aggregate
  properties, and an independent random small-database LATEST/ASOF model close the Phase 8 test
  boundary. The query microbenchmarks track lexing, parsing, binding, expression/decimal evaluation,
  grouped reference execution, and INSERT materialization; scalar speed is not a product claim.

- **Scope:** specify a typed analytical SQL subset; custom lexer/parser; catalog binding; scalar expression/relational reference execution; event-time and system-time query syntax required by initial workloads.
- **Explicit non-scope:** full SQL compliance, vectorization, cost-based optimization, distributed SQL, unsupported mutation syntax, and streaming syntax beyond contracts scheduled later.
- **Required artifacts:** grammar and semantic specification; type/null/decimal/temporal rules; AST and bound-plan interfaces; scalar engine; diagnostic catalog; golden and differential tests; learning document.
- **Correctness exit gate:** binding is schema-version stable; invalid/ambiguous queries fail deterministically; scalar results match specified null, numeric, temporal, ASOF, aggregation, and snapshot semantics.
- **Testing exit gate:** parser/AST fuzzing, grammar goldens, type-error cases, metamorphic and reference-model comparisons, random small databases, schema-change races, and sanitizers pass.
- **Measurement exit gate:** track parse/bind/reference execution baselines and memory limits to detect pathologies; scalar speed is not a product performance target.

## Phase 9 — Vectorized execution and parallel scheduling

- **Implementation status:** ADR 0020 and the first implementation increment provide move-only
  identity-free canonical physical vectors, explicit strictly increasing selection vectors,
  caller-bounded vector chunks, exact logical/retained buffer accounting, selected-cell access,
  deterministic property tests, fuzzing, microbenchmarks, and installed-consumer coverage. ADR 0021
  and the second increment add one shared query-wide byte budget, exact move-only RAII reservations,
  monotonic peak accounting, idempotent cooperative cancellation, an explicit relaxed-atomic memory
  ordering argument, deterministic concurrency/property tests, microbenchmarks, and consumer
  coverage. ADR 0022 and the third increment add accounted chunk ownership, explicit pull/chunk/end
  steps, stable end and empty-chunk semantics, allocation-free SQL Boolean selection, failure-driven
  sibling cancellation, scalar-truth differential properties, fuzzing, and compaction benchmarks.
  The fourth increment adds allocation-free stable column-subset projection, zero-column
  cardinality preservation, bounded projection plans, deterministic cell-preservation properties,
  hostile/fuzz coverage, a projection microbenchmark, and installed-consumer linkage. These remain
  substrates rather than a plan engine. The fifth increment adds allocation-free UINT64 global
  LIMIT across arbitrary and empty chunk boundaries, partial-prefix output, eager release of
  unpulled upstream credit, scalar-prefix properties, fuzzing, a batched truncation benchmark, and
  installed-consumer linkage. ADR 0023 and the sixth increment add a bounded immutable unary
  physical pipeline plan, exact type/nullability propagation and runtime source enforcement,
  retained-configuration limits, composed fixed-seed scalar-model differential execution, hostile
  fuzzing, plan-overhead benchmarks, and installed-consumer linkage. ADR 0024 and the seventh
  increment add lifetime-pinned immutable chunk backings, uniform physical views, conservative
  backing/ordinal accounting, direct-versus-backed projection reclamation, coupled pin/credit
  lifetime, deterministic ownership properties, fuzzing, backing-attachment benchmarks, and
  installed-consumer linkage. ADR 0025 and the eighth increment add allocation-free CSEG
  projected-granule planning, exact raw-versus-owned decoded-byte requirements, mandatory system
  page accounting, borrowed plan lifetime, output allocation-failure classification, deterministic
  planned/direct properties, fuzzing, planning benchmarks, and installed-consumer linkage. ADR 0026
  and the ninth increment add a query-accounted single-part CSEG source, explicit immutable part
  pins, pre-open and pre-decode reservations, raw/decompressed backing lifetime, explicit
  granule-sized chunk bounds, source/LIMIT/cancellation cleanup, deterministic multi-granule
  properties, exhaustive allocation failure, scan fuzzing, scan microbenchmarks, and installed
  consumer linkage. ADR 0027 and the tenth increment add per-part lifetime identities carried
  across publication epochs, snapshot-bound predecessor part loading, exact database/WAL/generation
  provenance, conservative aggregate-pin accounting, a storage-validated CSEG scan adapter,
  reclamation lifetime regression coverage, and installed-consumer linkage. ADR 0028 and the
  eleventh increment add bounded canonical durable-part plans, Manifest-then-CSEG event-time
  pruning with no false negatives, selected-only snapshot image loading, query-accounted sequential
  multi-part composition, deterministic properties, hostile and allocation-failure coverage,
  pruning/selected-pull benchmarks, and installed-consumer linkage. ADR 0029 and the twelfth
  increment add a query-accounted source over one exact mutable-head publication, bounded canonical
  bitmap/offset materialization, schema-successor NULL synthesis, preserved caller projection
  order, deterministic snapshot/boundary properties, exhaustive allocation failure, fuzzing,
  materialization benchmarks, and installed-consumer linkage. ADR 0030 and the thirteenth increment
  add edge-safe open/closed `TIMESTAMP_NS` predicates, allocation-free exact selection compaction,
  NULL and empty-chunk semantics, query-accounted pull behavior, bounded physical-plan integration,
  deterministic scalar properties, hostile fuzz coverage, microbenchmarks, and installed-consumer
  linkage. ADR 0031 and the fourteenth increment automatically compose conservative Manifest/CSEG
  pruning with exact row truth in aggregate snapshot CSEG scans, retain and remove an unrequested
  event-time helper column, preserve caller projection order, enforce effective projection limits,
  and add deterministic/property/failure/fuzz/benchmark evidence. ADR 0032 and the fifteenth
  increment add an exact event-time mutable-head factory, projection-aware helper materialization
  and removal including zero-column output, chunk-boundary truth, effective-limit validation, and
  deterministic/property/failure/fuzz/benchmark/consumer evidence. ADR 0033 and the sixteenth
  increment add query-accounted owned source-column output materialization, arbitrary caller order
  and duplicates, sparse-to-identity compaction, exact canonical type/NULL preservation, checked
  pre-allocation limits, plan-shape integration, and deterministic/property/failure/fuzz/benchmark/
  consumer evidence. ADR 0034 and the seventeenth increment add mixed caller-ordered source and
  typed-constant output positions, canonical all-type and typed-NULL physical expansion without
  per-row scalar allocation, exact checked admission and plan shapes, and deterministic/property/
  failure/fuzz/benchmark/consumer evidence. ADR 0035 and the eighteenth increment add bounded
  immutable numeric/Boolean physical expression DAGs, exact type/nullability inference, checked
  integer/decimal and IEEE kernels, SQL NULL/NaN/short-circuit behavior, computed output positions,
  fixed-stack allocation-free successful row evaluation, plan/accounting integration, and deterministic/
  property/failure/fuzz/benchmark/consumer evidence. ADR 0036 and the nineteenth increment add
  exact single-source, nonaggregate bound-SELECT lowering for WHERE, ordered projection, stars,
  checked expressions, typed constants, BETWEEN/IN expansion, and LIMIT with explicit unsupported
  diagnostics and end-to-end vector execution evidence. The durable and mutable sources still
  expose user columns independently. ADR 0037 and the twentieth increment add exact checked
  fixed-width numeric/decimal/temporal CAST, lazy common-type COALESCE, and negative-safe
  `time_bucket` kernels with bound lowering, scalar differential, hostile/failure/fuzz/benchmark,
  and consumer evidence. ADR 0038 and the twenty-first increment add borrowed STRING/SYMBOL
  expression rows, exact two-pass canonical offset/value admission, direct ASCII LOWER/UPPER,
  text casts, lazy COALESCE, and failure/fuzz/benchmark/consumer evidence without per-row payload
  allocation. ADR 0039 and the twenty-second increment unify fixed and borrowed values in one
  bounded hybrid memo and add exact-type STRING/SYMBOL byte-order comparisons, NULL predicates,
  Boolean/BETWEEN/IN composition, and property/failure/fuzz/benchmark/consumer evidence without
  transformed-string allocation. ADR 0040 and the twenty-third increment add one fixed-state
  streaming global aggregate stage for COUNT/SUM/AVG/MIN/MAX/variance, exact scalar-oracle numeric,
  NULL, empty-input, and NaN semantics, checked plan shapes/configuration, cancellation and query-
  owned canonical output, plus property/failure/fuzz/benchmark/consumer evidence. Bound-SQL
  global aggregate lowering follows in ADR 0041 and the twenty-fourth increment with exact
  WHERE/input/aggregate/final-output/LIMIT order, direct and computed arguments, final aggregate
  expressions, empty-input semantics, binder-to-kernel shape agreement, bounded limits, and
  failure/fuzz/benchmark/consumer evidence. ADR 0042 and the twenty-fifth increment add finite
  query-accounted grouped physical state, exact fixed/variable/NULL keys, shared aggregate kernels,
  immediate failure cleanup, and property/failure/fuzz/benchmark/consumer evidence. Bound GROUP BY
  lowering follows in ADR 0043 and the twenty-sixth increment with structural bound-expression
  identity, computed/multiple keys and arguments, final expressions, empty semantics, finite limits,
  and failure/fuzz/benchmark/consumer evidence. ADR 0044 and the twenty-seventh increment add a
  blocking query-accounted physical sort, explicit all-type direction/NULL keys, allocation-free
  borrowed variable comparison, deterministic stable merge, arbitrary canonical row gather, exact
  plan shapes, and failure/fuzz/benchmark/consumer evidence. ADR 0045 and the twenty-eighth
  increment add one opt-in non-null WAL ID/record sequence/row ordinal/operation suffix to both
  CSEG and mutable-head vector sources, checked shared layout, zero-copy CSEG exposure, accounted
  canonical head materialization, exact-filter helper preservation, and failure/fuzz/benchmark/
  consumer evidence. ADR 0046 and the twenty-ninth increment add exact bounded SQL ORDER BY
  lowering for supported base and aggregate queries: binder-resolved aliases and non-projected
  keys, direction and NULL placement, DEDUP/group identity ties, the WAL commit-position suffix,
  hidden-column removal, sort-before-LIMIT order, scalar-oracle/failure/fuzz/benchmark/consumer
  evidence, and explicit rejection when generated logical identity is unavailable. ADR 0047 and
  the thirtieth increment add one exact append-only tablet source over the aggregate database
  epoch: durable parts, every sealed head, and the active head share exact predicates, projections,
  row-version shape, bounded admission, cancellation, and ownership without duplicate or omitted
  rows across flush replacement. ADR 0048 and the thirty-first increment connect any checked
  supported physical pipeline to that exact tablet source: schema and optional row-version input
  shape select one uniform source mode, bounded planning/loading/composition retains one snapshot
  epoch, and bound WHERE/ORDER BY/LIMIT or aggregate stages execute without manual source wiring.
  ADR 0049 and the thirty-second increment add exact STRING/SYMBOL/BINARY MIN/MAX to global and
  grouped aggregation with unsigned byte order, reserve-before-copy replacement, per-state payload
  limits, query-credit cleanup, SQL lowering, failure injection, fuzzing, and microbenchmarks.
  ADR 0050 and the thirty-third increment replace linear group lookup with one pre-sized
  query-accounted open-addressed table, canonical all-type hash framing, exact collision checks,
  floating zero/NaN equivalence, independent-model fuzzing, and cardinality profiles. ADR 0051 and
  the thirty-fourth increment add exact bounded LATEST BY lowering: computed timestamp preparation
  before WHERE, typed multiple/NULL groups, explicit physical-ordering and WAL/record/row winner
  ties, allocation-free adjacent winner compaction, suffix/helper hiding, aggregate/ORDER/LIMIT
  composition, scalar differential tests, and hostile/failure/fuzz/benchmark/consumer evidence.
  ADR 0052 and the thirty-fifth increment add a bounded two-input physical ASOF primitive with SQL
  NULL/NaN equality, exact timestamp/physical-key/row-version winners, ASOF LEFT null extension,
  explicit match presence, conservative state/output credit, sibling cancellation, failure
  injection, hostile fuzzing, and cardinality benchmarks. ADR 0053 and the thirty-sixth increment
  add a finite immutable left-deep ASOF plan with exact preparation/join/final shape handoffs,
  retained-configuration accounting, source ownership, allocation-failure cleanup, hostile
  fuzzing, and instantiation benchmarks. ADR 0054 and the thirty-seventh increment add bound ASOF
  lowering for computed and widened keys, prior-source expressions, ASOF LEFT nullability, exact
  joined identity ties, post-join WHERE/aggregation/ORDER/LIMIT order, hidden-column removal,
  failure injection, fuzzing, and lowering benchmarks. ADR 0055 and the thirty-eighth increment
  bind every checked ASOF source to one exact aggregate snapshot epoch, infer each source's suffix
  from its preparation shape, clean up partial construction, execute three-source SQL plans, and add
  failure/fuzz/benchmark/consumer evidence. ADR 0056 and the thirty-ninth increment add one-copy
  shared query credit plus a bounded parallel merge for independent unordered pipelines: complete
  task thread affinity, a fixed-capacity accounted-chunk queue, explicit release/acquire
  publication, deterministic failure arbitration, cooperative join cleanup, and concurrency/
  failure/fuzz/benchmark/consumer evidence. ADR 0057 and the fortieth increment add finite external
  sorting over contiguous stable runs: an ephemeral versioned/checksummed identity-free row format,
  exact cross-run tie recovery, bounded record/file/disk/configuration ownership, canonical
  pull-based output, corruption and early-cleanup behavior, and failure/fuzz/benchmark/consumer
  evidence. ADR 0058 and the forty-first increment split exact aggregate-publication bytes from
  per-image ownership and use one last-owner query reservation across complete tablet scans,
  surviving CSEG chunks, mutable heads, and same-epoch ASOF aliases, with hostile ownership,
  failure, fuzz, benchmark, and consumer evidence. ADR 0059 and the forty-second increment add a
  bounded physical strategy selector that owns its exact checked pipeline, consumes authoritative
  finite per-sort bounds, selects in-memory or stage-indexed external sort without changing SQL
  keys, and chooses serial or bounded parallel source composition only under an explicit complete-
  pipeline order-independence proof and lower deterministic work cost. The complete snapshot
  adapter executes optimizer-selected external SQL ORDER BY without inventing parallel tablet
  splitting. The forty-third increment closes the accepted Phase 9 boundary with 192 deterministic
  randomized full-plan scalar/vector comparisons across base, aggregate, LATEST, and ASOF plans,
  variable batch boundaries, exact result order and system ties, matched runtime failures and
  credit cleanup, expanded lowering fuzzing, and end-to-end batch/memory/tail profiles. **Phase 9
  exit gates are complete for the explicitly supported append-only SQL surface.** Future
  correction/delete row-version resolution awaits an accepted operation/visibility contract;
  mapped/asynchronous providers, parallel tablet morsels, adaptive rewrites, and additional join
  algorithms remain future optimizations rather than weaker substitutes in this phase.

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

Implementation progress: the first Phase 10 increment accepts Protocol v1's fixed 40-byte
checksummed frame, permanent message-type registry, compatibility/rejection rules, 16 MiB ceiling,
portable bounded codec, golden/corruption/boundary tests, allocation-failure classification, and
installed public target. Handshake payloads and connection/reactor behavior remain subsequent
increments.

The second increment accepts exact hello, ingest, durability acknowledgement, query, and error
payloads plus a bounded server connection state machine. It proves negotiated limits, strict
non-reusable request identities, finite in-flight admission, idempotent cancellation, direction,
close behavior, UTF-8 validation, and allocation-failure classification. Streaming transport and
reactor-to-shard routing remain next.

The third increment adds bounded portable connection buffers: arbitrary partial/coalesced reads,
exact ordered frame extraction, immutable partial-write ownership, independent inbound/outbound
byte and frame-count limits, deterministic overload, clear-on-disconnect, allocation sweeps, and
installed consumption. Linux readiness and shard handoff remain separate.

The fourth increment adds the finite reactor-to-shard SPSC task ring with an accepted ownership and
acquire/release proof, explicit saturation, allocation-free steady-state push/pop, FIFO/wrap tests,
a 100,000-task concurrency check, allocation failure, installed consumption, and TSan evidence.
The fifth increment adds the Linux nonblocking epoll owner, real socket I/O and response routing,
bounded admission, handshake/idle expiry, deterministic disconnect detachment, late-response
rejection, Linux-free public API, startup allocation classification, and Linux socket tests. Worker
wakeups, complete query-result payloads and the test client/server adapter, authentication/TLS
boundary, fuzzing, and Phase 10 measurement gates remain next.

The sixth increment assigns bounded self-describing query-result batches without fabricating table
identity: ordered names/types/nullability, canonical row-major cells, zero-row schema, exact NULL and
UTF-8 rules, independent decoding, allocation classification, and reactor response validation.
The test client/server adapter, authentication/TLS boundary, packet fuzzing, and measurements remain.

The seventh increment accepts the fail-closed security boundary: loopback-only plaintext, a borrowed
single-owner authenticator, stable principal propagation through request/cancel tasks, explicit
rejection metrics, and `TLS_REQUIRED` rejection until a maintained backend exists. The test
client/server adapter, packet fuzzing, hostile Linux lifecycle matrix, and measurements remain.

Phase 16 follow-up under ADRs 0144 and 0145 later supplied the maintained OpenSSL mutual-TLS carrier
and bounded epoll integration; the paragraph above remains the historical Phase 10 increment boundary.

The eighth increment adds the bounded portable native client session and replaces self-agreeing
packet-only coverage with real client-to-epoll interoperability. It covers partial/coalesced I/O,
negotiation, monotonic requests, query and ingest terminal rules, cancellation, fail-closed input,
allocation sweeps, installation, and Linux sockets. Packet fixtures/fuzzing, expanded hostile Linux
lifecycle coverage, and measurements remain.

The ninth increment adds a response-side Linux `eventfd` wakeup. It coalesces shard notifications,
interrupts blocked epoll waits without polling, preserves the SPSC release/acquire data edge, and
has a narrow cross-thread lifetime plus a real blocking-wakeup test. Hostile lifecycle, packet fuzz,
and measurement gates remain.

The tenth increment closes the hostile Linux transport matrix with explicit-cancel publication and
late-result rejection, readable-before-half-close dispatch followed by deterministic detach cancel,
128-connection descriptor churn, slow-handshake and admission bounds, request-queue shard stall,
and an 8 MiB real result forced through short writes with exact terminal ordering. Packet fuzz and
measurement gates remain.

The eleventh increment adds source-controlled valid, truncated, and bad-checksum packet fixtures
plus a dedicated protocol/stream/message/server-state/client-state libFuzzer target. A 100,000-input
ASan/UBSan campaign passes with finite 128 KiB inputs. Measurement and final phase audit gates remain.

The twelfth increment adds a portable and Linux-only native-network benchmark suite covering frame
and result-batch codecs, allocation counts, partial-read shapes, queue capacities and explicit
saturation, connection churn, and equal-work 1/8/32-connection request rounds. The first Linux run
identified a delayed terminal-frame interaction and justified `TCP_NODELAY` before admission. A
clean-commit Ubuntu 24.04/LinuxKit aarch64 baseline retains three raw repetitions, unfavorable
outliers, exact configuration, and evidence limits. Final phase audit gates remain.

The Phase 10 exit audit closes the remaining Linux gate with an Ubuntu 24.04 GCC warnings-as-errors
build and complete test run, plus an explicit real-socket networking run. Malformed-input bounds,
partial I/O, slow peers, churn, queue saturation, shard wakeup/stall, cancellation, half-close,
large short writes, allocation classification, security fail-closed behavior, and the measured
`TCP_NODELAY` decision are covered. **Phase 10 exit gates are complete for the bounded embeddable
native-network library.** A packaged production daemon, remote plaintext, TLS record backend,
`io_uring`, distributed routing, and storage/query service adapter remain outside this phase.

## Phase 11 — Subscriptions and incremental materialized views

- **Feature-pass status:** the `chronos_live` target implements source-tagged authenticated Resume
  Token v2 issuance with v1 WAL compatibility,
  single-source register-before-boundary handoff, bounded retained/buffered committed changes,
  at-least-once poll/acknowledge/resume, fail-closed overflow/cancellation, removable count/sum/
  min/max/VWAP/OHLC/Welford state, tumbling/sliding windows, watermarks, corrections, and logical
  materialized-view progress. Exact bounded logical checkpoints now preserve rows, aggregate running
  state, revisions, finalization, watermark, and committed progress across owner reconstruction.
  Materialized View Checkpoint v1 now preserves that state in bounded, versioned, checksummed exact
  bytes, with a bound envelope for database/view/table/schema/version/plan identity. A view-scoped
  locked owner exact-validates, file-syncs, no-replace installs, directory-syncs, reopens, and selects
  those immutable checkpoints. Backward-compatible bound-envelope generations preserve multiple
  watermark/revision states at one source sequence. A durable application owner now creates or
  reopens exact view configuration, applies only the consecutive committed suffix, checkpoints
  watermark-only progress, and exposes a source-retention frontier only after durable installation.
  Negotiated Native Protocol 1.1 now carries snapshot batches, ready tokens, WAL-only committed
  changes, acknowledgements, checkpoints, resumable termination, and manager-backed delivery.
  Protocol 1.2 preserves explicit WAL/Raft source tags in committed changes while retaining the
  same bounded envelope size and rejecting cross-version payloads. A plan-bound
  multi-tablet coordinator now captures canonical source vectors, enforces each log independently,
  records cross-tablet delivery admission, and resumes the exact retained component-wise suffix.
  A single-tablet service owner now executes an already-lowered physical plan against the exact
  registered aggregate-storage boundary, encodes snapshot batches and END_STREAM, then opens READY
  without exposing buffered changes early. `SUBSCRIBE SELECT` now has bounded parse/bind/lowering,
  exact schema-bound SHA-256 plan identity, and plan-bound single-tablet registration/resume.
  Multi-tablet coordinators can now checkpoint and restore an exact validated retained admission
  order and per-source expiry vector before token-based replay. Multi-tablet Subscription Checkpoint
  v1 freezes the WAL-only layout; Checkpoint v2 and Generation Envelope v2 now preserve explicit
  WAL/Raft source tags while compatibility decoders recover existing v1 generations. A lock-owning
  filesystem owner
  now exact-validates, synchronizes, no-replace installs, reopens, and selects contiguous immutable
  coordinator checkpoint generations. A durable coordinator owner restores that exact generation,
  resumes token suffixes, accepts consecutive post-checkpoint replay, and publishes source-retention
  frontiers only after installation succeeds. Multi-tablet historical execution now validates the
  complete registered vector against one aggregate storage epoch and runs one global physical plan
  before END_STREAM and READY. A locked durable plan registry preserves exact SQL definitions and
  returns an executable fingerprint only after exact catalog-bound reprepare. Recovered plans now
  start exact global snapshots through the durable coordinator without exposing mutable manager
  state, and every pre-READY failure uses allocation-independent abandonment. A committed schema
  incompatibility now terminates the old plan distinctly, emits the precise subscription reason,
  invalidates resume, and survives durable checkpoint/reopen. A bounded reactor-facing owner now
  drives new/resumed snapshots, READY, live delivery, acknowledgement, cancellation, exact
  response-ring retry, and resumable shutdown. A topology-bound retention authority now intersects
  storage/Raft safety with every durable plan frontier and rejects placement drift before invoking a
  physical source reclaimer. WAL-backed sources now exact-map complete logical batches to verified
  record-end checkpoints, conservatively merge tablets sharing one WAL, and reclaim only after
  all-WAL prevalidation. A deterministic Release microbenchmark suite now covers logical handoff,
  fan-out, slow-consumer overflow, resume replay, late aggregate correction, and logical aggregate
  recovery scaling without publishing unreviewed numbers. Source-neutral in-memory positions and
  Resume Token v2, coordinator membership, and Checkpoint v2 now distinguish WAL IDs from Raft
  group UUIDs without aliasing. Protocol 1.2 now delivers those tagged positions without changing
  Protocol 1.1 or 2.0 bytes. Homogeneous Raft-backed subscriptions now acquire exact immutable
  applied tablet snapshots after registration and execute one global historical pipeline. Their
  topology-bound retention path requires matching application and durable Raft snapshot coverage,
  then schedules one bounded worker-owned all-group checkpoint before reclaiming older shared-log
  segments. Mixed WAL/Raft historical subscriptions now exact-bind the registered product vector,
  share one aggregate publication across WAL members, retain independent immutable Raft
  publications, and run one global physical plan in canonical member order. The bounded retention
  registry now admits plans only before service activation and at or beyond prior reclamation, then
  retires them explicitly after service drain without advancing physical authority in the removal
  call. A real multi-chunk WAL snapshot now preserves commits injected before the first pull and
  after every chunk, END_STREAM, and READY boundary, while each premature pre-READY teardown
  fail-closed abandons its buffered session. Deterministic allocation failure at every observed
  single-tablet WAL and multi-tablet WAL/Raft/mixed owner allocation across start, chunk,
  END_STREAM, and READY is classified as resource exhaustion and leaves no active session or query
  credit. Service admission allocation failures likewise either admit the exact durable snapshot or
  fail closed without an active request; first result, END_STREAM, and READY failures likewise
  terminally remove the service entry. One-slot response-ring saturation across a five-chunk
  snapshot retains and retries each exact response without advancing the snapshot twice. The
  durable checkpoint store now classifies every observed install and generation-load allocation,
  preserves retryable exact state after each failed install, and does not mislabel allocation
  failure during canonical filename validation as durable corruption. Its coordinator owner now
  proves allocation failure cannot publish a generation or retention frontier before exact durable
  installation. Materialized View Checkpoint v1 and its bound envelope now have structure-aware
  hostile-byte, checksum-repaired mutation, limit, and exact re-encoding fuzz coverage, and
  classify every observed encode/decode allocation. Their store provides the same exhaustive
  install, generation-load, and latest-selection allocation guarantees, while the durable owner
  preserves the prior generation and source-retention sequence until exact installation. Durable
  plan-registry install and load/reprepare allocations are likewise
  classified, retryable, and non-poisoning. The
  homogeneous WAL vector now executes grouped aggregate, global sort/limit, and latest/limit plans
  once above all tablets in addition to the existing global ungrouped aggregate; the real mixed
  WAL/Raft vector now passes the same matrix across both authorities. A release-store schedule now
  proves concurrent aggregate publication yields either the complete predecessor snapshot or an
  exact-boundary cancellation after the successor linearizes. The remaining exit evidence remains
  deferred. The phase exit gate is not claimed.

- **Scope:** committed change model; gap-free snapshot-to-stream handoff; deterministic versioned resume tokens; bounded subscriber policies; supported incremental operators; materialized-view progress/recovery and late-event corrections.
- **Explicit non-scope:** unqualified end-to-end exactly-once claims, unlimited retention, every SQL operator, cross-cluster delivery, and external-sink transactions not explicitly integrated.
- **Required artifacts:** subscription/delivery protocol; resume-token and retention specifications; handoff ADR; watermark/lateness and view-correction contracts; incremental engine; recovery metadata; learning documents.
- **Correctness exit gate:** snapshot plus continuation omits no post-boundary committed change; resume yields the defined suffix or precise expiry/incompatibility error; views equal full recomputation; slow subscribers cannot indefinitely block ingestion.
- **Testing exit gate:** commits injected at every handoff step, disconnect/retry/restart, duplicate delivery, token tamper, retention expiry, late corrections, subscriber fan-out, stalled consumers, and differential recomputation pass.
- **Measurement exit gate:** measure handoff delay, update latency, operator state, replay/recovery cost, fan-out scaling, memory bounds, and ingestion impact under lateness and slow-consumer distributions.

## Phase 12 — Performance engineering and io_uring comparison

- **Feature-pass status:** explicit portable backend selection and optional thread placement hooks
  are implemented. The opt-in Linux liburing backend now owns accept, receive, send, and response
  wakeup socket operations without leaking Linux types. A focused Ubuntu/GCC/liburing build and
  Linux 6.12 fragmented-I/O lifecycle test passed. Parallel query workers now apply exact optional
  placements behind an all-worker startup gate before any pipeline runs. Dense zero-NULL identity
  timestamp filtering now has runtime-dispatched scalar, AVX2, and AArch64 NEON kernels with
  differential coverage; NEON ran locally and AVX2 passed compile-only validation. Epoll remains the
  reference. Broader parity/SIMD work, NUMA memory placement, profiling, and performance evidence
  remain deferred; no speed claim or phase measurement exit is declared.

- **Scope:** profile verified single-node paths; remove measured bottlenecks; establish reproducible benchmark governance; compare an optional `io_uring` prototype with epoll under equal semantics.
- **Explicit non-scope:** weakened durability/checksums/visibility, selective publication of favorable runs, a mandatory `io_uring` migration, distribution, and novel allocators or lock-free rewrites without evidence.
- **Required artifacts:** benchmark specification/harness/datasets; hardware and run manifests; profiles; regression thresholds; optimization ADRs where architecture changes; epoll/`io_uring` comparison report.
- **Correctness exit gate:** optimized and reference paths pass identical invariant, crash, corruption, and differential suites; all benchmark modes name their durability and consistency guarantees.
- **Testing exit gate:** reproducibility across repeated clean runs; performance regression tests with noise policy; sanitizer/fault suites remain green; `io_uring` failure/cancel paths receive parity testing if retained.
- **Measurement exit gate:** report distributions and resources, not a single peak; retain unfavorable results; adopt `io_uring` only if an important tested workload shows durable end-to-end benefit that justifies complexity.

## Phase 13 — System-time history

- **Feature-pass status:** the existing parser/binder/executor `FOR SYSTEM_TIME AS OF` path now has
  a real committed in-memory temporal snapshot provider. It atomically appends originals,
  corrections, replacements, and tombstones, distinguishes event/receive/system time, resolves the
  latest visible version at a system boundary, and fails closed after retention expiry. Frozen CSEG
  v1 remains append-only. A canonical checksummed Temporal Mutation Command now stores columnar
  originals/corrections/replacements/tombstones and their event/receive/system times; WAL commit
  application plus command-specific whole-log recovery rebuild fresh multi-table scalar history and
  reopen the writer at the next sequence. Live single-writer admission validates before WAL,
  preserves `ASYNC`/`LOCAL_SYNC`, publishes after completion, and fails closed on post-admission
  uncertainty. CSEG v2 now has an accepted eight-column WAL/Raft temporal system registry, a checked
  canonical layout planner, a strict checksummed metadata codec with schema binding and v1/v2
  rejection boundaries, and deterministic full-part composition/structural decoding that validates
  every stored page and alignment region. Bounded semantic acceptance now validates every temporal
  tuple, source/order domains, logical identity, event-time extrema, physical ordering, and exact
  schema/tablet binding. Schema-aware projected reads now account for and validate all eight system
  pages while touching only requested user pages. A bounded scalar CSEG resolver now provides
  current/as-of winners for one explicit authoritative WAL/Raft source lineage, including tombstone
  removal and deterministic timestamp/position ties. Manifest v2 now has an accepted source-neutral
  registry, checked layout, and strict checksummed codec with exact CSEG content fields. Exact
  single-part admission now derives and checks SHA-256, full temporal semantics/schema binding,
  uniform WAL/Raft lineage, and recomputed commit/event/system extrema. Ordinary add-only generation
  transitions now enforce exact succession, immutable retained history/retries/source lineage, and
  monotonic application/reclaim/schema boundaries. Complete-generation admission now requires
  exact descriptor-order object coverage, canonical names, retained schemas, and per-tablet source
  validation. The locked local filesystem owner now durably installs exact CSEG v2 parts and
  add-only Manifest v2 successors with prevalidation, readback validation, file sync, no-replace
  rename, directory sync, and post-rename poisoning. Highest-generation v2 recovery selection now
  exact-decodes without fallback, binds the database/schema/WAL-or-Raft owner registry, validates
  every referenced temporal part, and returns an owning unpublished generation. Exact requested
  part loads now pin that generation and revalidate its CSEG bytes. A bounded tablet resolver
  composes those images, prunes only future-only parts by Manifest system-time minima, validates and
  decodes every remaining page, and applies the exact scalar current/as-of oracle; requests before
  retained history fail with `NOT_FOUND`. Fresh temporal providers now expose an atomic canonical
  retained-history seed boundary that permits compacted correction/tombstone predecessors and
  leaves no partial state on validation or allocation failure. Complete generation-pinned CSEG part
  sets can now be exact-decoded, copied, canonicalized across parts, and atomically restored into a
  provider under a caller-proven tablet-wide retention boundary. One fail-closed startup owner now
  permits the global physical checkpoint to equal or trail a single tablet's durable position,
  exactly verifies every physically retained covered row, permits only absent pre-retention rows to
  be treated as reclaimed, applies only the later WAL suffix, retains both locks and the selected
  generation, and returns the writer at the next sequence. Its report distinguishes the checkpoint,
  tablet boundary, verified overlap, and applied suffix. The same owner now restores every selected
  distinct-table WAL tablet, routes covered/suffix commands by table, and reports exact per-tablet
  progress; ambiguous same-table multi-tablet routing fails explicitly because command v1 has no
  tablet identity. Resolved current/as-of scalar winners now feed canonical, bounded,
  query-accounted owned vector chunks with cancellation and sticky end. Raft/mixed-source
  application snapshots, allocation-failure coverage beyond the canonical empty/one-CSEG/two-CSEG
  one-tablet and CSEG-backed/empty two-tablet Manifest startup paths, complete query-epoch
  publication, v1 migration, direct vector winner resolution/lowering, and
  authorized durable retention/compaction integration remain deferred. In-memory history
  compaction now preserves the exact-boundary time-index predecessor, enforces monotonic position/
  time frontiers, and reports exact removal state. The phase exit gate is not claimed.

- **Scope:** formal bitemporal row-version model; SQL system-time clauses; history retention; correction/cancellation semantics; compaction and index support; audit visibility.
- **Explicit non-scope:** general distributed transactions, legal/compliance certification, retroactive mutation of immutable history, and distribution before the model is validated locally.
- **Required artifacts:** temporal model and SQL specification; retention/GC ADR; storage and query changes; migration plan; bitemporal oracle; learning document.
- **Correctness exit gate:** event time and commit system time remain distinct; every supported as-of query returns the model's version; compaction/retention never remove a still-promised version; corrections are auditable.
- **Testing exit gate:** generated multi-version histories, ties/boundaries, late corrections, restarts, compaction, retention pins, and scalar/vector differential queries pass against the bitemporal model.
- **Measurement exit gate:** quantify history space/write amplification, as-of scan cost, compaction overhead, and retention-policy sensitivity on declared version distributions.

## Phase 14 — Deterministic Raft

- **Feature-pass status:** `chronos_raft` implements a deterministic follower/candidate/leader core,
  persistent term/vote/log/commit/apply/snapshot metadata, RequestVote log freshness, AppendEntries
  validation/conflict rewind, next/match indexes, current-term majority commit, stale-term rejection,
  explicit persist-before-send transitions, and complete message-local validation before a newer
  term can mutate persistent state. Higher-term attempts to replace committed entries now have
  focused term-conflict and same-term byte-divergence state-preservation coverage. Canonical
  joint/final membership commands enforce a construction-time `u16` voter-limit bound and old-and-
  new election and commit quorums, recover from the retained log, and safely remove leaders.
  A sender outside the current active set must now prove AppendEntries leadership through a
  matching valid suffix that derives its membership before term observation, preserving new-only
  leader catch-up while rejecting unproven nonvoter heartbeats.
  Exact full-state payload accounting now bounds the aggregate snapshot voters, retained-entry
  framing, and payload bytes on recovery and before every log- or snapshot-changing transition, so
  an individually legal entry cannot create an unencodable persistent state. Recovery fills a
  legacy nonempty snapshot's missing voter checkpoint before applying that bound, so canonicalizing
  old state cannot create an over-budget replacement.
  Focused 3-node election, commit, failover, stale leader, restart catch-up, and membership tests
  pass. A two-stage snapshot protocol now withholds acknowledgment until external application
  installation is confirmed and the compacted Raft state is synchronized. AppendEntries
  predecessors below that installed boundary now fail as ordinary conflicts without aliasing the
  snapshot entry into retained-log storage, while higher-term failures still carry their required
  persistence transition. Pending external snapshot installation also excludes local compaction,
  preventing competing immutable snapshot identities at one index. Exact remote duplicates
  coalesce, while different pending requests fail negatively without replacing the original
  completion identity. A committed-prefix conflict preserves that identity until exact explicit
  rejection. Snapshot metadata whose last-included term exceeds its request term now
  fails direct-core and transport admission before higher-term observation. Recovered snapshots
  cannot exceed current term, and failed append feedback requires a nonzero conflict index with any
  conflict term bounded by its response term. Recovered term-zero state cannot contain a vote, and
  an index-zero snapshot cannot retain external or membership identity. Snapshot index `UINT64_MAX`
  is rejected by direct-core and transport admission before it can create an unreopenable completed
  installation. Vote requests also reject that reserved last-log index before higher-term
  observation, preventing votes for an impossible candidate history. AppendEntries requests reject
  the same value as a predecessor, including entry-free heartbeats, through core and transport
  admission, and reject it as `leader_commit` before higher-term observation. AppendEntries
  responses reject it as an impossible actual match index before response-term observation, and
  InstallSnapshot responses reject it as an impossible installed boundary. A bounded canonical
  group/source/destination transport envelope now round-trips vote, append, snapshot, and read-
  barrier messages without introducing sockets into the deterministic core. Header-first bounded
  stream ownership now validates allocation-relevant fields before exact frame allocation and
  retains complete outbound bytes across short writes. Eight independently generated immutable
  golden frames bind every current message kind to exact bytes in the standard GCC, Clang/libc++,
  and AppleClang matrix. Checksum-repaired hostile field matrices, exhaustive allocation sweeps, and
  a structure-aware raw/canonical/mutated-frame fuzzer now cover the complete codec and stream-owner
  boundary under sanitizers. An authenticated receiver now
  authorizes the claimed source, exact-matches the local destination, and admits the message through
  the asynchronous durable runtime before exposing response bytes. A persistent bounded inbound
  mutual-TLS session now reads exact fragmented frames, admits one durable operation, and publishes
  its complete result for embedding-owned routing. A persistent peer-authenticated outbound session
  now bounds FIFO frames/bytes and preserves complete frames for reconnect retry. A fixed-capacity
  exact-peer pool now preflights whole durable results against every route and aggregate queue bound,
  then returns failed carriers with complete retry frames. An exact-route nonblocking TCP attempt
  now retains complete retry frames through connect, creates TLS only after `SO_ERROR`, and transfers
  descriptor/carrier ownership together into the pool. A per-peer owner now retries indefinitely at
  exact monotonic deadlines with capped exponential backoff and retakes complete failed-carrier
  frames. A fixed-capacity multi-peer manager now validates immutable routes, exposes exact
  descriptor interests, installs successful pairs, recycles failed pairs, and leaves unroutable
  fresh durable results with their bounded upstream owner. A bounded TCP listener/poll table now
  admits persistent inbound mutual-TLS sessions and pins each post-sync result until explicit
  pickup; stable connection IDs and external readiness driving permit safe outer-loop composition.
  Terminal peer closure now retains already admitted durable work through exact result pickup.
  Every inbound receive now executes with its immediately following owning observation in
  one durable FIFO batch, providing the exact post-message role and term for timer rearming.
  Unified inbound/outbound/timer/durable-completion polling remains deferred. A bounded
  deterministic simulator
  now records and replays explicit or seeded partitions, delay/reordering, duplicate/loss,
  crash/restart, atomic persistence faults, application, membership, and snapshot actions; it checks
  election, log, commit, and leader-completeness safety after every step and bounded chunk-shrinks
  failing traces. Seeded schedules now derive valid joint-membership begin/finalize, local snapshot-
  compaction, and current-term read-barrier actions from current state and replay that churn exactly.
  A bounded exhaustive mode now enumerates every virtual-network delivery/loss suffix plus opt-in
  duplicate-message, directional partition/healing, atomic persistence-failure arming, eligible
  elections, multi-voter heartbeats, canonical bounded application proposals, current-term read
  barriers, application-frontier advancement, state-derived membership begin/finalize, applied-
  frontier snapshot compaction, pending snapshot-install rejection/safe completion, and live-state
  crash/restart faults after a valid setup trace, and reports replay-limit truncation without
  claiming completion. Variant-indexed attempt counters now exact-match retained seeded and replay
  traces, including the terminal failing action, while effect counters remain distinct. A local
  benchmark target now times seeded generation and fixed-trace replay, with the safety oracle
  enabled, for declared three- and five-voter topologies; it publishes no baseline and leaves the
  wider measurement gate open. Canonical Raft envelope encode and checked owned decode now have
  separate local cases for fixed, heartbeat, small/batched AppendEntries, and snapshot-metadata
  shapes, also without a published baseline. A separate production-path artifact harness now times
  deterministic full-state batches in APPEND_ONLY and LOCAL_SYNC modes, retains raw latency and log
  images, and requires exact immediate reopen; it publishes no clean-host baseline, does not qualify
  power loss, and leaves commit/catch-up/snapshot-transfer measurement open. Simulations may begin from
  exact per-node recovered persistent images; a combined terminal-term and terminal-index schedule
  proves proposals and post-restart elections fail without mutation. Deeper mixed-action campaigns,
  broader application payload domains, clock changes, physical-log faults, and the full exit evidence
  remain deferred.
  Linearizable reads now use one bounded explicit current-term leadership
  probe, require a current-term committed entry, freeze stable or joint voter quorums at issuance,
  reject nonvoter probe sources before higher-term observation while retaining learner replication,
  abandon pending work on leadership change, and return an exact committed read index that must be
  applied before visibility. Issuance now prepares all owned quorum state and the complete probe
  batch before publishing the pending barrier or consuming its context, so allocation failure is
  retryable with exact state and context preservation. Accepted-response allocation likewise
  either counts the voter once or returns resource exhaustion with the frozen quorum unchanged.
  Recipients now pre-own the response slot and exact post-term persistent state before observing an
  admitted request term. Leaders likewise own the post-term state before a higher-term response can
  demote them and discard the pending barrier, closing both allocation edges around term observation.
  Vote, append, and snapshot responses now use that same pre-observation persistence preparation,
  so no canonical higher-term response can demote a node before its durable transition is owned.
  Vote requests now prepare their prospective grant/rejection response and exact term/vote state
  before mutation, including higher-term and same-term first-vote paths.
  Local election start now prepares its exact persistent state, self vote, complete outbound batch,
  and any immediate-leader replication maps before publishing the new term or role.
  Candidate vote responses now prepare the replacement acknowledgement set and, at quorum, every
  leader replication map and initial heartbeat before publishing the vote or leadership.
  AppendEntries responses now prepare replacement follower progress and any derived membership,
  durable commit state, leader removal, and complete replication batch before publication. A
  decisive final-membership acknowledgement allocation sweep now proves the stable new-voter
  broadcast and removed-leader demotion publish together.
  InstallSnapshot responses likewise own successful replacement progress and the exact suffix or
  snapshot follow-up before publication.
  AppendEntries requests now retain their validated candidate state and own stale/conflict feedback
  or accepted suffix persistence and commit feedback before changing term, role, log, or membership.
  Joint-admission and final-commit allocation sweeps now prove the same boundary for follower
  membership changes, including authority established by the incoming joint suffix.
  InstallSnapshot requests now own stale/already-installed/competing feedback, any post-term
  persistence, and both the core pending identity and external task for a new installation before
  changing role, leader identity, or pending work. Same- and higher-term competing-request sweeps
  now prove failed allocation preserves the exact original completion authority, including the
  pre-demotion term/vote state in the higher-term case.
  Snapshot completion now owns rejection feedback or the complete retained-suffix, membership,
  durable-state, commit-notification, and acknowledgement transition before releasing its pending
  identity or installing snapshot state. The same ownership boundary covers a stale completion
  after a higher-term competing request, retaining the old authority until its current-term negative
  response exists.
  Local compaction now owns the canonical voter checkpoint, retained suffix, replacement snapshot
  base, and complete durable transition before erasing the live log prefix. Membership checkpoints
  now replay the exact compacted prefix, reject joint-state boundaries, and are precomputed by both
  application snapshot owners before file installation.
  Applied-index advancement now owns both post-apply persistent-state copies before changing the
  committed-unapplied boundary, preserving exact retry under resource exhaustion.
  Ordinary proposals now prepare the full prospective node, self progress, any immediate commit,
  complete replication batch, and returned durable state before publishing the appended entry.
  Current-term progress no-ops now share that transactional append boundary, so exact-retained
  prior-term retries cannot leak a proof entry or commit under resource exhaustion. Joint- and
  final-membership retry sweeps now prove this boundary preserves the retained command and appends
  only one no-op under the active joint quorum.
  Joint and final membership proposals now prepare the complete prospective node, derived active
  configuration and progress maps, outbound replication, and durable transition before publishing
  either configuration entry.
  A bounded generation-tagged monotonic timer scheduler now emits election and heartbeat actions,
  retries rejected admission without shifting deadlines, and rejects stale completion rearming;
  its bounded driver now composes those actions with ordered asynchronous durable observations and
  retains complete results. The durable owner now publishes a portable coalescing completion
  descriptor after owning results, and every timer/transport aggregate exposes its exact earliest
  monotonic deadline. Runtime-lifetime FIFO submission identities now order timer and inbound
  completion queues; terminal outbound events immediately enter whole-frame capped reconnect.
  Outbound TLS begins with client-write readiness and then follows exact OpenSSL handshake interest.
  A bounded unified production runtime now polls durable wakeups, inbound/outbound descriptors, and
  exact deadlines; merges FIFO completions, rearms activity, routes with retry-safe backpressure, and
  retains application/snapshot/read work for explicit pickup. Broader multi-node production-carrier
  fault validation remains.

- **Scope:** implement a deterministic Raft core for one logical group: elections, replication, commit, membership protocol as scoped by ADR, snapshots, read consistency mechanisms, and simulated transport/storage/time.
- **Explicit non-scope:** multi-group multiplexing, production network integration, distributed queries, hidden third-party Raft implementation, and serving uncommitted or merely appended entries.
- **Required artifacts:** protocol/state-machine specification and ADRs; deterministic core; simulator/model checker harness; persistent-state formats; snapshot/install contract; safety/liveness test corpus; learning document.
- **Correctness exit gate:** election safety, log matching, leader completeness, committed-entry durability, deterministic apply, membership safety, and read contracts hold; uncommitted entries are never visible.
- **Testing exit gate:** exhaustive bounded schedules where feasible; randomized long simulations with partitions, delay, duplication, crashes, disk faults, clock changes, snapshots, and membership; trace shrinking and reference/model comparison pass.
- **Measurement exit gate:** measure simulation coverage/rate, message and fsync cost, commit latency, recovery/catch-up, and snapshot transfer in stated topologies; performance cannot override safety.

## Phase 15 — Multi-Raft tablets

- **Feature-pass status:** a bounded node-local Multi-Raft owner multiplexes independent logical
  groups, node-global persistence sequences, outbound batches, and application indexes. A versioned
  checksummed full-state physical record codec, single-owner segmented append/sync/recovery log, and
  committed-order metadata state machine are implemented. Focused tests cover different group
  leaders, isolation, node loss, reopen, metadata order, rotation, tail repair, corruption, and
  terminal physical-sequence rejection before group mutation. Undersized one-transition outbound
  configurations now fail construction instead of discovering overflow after a core state change.
  The durable runtime also tightens the core's aggregate state budget to its configured segment
  target before constructing groups, preventing a record-size failure after a transition mutates
  volatile state.
  A single-thread-affine durable runtime now batches caller-provided operations behind one local sync
  and withholds outbound messages until it completes. It reserves aggregate worst-case outbound
  fanout before dispatch, so an undersized batch cannot partially mutate group state. Mutable heads,
  tablet publications, row
  versions, and retry outcomes now distinguish WAL histories from Raft group/index histories;
  frozen CSEG/Manifest v1 boundaries reject Raft identities they cannot represent. Asynchronous
  committed command application now decodes exact COLUMNAR_APPEND bytes, preserves uncommitted
  invisibility, durably advances applied indexes after publication, and rebuilds fresh tablet state
  from the complete retained committed log. A versioned, checksummed Raft tablet
  application-snapshot codec now binds group/table/tablet identities and complete snapshot metadata
  to exact original command positions. A group-locked local owner exact-validates, file-syncs,
  no-replace installs, directory-syncs, reopens, and selects those immutable bytes. The tablet state
  machine exact-matches a compacted Raft boundary, rebuilds that prefix plus the committed retained
  suffix, and preserves membership-only application frontiers. The same locked owner now extends
  exact application commands to a newer applied boundary, installs them durably first, and then
  compacts Raft to matching metadata. Node-wide shared physical-log reclamation now persists one
  fresh full state per resident group in a new segment, installs a checksummed recovery anchor, and
  only then removes older whole segments. Tablet and metadata owners now exact-match durable Raft
  snapshot authority before explicitly reclaiming every older or crash-orphaned future application
  snapshot and directory-syncing cleanup. A bounded FIFO asynchronous owner now exclusively runs
  the durable runtime,
  applies explicit batch/operation and pre-queue aggregate-outbound backpressure, publishes owning
  completions, drains on shutdown, and fails queued work closed after terminal storage errors;
  group-aware fairness remains deferred. A leader
  under stable or joint membership can now produce a checked quorum-sync receipt after
  majority-derived durable commit, and tablet application composes it with visibility. Protocol
  2.0 now negotiates the client capability, admits durability value 3 only under its feature bit,
  and carries an exact receipt-shaped acknowledgement; replicated service advertisement/execution
  and end-to-end crash evidence remain. The phase exit gate is not claimed.
  The asynchronous owner now also accepts one optional application extension whose initialize,
  per-batch prepare/complete, and shutdown hooks all run on the durable worker. Completion is not
  published until that extension finishes, providing the ownership-safe seam for tablet/metadata
  application and receipt construction without a Raft-to-ingest dependency cycle.
  A concrete bounded tablet extension now recovers every configured tablet before admission,
  applies only request-touched committed groups before completion publication, durably advances
  applied indexes, and exposes pinned snapshots plus copied latest receipts without leaking the
  synchronous owner. Bounded weakly owned completions now correlate an exact group, admitting
  leader term, and applied log index, reject term loss, and publish only after the whole extension
  batch succeeds. A nonblocking service operation now
  exact-validates the canonical term-bound proposal result, waits for that applied receipt, derives
  APPLIED versus MATCHING_RETRY from the tablet publication, and projects the frozen protocol-v2
  acknowledgement. Metadata composition, reactor routing/deadlines/backpressure, and packaged
  daemon composition remain. Reactor-dispatched requests and cancellation events now retain the
  exact negotiated version, feature bits, and payload bound through the SPSC handoff, so a service
  can authorize QUORUM_SYNC without reconstructing connection capabilities.
  A bounded service coordinator now owns multiple such operations, validates negotiated task
  authority, derives the tablet group from committed placement/binding metadata, queues an ordered
  group observation, exact-validates the committed active schema before and after that observation,
  and admits only stable local leadership under its exact term. It polls requests round-robin,
  enforces exact cancellation/deadlines, reports finite metrics, and releases one correlated
  acknowledgement or error for response-queue backpressure.
  Group-scoped read barriers now flow through both Multi-Raft owners without fabricating a durable
  transition, while higher-term recipient state still crosses the existing sync-before-response
  boundary. The packaged query gate now submits current-term progress and barriers through the
  authenticated transport owner, correlates exact group/term/context completions under one finite
  waiter, and requires metadata/tablet application publications to cover every returned read index.
  The asynchronous owner now exposes FIFO-ordered bounded owning group observations for local
  leader, commit/apply, and stable/joint membership state without releasing worker-owned pointers.
  Those observations now feed the same semantically validated, checkpoint-first and ledger-
  prepared reconfiguration reconciliation path. Durable requests can now atomically require the
  exact current leader term at single-owner dispatch; authenticated production leader routing
  remains.
  The dedicated metadata group now has canonical versioned/checksummed command bytes and committed
  application/reopen recovery for nodes, schema identities, tablet placement epochs, leader hints,
  complete partition/retention/history/lateness policy, and complete immutable table schemas with
  SQL catalog names. Canonical Metadata Application Snapshot v1 bytes now retain exact original
  metadata/schema entries and Raft membership identity for later compaction/recovery. Additive
  Snapshot 1.1 retains a separate checksummed immutable tablet-to-group binding without changing
  placement or minor-0 bytes. A dedicated
  locked owner now exact-validates, file-syncs, no-replace installs, directory-syncs, and reopens
  those immutable snapshots. The metadata application owner now installs them before Raft
  compaction and exact-rebuilds a compacted catalog from snapshot plus committed suffix. Database
  namespaces and catalog tombstones remain;
  replicated-ingest admission exact-compares placement with stable committed group membership;
  automatic placement-driven membership orchestration is not yet integrated.
  One owning replicated-ingest runtime now composes the exact tablet and metadata extensions,
  asynchronous durable worker, and coordinator with address-stable create/reopen and ordered
  shutdown. A bounded reactor-facing service now consumes negotiated request/cancellation tasks,
  retains one exact response across SPSC backpressure, reports response wakeups, and drains admitted
  work after closing admission. A database-root owner now preflights the committed metadata catalog
  from explicit resident group configuration, reconstructs bounded local tablet owners while
  ignoring nonresident remote bindings, and reopens the complete asynchronous service under the
  root lock. A strict bounded deployment-text parser now supplies the external resident group/voter
  set without overriding recovered consensus. The packaged daemon now securely loads that file,
  recovers the replicated owner, auto-elects exact local single-voter groups, advertises Protocol 2
  QUORUM_SYNC only in that mode, routes reactor tasks through the bounded service, and drains in
  ownership order. Authenticated multi-node peer transport/elections and applied-vector native
  SELECT are now composed. Protocol 2 now has a negotiated, terminal, placement/term-bound leader
  redirect response that rejects emission after partial query output. Replicated ingest now selects
  it only from exact committed placement/stable membership plus an ordered follower observation;
  endpoint-aware client retry now has a bounded exact-group route-policy owner, exact portable
  QUORUM_SYNC session replay, and a deadline-bound mutually authenticated TCP reconnect carrier,
  plus a whole-operation-deadline-aware poll owner and strict native endpoint/certificate-principal
  configuration with secure address-stable TLS-context ownership. The packaged `chronosctl`
  command now exact-validates one canonical append and drives that complete bounded route/TLS
  execution to a receipt. A finite-query replay owner now retains exact SQL across fresh Protocol 2
  sessions and withholds bounded owned result batches until `QUERY_END`; an authenticated
  deadline-bound TCP/mutual-TLS carrier now drives that owner and reconnects only for validated
  redirects. A whole-operation-aware poll owner supplies cancellation, bounded waits, and terminal
  route/attempt metrics. `chronosctl routed-sql` now packages that composition for one explicit
  group and emits only a terminally validated TSV result. The packaged service now derives a
  single-table-group route and redirects only when ordered observations prove one common stable
  remote leader across its entire read gate. A distinct proof-bound mutable vector fragment now
  binds and locally executes one exact committed/applied TabletState publication without
  reinterpreting Manifest-fragment positions. Its distinct checksummed request transport now
  authenticates and authorizes the claimed source before worker execution, bounds terminal typed
  responses, and retains exact authority through finite retries. Its nonblocking mutual-TLS
  carrier authenticates and node-authorizes both certificate roles before request bytes or worker
  execution. Dedicated nonblocking TCP owners now validate and retain one exact attempt across a
  separate connect deadline, bound listener admission and poll storage, expose saturated lifecycle
  metrics, and preserve TLS-before-descriptor teardown. A request-local production worker now
  reacquires and retains one exact TabletSnapshot/schema/placement/group/barrier context, encodes
  bounded Native result batches, and is composed with the authenticated receiver and TCP server in
  one reverse-safe owner. A portable multi-tablet mutable execution owner now validates one common
  plan/schema authority, retains one finite sender per tablet, preserves fresh-authority hints, and
  publishes only a complete plan-ordered result. A bounded TCP scheduler now drives one mutual-TLS
  client per tablet, rotates finite same-node addresses, enforces whole-query deadlines, and tears
  down all clients on failure. Explicit bounded rebinding now replaces retryable failures only with
  freshly constructed fragments that preserve exact logical query/tablet/group identity while
  allowing leader/position/placement/barrier authority to advance. The replicated snapshot now
  retains its committed metadata publication and database identity and binds a complete
  plan-ordered mutable fragment vector only after exact-correlating every selected resident
  publication with stable placement/group metadata and its leader barrier observation. Mutable
  fragments now enter the shared committed-node/TLS resolver, and a snapshot package returns the
  complete owning fragment and deduplicated finite route sets from that same metadata publication.
  Bound row SQL now lowers direct columns, bounded source-independent scalar outputs, and checked
  row-dependent vector-expression outputs into one schema-bound product with exact normalized
  event-time comparison and inclusive `BETWEEN` truth, canonical global order/limit including hidden
  direct-column sort helpers, and result descriptors while rejecting computed ordering or relational
  semantics. The coordinator injects constants and evaluates row expressions only after complete
  global sort/limit; computed plans carry the full source schema in ordinal order, all-constant
  queries retain a real event-time row-count anchor, and Plan Intent bytes remain unchanged.
  General checked Boolean WHERE programs now execute at the coordinator after every tablet stream
  closes and before global ordering/LIMIT; FALSE and NULL discard rows, source-dependent predicates
  carry the full schema, and exact event-time-only ranges retain worker execution. Mixed predicates
  currently remain wholly coordinator-side, and computed ordering is still rejected. Bound global
  aggregate SQL now also produces
  exact unique projection, sufficient-state definitions, event-time truth, LIMIT, and result
  descriptors for direct aggregate calls plus an exact unlimited identity input-row plan. The
  replicated Native service and packaged daemon now execute that input through the existing local/
  remote proof-revalidated mutable query plane, retry only with fresh all-group authority, and use a
  bounded transitional finalizer to emit one all-or-none Native aggregate payload. Worker-side
  sufficient-state pushdown remains an external performance step.
  The retained snapshot now joins the row product
  to the complete canonical tablet set and correlated
  current-leader authorities,
  then binds and routes the whole query without caller-built tablet authority. A move-only request
  owner now drives that routed fragment set through bounded mutual-TLS scheduling, consumes the
  all-tablet result exactly once, and retains only globally finalized Native row payloads. The
  replicated Native service now retains the correlated read authority and snapshot, lowers a real
  request, drives that owner under a finite deadline, and moves only its terminal payloads back into
  the original connection/principal/request route. The replicated database now supplies the real
  worker context by atomically reobserving the named local leader term, pinning one committed
  metadata/tablet snapshot, and exact-matching the fragment proof before execution. Retryable local
  or remote failure now discards the complete attempt, reacquires all-group authority and routes,
  exact-matches the original logical tablet/group identity, and repartitions replacement work under
  the original finite deadline. Reactor-routed query work now runs in one joined bounded slot,
  so an exact later Native cancellation can cooperatively tear down remote clients and suppress the
  whole result while mismatched requests remain isolated. Self-led fragments now execute through
  the same production proof-revalidating worker while remote fragments retain mutual-TLS
  scheduling; one
  complete coordinator merges both sets before global ordering, LIMIT, and Native publication.
  The packaged daemon now accepts an atomic native-server TLS credential
  and strict client-certificate-principal bundle, owns the immutable authority beyond its reactor
  borrow, permits non-loopback canonical IPv4 binding only in that mode, and fails closed instead of
  downgrading io_uring or partial configuration. A Linux-only gate invokes the actual client against
  the actual mutually authenticated daemon and proves APPLIED followed by MATCHING_RETRY for one
  exact append. A separate Linux-only gate now provisions
  three retained roots, starts three actual daemons with distinct mTLS identities, proves quorum
  ingest, kills the acknowledged tablet leader, proves higher-term matching retry, and reopens
  identical applied/retry state from every root. That daemon gate now also commits distinct private
  query endpoints, reuses the authenticated peer identities for a separately polled mutable-query
  listener, executes SELECT from a nonleader before leader loss, and executes it again through the
  replacement leader after failover. General provisioning, snapshot installation handling, dynamic
  endpoint/credential replacement, and broader distributed SQL remain external.

- **Scope:** map tablets to Raft groups; multiplex logical records over physical logs, threads, timers, and connections; lifecycle, placement, snapshot transfer, fairness, and safe per-group reclamation.
- **Explicit non-scope:** globally ordered logs, cross-tablet atomic transactions, general
  distributed relational/aggregate query execution, automatic rebalancing beyond scoped placement
  mechanics, and conflating physical offsets with logical indexes.
- **Required artifacts:** tablet/multi-Raft architecture and log format; scheduling/fairness/reclamation ADRs; placement metadata; production integration; deterministic cluster harness; operations and learning documents.
- **Correctness exit gate:** each tablet preserves independent ordered commit and identity through crash/leadership changes; multiplexed storage cannot cross-apply or prematurely reclaim another group's entries; snapshots and resume positions remain unambiguous.
- **Testing exit gate:** thousands of simulated groups, hot/cold skew, group creation/deletion, physical-log corruption, node loss, snapshot/install, leadership churn, starvation, and recovery are checked against per-group models.
- **Measurement exit gate:** characterize groups per node, scheduling fairness, physical-log amplification, commit tails, catch-up, snapshot bandwidth, memory, and noisy-neighbor behavior.

## Phase 16 — Distributed query execution and rebalancing

- **Feature-pass status:** bounded event-time tablet pruning, proof-bound consistency policies,
  mergeable partial aggregates, bounded exchange backpressure/cancellation, and a coordinator that
  rejects missing/failed fragments are implemented. Tablet movement enforces learner-first,
  checksummed retryable snapshot, catch-up, epoch-checked promotion, then source removal.
  Leader-linearizable admission now requires an applied Raft read barrier; bounded-stale requires an
  explicit position lag and fresh leader-commit observation; local-eventual remains distinct.
  Tablet movement now reconciles exact joint/final tablet membership with successive committed
  metadata placement epochs before locally recording target promotion or source removal.
  A canonical checksummed movement checkpoint now round-trips every phase and exact received
  snapshot prefix and reconstructs resumable ownership after semantic validation.
  A tablet-bound locked filesystem owner now installs immutable, contiguous generation envelopes
  through exact readback, file sync, no-replace rename, and directory sync, then reopens the latest
  generation without accepting renamed, foreign, corrupt, or gapped state. Every emitted promotion
  and removal step now carries a deterministic tablet/epoch/kind identity that reconstructs exactly
  after restart, plus a canonical checksummed envelope binding that identity to the exact supported
  Raft or metadata request. A tablet-bound locked pre-dispatch ledger now exact-readback/file-sync/
  no-replace-rename/directory-sync installs those immutable envelopes and rejects same-ID conflicts
  across restart. Canonical movement snapshot chunks bind each bounded payload range to the exact
  tablet, epoch, source/target, snapshot boundary, and checksums. Their locked session-bound owner
  now durably installs only a contiguous prefix, reconstructs it after restart, exact-retries
  immutable offsets, and validates the final whole-snapshot CRC. A separate canonical checkpoint
  reference now records received length and the original chunk-session epoch without rewriting the
  prefix, with a distinct checksummed generation envelope that preserves old envelope semantics;
  the locked checkpoint owner now durably dispatches both envelope magics in one contiguous
  generation sequence. Reference installation and recovery now exact-compose that generation with
  the session-bound durable prefix, reject missing/interior boundaries, and recover only the
  checkpointed prefix when a crash leaves chunks ahead. Completed recovered transfers now
  exact-decode and durably install canonical RTAS bytes with table/tablet/group/snapshot/voter
  binding, then exact-match pending source/full metadata/target identity and synchronize the Raft
  snapshot before releasing its success response. Reopen reconciliation now proves that exact RTAS/
  Raft boundary, durably installs the next ready movement checkpoint, and only then advances live
  movement. Reconfiguration construction now also resumes exact source removal from a durable
  target-promoted phase and reopens complete state terminally. Authoritative target promotion and
  source removal now install representation-preserving generation checkpoints before live phase
  adoption, while unchanged reconciliation can emit work without reopening snapshot chunks.
  Production-facing reconciliation now releases an action only with its matching durable
  pre-dispatch ledger receipt; a ledger failure after phase installation returns no dispatch and is
  exactly retryable from the new phase. That dispatch is now a sealed move-only capability, and a
  local synchronous executor releases its Raft result and outbound messages only after the existing
  physical-log synchronization boundary. Sealed dispatches also enter the bounded asynchronous
  single-owner FIFO without blocking producers or being consumed on admission rejection, and
  bounded owning group observations follow that same FIFO and can drive authoritative local
  reconciliation into the next prepared action. Exact
  retained placement and membership retries now suppress current-term/committed re-append, while an
  uncommitted prior-term match adds or reuses one empty current-term progress entry without
  duplicating the command. A separate cluster-integration target now provides canonical bounded
  remote action-request bytes and receiver-side authenticated-principal/source authorization,
  tablet/group binding, durable preparation, duplicate-safe replay, and atomic current-leader-term
  admission. Canonical response bytes and a sealed-action sender now add exact route/term/action
  correlation, explicit local-only success, advisory leader refresh, bounded attempts, and capped
  exponential backoff. Receiver admissions now retain exact correlation through asynchronous local
  durability and nonblockingly publish that sole result as the response exactly once. The
  maintained OpenSSL mutual-TLS carrier is now integrated with epoll admission and preserves
  authentication-before-protocol dispatch under partial readiness. Physical handoff now has a
  canonical one-tablet Manifest v2 projection whose exact CSEG descriptor table supplies Raft's
  aggregate part-set checksum. Canonical physical-part chunks now bind bounded payloads to the exact
  movement and CSEG identity while preserving 64-bit object lengths. Their locked durable owner
  admits an immutable contiguous prefix, reconstructs it across restart, and streams final SHA-256
  verification without assembling the CSEG. Verified final installation, restartable destination
  Manifest publication, physical-ownership-gated readiness, and durable receipt reclamation are
  implemented. Group-scoped aggregate dispatches and terminal exchanges now also have canonical
  bounded checksummed cluster request/response frames, exact route/result correlation, and an
  authenticated principal-to-source receiver that invokes the proof-revalidating worker through an
  embedding-owned service boundary. Fixed-storage fragmented/coalesced readers, move-only
  short-write ownership, and exact-correlation finite retry now cover the portable carrier
  lifecycle without silently rebinding a hinted leader. Coordinator-side compatible multi-tablet
  binding now owns one acquire-pinned Manifest v2 epoch and constructs every dispatch in exact plan
  order, rejecting mixed generations. A fail-closed execution owner now retains that epoch,
  correlates one sender per tablet, delivers each terminal exchange once, and distinguishes retry
  backoff from terminal coordinator failure. A maintained outbound OpenSSL client context now
  requires client credentials plus exact DNS or IP server-certificate identity and creates
  nonblocking mutually authenticated sessions through the same verified-fingerprint boundary.
  One outbound query-attempt carrier now authorizes that verified server principal for the exact
  target before request bytes, owns TLS readiness and bounded response framing, and applies sticky
  handshake/exchange deadlines without duplicating sender retry policy. The symmetric inbound
  carrier authenticates before reading, owns one fixed-bound request, invokes the authorized worker
  once, and owns the correlated response through all TLS short writes. Move-only nonblocking IPv4
  listener/connector owners now supply close-on-exec, `TCP_NODELAY`, exact endpoints, explicit
  connect completion, single-accept admission, and descriptor lifetime for those carriers. A
  dedicated TCP server now owns a fixed-capacity connection/poll table, finite accepts per poll,
  deadline driving, stable carrier-before-descriptor teardown, explicit overload, and real
  end-to-end mutual-TLS query serving. A matching one-attempt TCP client now validates before
  connect, binds route identity, enforces connect/TLS/exchange deadlines, and retains exact sender
  response bytes with fail-closed teardown. A pinned multi-tablet TCP scheduler now prevalidates
  every immutable node route, starts plan-ordered attempts and deadline-due retries, drives a fixed
  poll table, reports each terminal transport outcome once, closes peer attempts on query failure,
  and publishes only the complete aggregate while retaining the compatible Manifest epoch. A
  whole-query monotonic deadline and explicit idempotent cancellation now release every active
  client without exposing partial state. Retryable terminal failures may now replace the entire
  execution through finite explicit rebinding only after the caller supplies freshly proved
  authority for the same logical query and a nonregressing compatible generation; old partials are
  never carried across. Authenticated unavailable-worker responses now obtain an advisory
  leader/placement pair from a committed metadata-provider boundary and carry it through mTLS to the
  failed scheduler without treating it as authority. Coordinator aggregate binding now resolves
  active schema, placement, and immutable group identity from one committed metadata snapshot and
  derives policy admissions only from matching stable Raft observations. Selected serving-node TCP
  routes now resolve from the same committed node metadata through strict IPv4 or lowercase DNS
  endpoint grammars while retaining explicit node-specific TLS contexts. Fresh bounded DNS answer
  sets retain order and exact node authority; finite sender retries rotate their IPv4 candidates
  without expanding the retry budget or changing TLS identity. The replicated barrier owner can
  now return each leader-linearizable barrier with the exact ordered leader observation that
  validated it; a group-keyed binder joins those proofs to plan-ordered tablets through committed
  immutable group bindings. A packaged constructor requires the catalog to cover the exact
  metadata-group barrier,
  then carries that authority through compatible Manifest binding, committed route resolution,
  execution creation, and the move-only TCP lifecycle owner. A distinct canonical grouped exchange
  now carries one nullable FLOAT64 key and mergeable partial with signed-zero/NaN equivalence while
  preserving ungrouped v1 bytes; constant-storage fragmented reads and move-only short writes own
  its partial-I/O boundary. A distinct terminal-only frame closes an empty tablet without inventing
  a SQL NULL group, and its separate fixed reader/move-only cursor own fragmented reads and checked
  short writes without assuming a stream discriminator. A bounded single-owner coordinator now
  enforces contiguous exact-retry tablet streams, terminal-only empty tablets, all-tablet closure,
  and canonical cross-tablet grouping. A distinct checksummed grouped-fragment intent now binds one
  projected key index around the existing snapshot/route/proof-bound aggregate fragment without
  changing its bytes. Its authority binder reuses the complete pinned Manifest/placement/group/
  proof constructor and additionally proves the projected key FLOAT64 type. A distinct canonical
  schema-neutral vector-plan intent now covers row projection, all current aggregate operations,
  multi-key grouping, final output ordering, and LIMIT without freezing schema types or native
  physical-plan objects. A distinct group-scoped vector fragment binds that intent to complete
  snapshot/route/read-proof-shaped bytes, exact projection, and nested integrity without abusing
  aggregate v1. Its header-first reader validates every allocation-driving length before exact
  frame ownership, preserves a coalesced suffix through explicit consumed-byte reporting, and
  pairs with move-only checked short-write ownership. Its committed-authority binder now
  exact-matches read admission, placement,
  Manifest-v2 source/position, recovery schema, projection, and local aggregate operation/type
  rules before constructing an owned dispatch. A move-only compatible owner now pins one Manifest
  generation behind every plan-ordered vector dispatch under bounded aggregate projection
  ownership. A metadata-backed constructor now resolves the active schema, placement, immutable
  group, and policy-specific admission for every vector fragment from one committed catalog before
  entering that compatible owner. A leader-linearizable group-backed constructor now maps one
  canonical correlated barrier/observation vector through committed tablet-to-group bindings into
  plan order without exposing a caller-side authority join. A bounded-stale group-backed
  constructor likewise derives the commit frontier only from same-group, same-term stable
  leader/follower observations. Remote acquisition and execution remain deferred; a distinct
  all-type
  vector-result envelope now has a header-first
  bounded reader and move-only short-write cursor that preserve coalesced suffix ownership without
  allocating from unchecked lengths. A distinct node-routed vector query request now wraps one
  exact dispatch under independent header, payload, and complete integrity bounds. Its distinct
  reverse-route response exact-correlates one vector exchange or explicit failure and optional
  advisory leader hint. Header-first readers and a move-only checked cursor now own bounded
  fragmented/coalesced reads and short writes without fixed 16-MiB connection storage. Authenticated
  carrier ownership and execution remain deferred. A bounded single-owner vector coordinator now
  enforces exact per-tablet retries, contiguous sequences, terminal closure, first-failure
  ownership, count/byte-bounded retention, and plan-order release only after every tablet closes;
  it does not invent result-schema identity from nested table-schema-shaped batch bytes. A distinct
  checksummed schema-light descriptor vector now preserves SQL-owned names and exact plan-validated
  type/nullability for repeated rows and aggregate outputs. Accepted exchange v1 remains
  table-shaped and unchanged. A distinct schema-bound result exchange v2 now wraps the unchanged
  Native Protocol v1 result payload, exact-matches every ordered descriptor against the fragment
  schema, and owns bounded partial reads and short writes. A distinct v2 fragment wraps unchanged v1
  authority bytes plus the exact descriptor vector; its binder proves that shape from the same
  committed projection before publication. Its header-first reader and move-only cursor now own
  bounded partial I/O, while one compatible multi-tablet v2 owner pins the Manifest epoch and owns
  the shared schema once after proving it against every plan-ordered projection. A distinct v2
  cluster carrier now wraps that Fragment-v2 request and Result-Exchange-v2 response under separate
  versioned node routes, mandatory admitted-schema validation, exact correlation, and bounded
  partial-I/O ownership without changing v1. Its authenticated receiver now authorizes the claimed
  source and local target before one worker call, validates the complete schema-bound terminal
  stream, and applies independent response-frame and retained-byte bounds before publication.
  Single-attempt mutual-TLS client/server owners now authenticate certificate principals before
  protocol I/O, retain no fixed maximum-frame scratch, and expose results only after exact terminal
  closure under sticky deadlines. A move-only outbound TCP composite now validates the exact
  attempt before acquisition, proves nonblocking completion, binds authenticated route identity,
  enforces a separate connect deadline, and preserves TLS-before-descriptor teardown. A bounded
  inbound TCP owner now adds finite listener admission, stable connection records, metrics,
  per-carrier deadlines, and deterministic shutdown. A finite one-tablet sender now owns immutable
  whole attempts, exact schema/correlation revalidation, independent response count/byte bounds,
  advisory hints, and capped retry/backoff without authority rebinding. A schema-owning v2
  coordinator now independently validates canonical messages, arbitrates exact retries, bounds
  complete retained frame bytes, and releases plan-ordered schema-plus-results only after every
  tablet terminates. A request-local production row worker now independently reproves current local
  authority, resolves real temporal CSEG winners, materializes bounded schema-bound native batches,
  and leaves global ordering/limit unapplied. A heap-stable production owner now composes that
  worker with the authenticated receiver and bounded TCP/mTLS server under reverse-safe lifetime
  order. A portable execution owner now pins the compatible schema-bearing snapshot, drives one
  finite sender per tablet, and transfers plan plus schema-bound results only after all streams
  close. A pinned multi-tablet TCP scheduler now prevalidates complete routes, drives attempts and
  sender-authorized retries, rotates finite address candidates without rebinding authority, owns a
  whole-query deadline and cancellation, and publishes only the all-tablet result. Aggregate
  merge-state transport, authority rebinding, and process integration remain deferred. The local
  aggregate operators now share one move-only all-type mergeable state kernel, preserving wide
  exact sums, AVG counts, variance count/mean/M2, and query-accounted extrema as the foundation for
  that future transport. A distinct nested state v1 now canonically encodes the exact definition and
  sufficient state, validates integrity and bounds before variable allocation, reserves query
  credit for decoded extrema, and owns fragmented reads/short writes. A distinct schema-bound
  ungrouped envelope now binds one nested state to exact query/tablet and canonical aggregate
  ordinal/count/sequence/terminal position. It exact-matches the Fragment-v2-derived definition
  vector, validates integrity and bounds before variable decode, and owns fragmented reads/short
  writes. The compatible Fragment-v2 owner now retains that exact definition vector once after
  requiring every tablet's projected destination shapes to derive the same value, preventing final
  COUNT/AVG/variance descriptors from hiding input-type divergence. A distinct proof-revalidated
  vector-v2 aggregate worker now repeats every local authority gate, resolves real temporal CSEG
  winners, applies the event predicate, materializes the exact projection, and returns one canonical
  sufficient-state message per ungrouped definition without local final ordering, limit, or
  finalization. A bounded all-type coordinator now canonicalizes exact retry history, requires one
  complete vector from every planned tablet, merges in deterministic tablet order, and globally
  finalizes AVG/variance/exact sums only once. The retained definition authority then feeds a
  bounded all-type global finalizer that applies LIMIT and emits one canonical Native Protocol
  schema-bearing payload, including a zero-row payload for LIMIT zero. Group-key exchange,
  authenticated aggregate transport, and process integration remain deferred. A bounded
  global row finalizer now validates complete streams and native schemas, applies stable all-type
  ordering followed by LIMIT, and emits exact payload-bounded native result batches. A distinct
  group-scoped grouped dispatch now
  preserves exact Raft authority without reinterpreting ungrouped dispatch bytes. Its worker reuses
  every local authority gate, resolves real temporal CSEG winners, and emits canonical grouped
  partials or the terminal-only empty result. The grouped authority binder now directly packages
  its exact validated group and intent into that canonical dispatch, eliminating a second
  caller-side authority join. Distinct bounded grouped request/response codecs now carry that
  dispatch and explicitly discriminate correlated partial, empty-terminal, and failure payloads
  without changing ungrouped transport bytes. Fixed-storage readers and a move-only validated write
  cursor own fragmented/coalesced reads and short writes. An authenticated receiver gates worker
  invocation, validates the complete bounded contiguous result, and returns only an all-encoded
  response vector. A request-local production adapter now acquires coherent owning authority and
  invokes the real-CSEG grouped worker, and a move-only owner keeps that worker plus authenticated
  receiver at stable addresses through a complete in-process canonical request. A move-only
  mutual-TLS client/server pair now authenticates before bytes, carries the complete ordered
  multi-response stream over one already-connected nonblocking socket, and withholds client results
  until terminal closure. A move-only outbound TCP composite now validates before connect, proves
  nonblocking completion, binds route identity, enforces a separate connect deadline, and preserves
  TLS-before-descriptor teardown. A bounded TCP server now owns the listener/TLS lifetime, finite
  admission, stable connection records, metrics, and deterministic shutdown. A production owner
  composes that server with the authenticated receiver and request-local real-CSEG worker; one real
  loopback request returns the exact installed-CSEG group. A finite sender now constructs immutable
  attempts, exact-correlates only complete terminal response vectors, exposes advisory hints without
  rebinding authority, and retries whole attempts under capped backoff. A compatible grouped
  snapshot binder now derives every plan-ordered grouped dispatch under the same pinned Manifest
  epoch and exact per-tablet FLOAT64 schema proof. A portable execution owner now constructs one
  finite sender per bound tablet, delivers each complete terminal stream to the grouped coordinator
  exactly once, reports only terminal sender failure, retains the Manifest pin, and withholds the
  result until every tablet closes. A pinned multi-tablet TCP scheduler prevalidates every immutable
  route, drives plan-ordered attempts and deadline-due retries over the grouped mTLS client, rotates
  bounded address candidates under the sender budget, releases all clients on failure/deadline/
  cancellation, and publishes only the complete grouped result. A packaged leader-linearizable
  grouped constructor now acquires correlated barriers, binds committed metadata and one compatible
  Manifest epoch, proves the projected FLOAT64 key while specializing that exact aggregate owner,
  resolves its immutable authenticated routes, and returns the grouped TCP lifecycle. A distinct
  bounded-stale grouped constructor enters through canonical correlated leader/follower authority,
  preserves the proved follower route, and reuses the same specialization and lifecycle gates.
  A move-only lifecycle now retains the plan and Manifest pin through placement-backed authenticated
  remote observation acquisition, transfers only the complete canonical pair vector into grouped
  construction, and unifies metrics/cancellation across acquisition and execution. Retryable
  grouped failures now admit only a finite explicit whole-query replacement with identical logical
  shape, nonregressing Manifest generation, unchanged deadline/budget, discarded old partials, and
  cumulative metrics.
  Coordinator-side group-key ORDER BY with explicit null placement and LIMIT now runs only after
  global cross-tablet merge, providing correct bounded top-N for the supported FLOAT64 grouping
  surface. COUNT/SUM/MIN/MAX/mean/population-variance ordering now uses those globally merged states
  with a deterministic group-key tie-breaker before LIMIT. Replicated Native SQL additionally has
  a bounded row-backed coordinator baseline that feeds complete authority-proved tablet rows into
  the existing multi-key, all-type physical GROUP BY engine, including checked WHERE, computed
  keys and aggregate inputs, final expressions, global ordering, and LIMIT. A distinct bounded
  checksummed group-state frame now carries multi-key canonical tuples and all-type sufficient
  states with exact empty-tablet and partial-I/O ownership. The local grouped hash/equality/state
  table now exposes stable borrowed groups to that encoder and merges decoded scalar-key states with
  the same all-type identity rules while remaining the local SQL oracle. A bounded in-memory owner
  now retains exact retries, requires every tablet terminal, and merges in plan order before output.
  A proof-revalidated real-CSEG worker now produces bounded canonical sufficient-state streams for
  direct projected keys and aggregate inputs, and the compatible Fragment-v2 snapshot retains one
  exact cross-tablet grouped authority vector. A portable move-only execution owner now pins that
  compatible snapshot, exact-decodes complete canonical worker batches under a separate query
  memory bound, makes every partial-batch failure sticky, delegates plan-ordered all-tablet merge,
  and exposes rows only after global terminal closure. A distinct node-routed `CHDVGRP2` envelope
  now carries one grouped sufficient-state frame with exact route/correlation, independent
  integrity, authority-bound query-accounted decode, header-first fragmented reads, and move-only
  short writes. An authenticated receiver now authorizes source identity, rebinds exact current
  grouped authority before execution, and publishes only a complete bounded terminal response
  vector. Its finite sender owns immutable request bytes, canonically reconstructs the complete
  nested stream under query memory, and retries only whole attempts while treating leader hints as
  advisory. A mutual-TLS carrier now authenticates both certificate fingerprints before application
  I/O, authorizes the exact target before request write, retains complete authority and query memory,
  supports the distinct empty terminal, and clears every incomplete response prefix on failure. A
  one-attempt TCP client now retains the immutable request, complete grouped authority, and query
  resources through deadline-bound nonblocking connect, proves `SO_ERROR`, transfers ownership only
  into an authenticated TLS carrier, and closes that carrier before its descriptor on failure. A
  bounded inbound TCP owner now adds finite listener admission, stable descriptor/carrier records,
  per-poll work limits, connection metrics, and ordered shutdown while preserving the authenticated
  grouped receiver boundary. A production service adapter reacquires coherent Manifest/schema/
  placement/group/barrier authority independently for binding and real-CSEG execution, while one
  heap-stable owner preserves worker/receiver/server dependency order across public moves. A pinned
  all-tablet TCP scheduler now prevalidates complete routes, owns one finite sender and at most one
  grouped client per tablet, rotates addresses only within immutable target authority, caps waits by
  retry and query deadlines, cancels all active clients on failure, and exposes merged physical rows
  only after every terminal stream closes. A consuming finalizer now revalidates the exact raw
  key/aggregate result schema, runs pinned global order and limit through the shared physical
  operators under the coordinator's same query-memory authority, and emits bounded Native batches
  only after full success. The TCP scheduler now validates those output bounds before acquisition
  and becomes complete only after retaining the all-or-nothing Native result. Replicated
  leader-linearizable and bounded-stale constructors now acquire metadata coverage, bind one
  compatible Manifest authority and result schema, resolve authenticated committed routes, and
  transfer the complete bounded lifecycle into that scheduler. Mutable Native sufficient-state
  worker integration remains deferred. Schema-bound grouped SQL now has a separate direct-input
  lowerer that emits the exact unique projection, key/aggregate intent, event-time predicate,
  selected-output global order/limit, and typed result schema consumed by this scheduler. It fails
  closed to the row-backed path for computed/reordered/hidden semantics. Replicated SQL plan-tablet
  preparation, computed pre-group/final projection splitting, and shuffle routing remain deferred.
  The
  distinct
  bounded-stale constructor carries correlated leader/follower observations through the same
  catalog, Manifest, route, and execution gates;
  a separate canonical checksummed cluster protocol now requests one group-correlated ordered
  observation from an authenticated exact node, with receiver-side principal/source authorization
  and a durable-owner service boundary. Bounded request/response readers now validate fixed headers
  before retaining one exact frame, and a move-only cursor owns short writes. Those frames now feed
  a maintained outbound mTLS attempt that authenticates and authorizes one target
  before writing and enforces exact response correlation and deadlines. Its nonblocking TCP owner
  binds the authentication address to the route, proves connect completion, and closes the
  descriptor after TLS on failure. An accepted-socket mTLS session authenticates before request
  reads, dispatches the receiver once, and owns the response under exact deadlines. A dedicated
  bounded TCP server owns listener admission, stable session/descriptor lifetimes, metrics, and
  shutdown. A single-node acquisition owner rotates a bounded ordered address snapshot across one
  finite retry/backoff budget without changing node or request authority. A two-target owner fans
  out a selected leader/follower pair, cancels the survivor on failure, and publishes only complete
  same-term stable-membership authority. A blocking pre-poll resolver joins selected nodes to
  canonical committed numeric/DNS endpoints and exact TLS contexts under hard answer bounds.
  A canonical batch owner starts all selected group pairs before blocking, polls the global earliest
  deadline, cancels every survivor on failure, and publishes only one complete group-sorted vector.
  A placement-backed constructor derives all planned groups, selects an eligible coordinator or
  lowest nonleader follower, resolves targets once, and assigns overflow-safe correlations.
  A packaged service owner pins the plan/Manifest through remote acquisition, binds the complete
  authority vector through the metadata barrier, and transfers directly into TCP query execution
  with cross-phase cancellation. A production receiver service now acquires one coherent owning
  Manifest/schema/placement/group/barrier context per dispatch and invokes the proof-revalidating
  real-CSEG worker. A production inbound owner composes that worker with the authenticated receiver
  and bounded mTLS server; one focused loopback request returns the exact installed-CSEG aggregate.
  A focused in-process movement gate now transfers a checksummed real CSEG, installs it under a
  distinct target root, reopens its Manifest, and returns the identical grouped state from the
  promoted target through the production mTLS worker stack. Multi-process real-CSEG queries and
  broader multi-node failure validation remain deferred.
  Live DNS churn, resolver-latency policy, caching, and IPv6 remain
  qualification gaps. A focused real-mTLS gate now queries two tablets, drives one through
  checksummed learner-first movement and externally committed promotion/removal milestones, rebinds
  it to the target, and proves the complete aggregate state is identical before and after. A
  bounded Linux process gate now additionally starts three mutually authenticated daemons with
  deterministic distinct local election deadlines, commits and exactly retries one replicated
  batch, executes mutable row, global aggregate, scalar/predicate, and row-backed multi-key/all-type
  grouped SQL through safe common-leader redirect, kills that leader, repeats the retry and query
  surface through the two-node replacement quorum, shuts down cleanly, and verifies all retained
  roots. Strict cross-source durable completion FIFO, connected-peer election routing, terminal
  Native result framing, and advisory placement hints are exercised by that composition. A distinct
  canonical remote read-authority protocol and authenticated receiver now return one exact
  quorum-confirmed barrier with its correlated stable-leader observation. Bounded stream ownership
  and authenticated mutual-TLS client/server sessions now carry one exact request. Deadline-bound
  TCP connect and bounded listener/poll ownership make that exchange process-capable. Finite
  address rotation now preserves one immutable target/group/correlation under a capped retry and
  backoff budget. One all-or-nothing batch now concurrently acquires every canonical group,
  cancels all siblings on one failure, and withholds the authority vector until the whole attempt
  succeeds. A production adapter now issues only one requested configured group through the durable
  replicated barrier owner while a separate Raft poll thread drives completion. Daemon listener
  composition can now reuse the one committed private endpoint: a bounded mutual-TLS owner
  authenticates before selecting mutable-query or read-authority framing by exact frozen magic.
  The packaged daemon now owns that shared listener on the committed local endpoint, composes both
  production receivers in reverse-safe lifetime order, and polls them off the Raft thread. Native
  outbound authority integration now observes each local group, combines local barriers with one
  all-or-nothing remote batch, and re-observes the whole attempt under the Native deadline. A
  focused two-group gate completes SQL with local metadata leadership and remote tablet leadership.
  Linux multi-daemon split-leader process qualification remains. Partitions, multi-process real-CSEG
  scans, and sufficient-state multi-key shuffle also remain open. The shuffle now has a distinct
  bounded checksummed multi-key/all-type group-state frame with exact fragmented-read and short-write
  ownership plus a shared query-accounted local/worker/coordinator group-state owner. Equal-key
  sufficient-state merge, canonical all-tablet closure/order arbitration, and final row
  materialization now exist in memory, and the direct-input Fragment-v2 real-CSEG worker produces
  bounded canonical streams under one compatible snapshot-owned all-tablet key/state authority.
  Computed pre-group plan splitting, scheduler request ownership, authenticated transport/read
  authority, final SQL projection/order/limit integration, and partition routing are still missing.
  The full phase exit gate is not claimed.

- **Scope:** distributed planning/fragments/exchanges; compatible multi-tablet snapshot acquisition; explicit linearizable and bounded-stale reads; tablet movement, routing epochs, and failure retry.
- **Explicit non-scope:** general cross-tablet write transactions, silent consistency downgrade, unlimited shuffle, and topology changes that invalidate tokens without an explicit error/mapping protocol.
- **Required artifacts:** distributed query and consistency specifications; exchange protocol; snapshot-coordination and rebalancing ADRs; planner/scheduler; fault recovery; observability and learning documents.
- **Correctness exit gate:** results correspond to the declared distributed snapshot/consistency level; retries do not duplicate result fragments beyond contract; rebalancing preserves committed data, identities, retention pins, and query/subscription boundaries.
- **Testing exit gate:** differential single-node/distributed queries; partitions, node/leader loss, skew, exchange duplication/loss, movement at each state, stale routing, cancellation, and deterministic fault simulations pass.
- **Measurement exit gate:** measure scale-out efficiency, exchange bytes, coordination latency, skew/straggler impact, movement duration, foreground interference, and consistency-level costs in declared topologies.

## Phase 17 — Object-storage tiering and interoperability

- **Feature-pass status:** `chronos_tiering` provides an S3-compatible immutable put/stat/range/delete
  abstraction, a deterministic memory backend, and a production libcurl carrier with SigV4,
  TLS-by-default, conditional immutable PUT, checksum metadata, finite timeouts, and exact bounded
  range responses. Verified idempotent SHA-256 upload precedes a caller's atomic manifest-install
  callback; bounded full-object caching and authenticated large range reads are implemented.
  Manifest v1/v2 bytes are unchanged. A separate checksummed Cold Location Manifest v1 now binds
  bounded object keys and deployment store identity to exact Manifest v2 part bytes without using
  listings. A dedicated lock-protected storage owner now performs exact-readback, file-sync,
  no-replace, directory-sync installation and highest-consecutive/no-fallback recovery of add-only
  cold generations bound to an exact Manifest v2 value. One atomic shared epoch now exposes and
  reader-pins a complete compatible Manifest-v2/cold pair. A fixed checksummed pair registry makes
  already-durable component generations atomically crash-selectable and ignores higher uncommitted
  finals. A bounded snapshot-bound CSEG loader now prefers a fully validated local final and uses
  only an exact pinned cold route when that final is absent, repeating object metadata, SHA-256,
  CSEG, schema, and source validation while retaining the aggregate epoch. The distributed
  aggregate worker now invokes that loader only after its complete route, placement, Raft barrier,
  Manifest, tablet, and schema proof gates, and requires the exact aggregate Manifest owner. Pair
  recovery now authenticates Manifest metadata and the exact committed cold generation before fully
  validating locally absent CSEGs through their remote routes and creating publication state.
  Reader-pinned reclamation for WAL- and Raft-owned parts now waits only for historical aggregate
  epochs that lack an exact route, revalidates the selected pair and every remote/local image, then
  unlinks and synchronizes with idempotent retry. Cold successors may now omit a route only when a newer base
  Manifest removed that logical part, establishing the metadata transition required by later
  reader-pinned remote deletion. Memory and S3 backends now expose idempotent exact deletion; S3
  validates length/SHA-256 with HEAD and conditions DELETE on the observed ETag. Cross-layer remote
  reclamation now requires the exact selected pair to omit the logical part/route/key, waits for all
  historical aggregate readers that can expose the route, preflights every object's metadata, and
  conditionally deletes with idempotent absent retry. Restart reclamation now treats immutable
  consecutive cold-generation history as its durable garbage journal: before reader admission it
  exact-binds every historical generation to its own Manifest/catalog authority, revalidates the
  selected pair and remote metadata, and conditionally deletes routes absent from current logical
  and cold authority with idempotent retry. The S3 carrier now retries only replay-safe operations
  within a bounded capped-backoff budget, freshly signs every attempt, and force-refreshes one
  caller-supplied concurrent credential provider after authorization rejection. Bounded
  delta-seconds and strict RFC HTTP-date Retry-After hints may raise—but never exceed—the configured
  delay ceiling; bounded per-store jitter spreads retries within that same ceiling. Large uploads now
  use bounded parallel multipart workers with per-part signing/retry, sorted opaque ETag completion,
  `If-None-Match: *`, strict embedded-error-aware completion parsing, exact final HEAD verification,
  and failure-path abort. Upload admission now
  performs full Manifest-v1 CSEG validation against the exact schema, tablet, part descriptor, and
  WAL source before any remote request or manifest callback. Other query paths,
  high-concurrency stress and live provider qualification remain deferred.
  An explicit built-in environment provider now snapshots and validates the standard AWS access
  key, secret, and optional session token without implicit precedence or unsafe refresh. The
  explicit ordered chain advances only past `NOT_FOUND`, pins its first identity, and cannot fall
  through after authorization rejection. An explicit ECS/EKS-compatible container provider fetches
  bounded temporary credentials from one reviewed endpoint and refreshes before expiration without
  ambient proxy/redirect policy. An explicit EC2 provider uses IMDSv2 token, role, and credential
  requests against link-local authorities only by default, with no IMDSv1 fallback. The carrier
  disables ambient proxy variables; one bounded credential-free HTTP(S) proxy requires explicit
  configuration and cannot be bypassed by ambient `no_proxy`. Optional explicit SSE-S3 or
  SSE-KMS policy is signed on object creation and exact HEAD verification rejects missing,
  wrong-mode, or wrong-KMS-key metadata before immutable content is accepted. The signed
  conditional-write path has focused two-client races proving equal-identity convergence and
  unequal-identity single-winner/no-overwrite behavior. The
  bounded full-object LRU now supports concurrent post-install readers while keeping remote I/O and
  digest validation outside its cache-state critical section. After restart, a fresh manager can
  transactionally restore a bounded exact-metadata-preflighted catalog from caller-selected durable
  authority; its nonauthoritative cache starts empty and rebuilds on verified demand. An
  optional Apache Arrow/Parquet provider now imports and exports files through an exact
  caller-supplied schema, maps all
  current logical types, bounds source/final canonical storage, rejects corruption and mismatch,
  and atomically publishes completed exports without changing CSEG or Manifest bytes. Independent
  ecosystem fixtures and broader resource/fault qualification remain deferred; the phase exit gate
  is not claimed.

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
