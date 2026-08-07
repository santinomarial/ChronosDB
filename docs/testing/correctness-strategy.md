# Correctness Strategy

> **Status: partially implemented.** This document turns the
> [architecture invariants](../architecture/invariants.md) into verification obligations under
> [ADR 0012](../adr/0012-correctness-testing-and-performance-evidence.md). Phase 1 has
> unit/property-style tests, sanitizer jobs, and optional ByteReader, WAL-codec, columnar-batch,
> and columnar-append codec libFuzzer targets,
> and deterministically injected POSIX I/O failure tests. WAL v1 has an implemented physical codec,
> reusable file/directory primitives, writer, locked discovery and verification, explicit final-tail
> repair, preflight/replay passes, existing-history reopen path, and read-only inspection tool.
> The bounded WAL commit coordinator now has deterministic concurrency, backpressure, mixed-mode,
> group-limit, rotation-frontier, failure, shutdown, and metrics tests. Its deterministic
> subprocess crash harness covers real host-filesystem creation, installation, append, sync,
> group commit, rotation, process kill, recovery, repair, reopen, corruption, and locking.
> The logical schema foundation has deterministic identity, type, UTF-8, schema, successor,
> historical-reuse, and generated-lineage projection tests. The canonical immutable columnar-vector
> and owned-batch model adds boundary and deterministic generated tests for bitmaps, offsets, value
> domains, schema shape, row inspection, logical/retained-memory bounds, installation, and external
> consumption. The pure in-memory Columnar Batch v1 codec adds checked layout planning, exact
> encoding, prefix/exact borrowed decoding, explicit incomplete/invalid/unsupported/limit outcomes,
> schema binding, independently generated golden bytes, deterministic property and hostile
> corruption suites, fuzzing, and codec microbenchmarks. The pure in-memory first WAL
> application-kind layer adds SHA-256 vectors/provider integration, a canonical-preimage golden
> fixture, explicit incomplete/corruption/unsupported/limit classification, nested-batch and
> metadata validation, deterministic property/corruption suites, fuzzing, and digest/codec
> microbenchmarks. The process-local retry reservation directory adds bounded capacity, exact
> outcome-pointer, deterministic reference-model, lifetime, and concurrent single-owner tests. The
> bounded mutable-head generation adds capacity/schema rejection before WAL, complete-batch
> materialization, exact old/new snapshot boundaries, hidden row identity, seal/lifetime coverage,
> every-logical-type property cases, controlled pauses after every materialized column and hidden
> metadata, and concurrent acquire/release publication tests. The bounded tablet owner adds exact
> rows/position/retry pointer publication, global-directory pointer handoff, whole-batch rotation,
> sealed/retry backpressure, stable old epochs, a controlled pause between inner and outer
> publication, and concurrent outer-epoch readers. The single-tablet executor adds real-WAL tests
> for ordered `ASYNC` and `LOCAL_SYNC` publication and exact matching retries, plus deterministic
> in-flight rejection, WAL admission rollback, invalid durability, and accepted-WAL I/O failure
> that leaves both identity and tablet failed closed. Retained-lineage columnar recovery adds
> real-WAL whole-history preflight/replay, schema-bound generation switching, first-apply plus
> ancestor duplicate-no-op behavior, exact outcome-pointer reconstruction, repeatability,
> conflict/schema-regression/unknown-target/incomplete/unsupported classification, and continued
> live sequence assignment. Its checkpoint mode additionally seeds exact durable retry/tablet
> state, activates the selected recovery schema, skips only protected commands through the tablet
> boundary, rejects an unprotected covered command, repeats deterministically, and continues at the
> verified suffix end. Tablet admission additionally checks every frozen logical-key type,
> IEEE signed-zero/NaN equality, generated key sets, visible-generation conflicts, and replay
> conflict classification. A test-only allocator now forces each retry-directory, mutable-head,
> tablet preparation, deduplication, and rotation allocation to fail in turn, and verifies the
> expected post-WAL in-memory path is allocation-free. Manifest/CSEG installation, selected-state
> recovery, checkpoint-aware WAL replay/reopen/reclamation, and deterministic sealed-head-to-CSEG
> conversion now add golden/property/corruption, filesystem fault, sanitizer, installation,
> external-consumer, and benchmark evidence. Bounded flush scheduling, aggregate Manifest/head
> publication, and receipt-authorized generation retirement add deterministic interleaving,
> hostile-identity, lifetime, and sanitizer coverage. A subprocess SIGKILL matrix now stops after
> every part/Manifest write, file sync, rename, and directory sync, then proves repeated recovery
> selects one complete generation and cleans only recognized temporaries. Retry retention and
> routing/admission remain unimplemented.
> A separate installed flush-harness smoke test requires exact durable generation/part/retry state,
> bounded raw foreground samples, byte-identical repeated Manifest selection, and exact repeated WAL
> suffix replay before marking its measurement artifacts valid.
> The Manifest columnar startup integration now uses real generation-1 through generation-3 database
> images to prove lock exclusion, part/retry restoration, covered no-op verification, uncovered row
> replay, recognized-temporary cleanup, aggregate publication, exact reopen sequence, invalid caller
> seed rejection, missing-tablet failure, disabled-by-default WAL retention, corruption-before-
> deletion failure, exact covered-segment cleanup, and logical convergence across repeated opens.
> The Phase 8 SQL v1 reference engine adds bounded hostile-byte lexer/parser tests and fuzzers,
> owned-AST grammar goldens, exact literal/calendar boundaries, schema-generation-stable binding,
> ambiguous/type/grouping failures, exact decimal properties, NULL/NaN ordering, aggregate
> reference comparisons, system-time/LATEST/ASOF execution, deterministic random small-database
> temporal-join models, CREATE TABLE/INSERT validation, stable EXPLAIN goldens, measured ANALYZE
> counters, installation checks, and sanitizer coverage. The first Phase 9 vector-chunk foundation
> adds identity-free canonical owners, explicit-selection unit/property/fuzz coverage, retained-byte
> bounds, checked traversal benchmarks, and installed-consumer checks. The query resource context
> adds exact RAII accounting, a fixed-seed reservation model, concurrent saturation/cancellation
> tests under ThreadSanitizer, resource microbenchmarks, and consumer checks. The first physical
> operator adds accounted-lifetime, end/error/cancellation, hostile predicate, fixed-seed
> chunk-boundary scalar-truth differential, fuzz, and selection-compaction benchmark coverage.
> Stable column-subset projection adds bounded/order/range failures, zero-column cardinality,
> exhaustive eight-row selection-mask cell preservation, hostile fuzz inputs, ownership-release
> measurement, and installed-consumer linkage.
> Global LIMIT adds zero/exact/partial/oversized boundaries, empty-chunk progress, eager unpulled
> credit release, deterministic scalar-prefix comparison, truncation fuzzing, batched measurement,
> and installed-consumer linkage.
> The bounded physical pipeline adds sequential shape-transition failures, exact runtime source
> shape and query-identity enforcement, finite retained configuration, composed filter/subset/LIMIT
> differential execution across randomized selections and chunk boundaries, hostile plan fuzzing,
> sanitizer coverage, and plan-overhead measurement. Bound-SQL/full scalar-engine plan differential
> and distributed query harnesses remain planned for their roadmap phases.
> Lifetime-pinned vector backing adds missing/shape/underreporting failures, caller-handle and
> accounted-credit destruction checks, conservative projection accounting, direct-versus-backed
> fixed-seed equivalence, backed-path fuzzing, sanitizer coverage, and attachment-overhead
> measurement before the first storage scan is admitted.
> Allocation-free CSEG read planning adds exact raw/compressed/synthesized/system byte accounting,
> borrowed-plan and foreign-reader failures, direct-versus-planned deterministic equivalence, a
> dedicated zero-allocation/failure-injection executable, plan-then-read fuzzing, sanitizer coverage,
> and planning-overhead measurement before scan integration.
> The pinned CSEG source adds pre-open and pre-decode admission, raw/decompressed pin lifetime,
> source-destruction and LIMIT ownership, cross-query/cancellation behavior, multi-granule
> deterministic equivalence, exhaustive allocation failure, hostile scan fuzzing, sanitizer
> coverage, and raw/Zstandard pull measurement.
> Snapshot-bound multi-part CSEG scanning adds independent part/granule range models,
> corrupted-pruned-file and corrupted-pruned-page evidence, exact epoch/image-order rejection,
> empty-plan validation, exhaustive retained-allocation failure injection, predicate-aware decoder
> fuzzing, and separate metadata-plan versus selected-pull measurements. It explicitly excludes
> mutable heads and non-event-time SQL filtering from its correctness claim.
> The single-publication mutable-head source adds byte-per-row-to-bitmap canonicalization,
> native-to-little-endian rebased offsets, exact old-snapshot boundary checks, schema-successor NULL
> synthesis, every-frozen-type and varied-boundary properties, exhaustive source/pull allocation
> failure, hostile projection fuzzing, sanitizer coverage, and isolated materialization
> measurements. Exact event-time head construction now adds projection-aware helper materialization
> and removal, forced-boundary/empty-progress truth, independent point properties, allocation
> failure, hostile fuzzing, and selective materialization measurement. It explicitly excludes
> hidden row versions, multi-head/part composition, and non-event-time SQL filtering from its
> correctness claim.
> Exact timestamp-range vector filtering adds direct open/closed signed-domain edge tests, NULL and
> empty-range semantics, sparse-selection/chunk-boundary scalar properties, invalid type/shape/query
> ownership failures, bounded-plan validation, hostile vector/plan fuzzing, sanitizer coverage,
> compaction microbenchmarks, and installed-consumer linkage. Conservative storage pruning remains
> separate from exact truth in the low-level source. Aggregate snapshot CSEG construction now
> copies identical bounds into both stages, retains and removes an omitted event-time helper,
> checks the effective projection against both limits, and adds independent exact-result,
> allocation-failure, fuzz, and selected-pull evidence.
> Accounted source-column output materialization adds reverse and duplicate output positions,
> independent canonical owners, sparse-to-identity compaction, empty-progress and zero-column
> cardinality, every-frozen-type cell comparison, exact pre-allocation byte limits, exhaustive
> allocation failure, hostile physical-plan fuzzing, sanitizer coverage, installed-consumer
> linkage, and isolated dense/sparse output measurement. Mixed typed-constant output adds exact
> caller order, nonnullable constants, all-NULL typed vectors, direct canonical materialization for
> all 18 frozen logical types, sparse/empty selection behavior, checked retained configuration and
> output admission, exact physical-plan shapes, exhaustive allocation failure, hostile plan
> fuzzing, sanitizer coverage, external-consumer linkage, and fixed/string/NULL measurement.
> Computed expressions and bound-SQL lowering remain outside this claim.

## Test types

- **Unit:** small deterministic examples and boundary behavior.
- **Property:** generated valid structures/operations checked against algebraic or model properties.
- **Fuzz:** coverage-guided hostile bytes or operation sequences, always under applicable sanitizers.
- **Differential:** compare independent implementations or a reference model.
- **Failpoint crash:** interrupt every durable transition and recover from the resulting image.
- **Sanitizer:** AddressSanitizer, UndefinedBehaviorSanitizer, and ThreadSanitizer where feasible.
- **Deterministic concurrency:** controlled schedules, virtual clocks, and explicit yield points.
- **Deterministic distributed simulation:** virtual Raft nodes, disks, networks, clocks, and reproducible fault traces.
- **Long-running stress:** sustained mixed workloads, churn, resource pressure, and periodic invariant audits.

Every randomized failure prints the random seed, generator/scenario version, minimized operation/input sequence when available, platform/build configuration, and an exact reproduction command. Crash failures retain the pre-crash durable image or a deterministic recipe for recreating it. A failure without enough information to reproduce is itself a test-harness defect.

## Invariant verification matrix

| # | Invariant | Primary future test types and oracle |
| --- | --- | --- |
| 1 | Acknowledged durability | WAL v1 failpoint crash and long stress reconcile sent/fully-written/synchronized/acknowledged/recovered identities for each mode at every installation, append, sync-frontier, and repair boundary; future distributed simulation removes minority replicas. |
| 2 | Complete manifest installation | Failpoint crash at every write/sync/rename/edit step plus fuzzed missing/corrupt files; oracle accepts only old or new complete generation. |
| 3 | Part immutability | Unit/property tests hash installed parts across flush, compaction, repair, and tiering; sanitizer/stress detects writes or identity reuse. |
| 4 | Per-tablet log order | Property and deterministic concurrency compare applied storage/live state after every position with a serial reference state machine. |
| 5 | No uncommitted Raft visibility | Deterministic distributed simulation tags every returned version and proves its index committed and applied despite partitions/divergent tails. |
| 6 | Stable query snapshots | Differential and deterministic concurrency hold queries across commits, seal, flush, schema edits, and compaction and compare with the captured descriptor model. |
| 7 | Compaction equivalence | The append-only CSEG oracle fully validates both sorted part sets, bounds active decoded granules, rejects duplicate physical tuples, and compares every user/system cell and multiplicity; later version-aware compaction adds retained-snapshot and crash-install coverage. |
| 8 | Idempotent recovery | WAL tail repair and later manifest recovery crash recursively, repeat from identical images, and compare classification, segment bytes, applied position, files, and logical rows byte-for-byte and logically. |
| 9 | Idempotent retry | Property, crash, and long stress retry matching/conflicting client batch identities around acknowledgments/restarts and compare with the identity model. |
| 10 | Integrity coverage | WAL v1 golden/property/fuzz suites flip, truncate, splice, and corrupt every segment/record field, payload, padding, and CRC; later page codecs receive equivalent coverage, and sanitizers require bounded failure before unsafe access. |
| 11 | Safe reclamation | Deterministic concurrency pauses readers at dereferences while seal/compact/cancel/evict runs; ASan/TSan and stress verify reuse after final pin only. |
| 12 | Deterministic resume boundaries | Property/fuzz tokens, tampering, restart, retention expiry, and future topology changes; differential replay requires the same suffix or exact error. |
| 13 | Dual-time corrections | Bitemporal property/differential model covers late originals, replacements, tombstones, watermark crossings, history retention, and compaction. |
| 14 | Versioned formats | WAL v1 and every later durable/network release maintain golden fixtures, fuzzing, compatibility matrices, endian-cross checks, and deterministic unknown version/type/flag rejection. |
| 15 | Bounded slow subscribers | Long stress and deterministic concurrency halt consumers, fill every bound, and verify explicit overflow while ingest and reclamation continue. |
| 16 | Complete mutable-row publication | Deterministic concurrency yields after every column write/publication step; poisoned memory, TSan, and scan assertions permit old boundary or complete row only. |
| 17 | Gap-free snapshot-to-stream | Differential/failpoint/concurrency tests commit at every handoff step, disconnect/restart, and compare snapshot plus deduplicated continuation with source log. |
| 18 | Optimization preserves guarantees | Differentially run reference and optimized paths through the same invariant, crash, corruption, sanitizer, and mode suites before benchmark acceptance. |

Every invariant has at least one executable future test type; implementation phases must refine these into named suites and trace them back to the invariant number.

## Initial subsystem principles

### Codecs and corruption

WAL, Columnar Batch, WAL application commands, CSEG, manifest, checkpoint, resume-token, and network codecs require golden fixtures plus property round trips for every type and boundary. Fuzzers target lengths, offsets, version/flag combinations, checksummed and unchecked regions, compression limits, truncation, and cross-endian fixtures. A parser must validate enough framing to bound work before allocation or interpretation. The Columnar Batch v1 decoder implements this order by authenticating the fixed header before trusting counts or total length, enforcing configured and format bounds before descriptor allocation, authenticating the complete batch before value interpretation, and then validating canonical placement and value domains. `COLUMNAR_APPEND` first bounds the application and command headers, validates the exact embedded batch, checks duplicated metadata, and finally recomputes its canonical SHA-256 request digest.

### Partial writes and crash consistency

The storage test environment can return short writes, delayed completion, sync errors, reordered completion where the platform permits it, and crashes after each state transition. WAL tail recovery, part installation, manifest edit, checkpoint advancement, and reclamation are tested separately. The exact [WAL v1 incomplete-tail rule](../formats/wal-v1.md#clean-end-incomplete-final-tail-and-corruption) permits only a short suffix at the verified end of the highest segment. Complete-record checksum failure and any non-final truncation are corruption, never a truncation hint.

The implemented POSIX and WAL-writer boundaries have deterministic syscall substitution for
`EINTR`, short reads and writes, EOF, hard errors, zero-progress writes,
metadata/truncate/synchronization failures, rename support and collisions, lock contention, close
behavior, ordered initial segment installation, partial-append poisoning, explicit sync failure, and
pre-rotation sync failure. Writer tests also cover configuration bounds before mutation, strict
new-directory entry classification, identity-generation ordering, configured payload admission,
small-target rotation, sequence exhaustion diagnostics, and invalid internal-position poisoning.
Host-filesystem integration tests cover directory-relative lifecycle, entry types without symlink
following, same-process and cross-process locking, complete record append, sync frontiers, and
two-segment rotation. Recovery tests cover strict discovery, segment/record continuity, corruption,
both accepted incomplete-tail shapes, read-only classification, explicit and repeated repair,
temporary cleanup, synchronization failures, whole-log preflight before replay, deterministic replay
order, exact reopen positions, and lock lifetime. The subprocess harness now interrupts real
host-filesystem operations after selected successful syscalls, reconciles parent-observed
acknowledgments with recovered records, and checks repeated crash-image recovery. It remains process
termination evidence rather than physical power-loss or storage-device qualification.

The commit-coordinator suite gates its sole worker before admission so count/byte limits and batch
composition do not depend on scheduler timing. It checks concurrent producer admission against
physical sequence order, `ASYNC` completion while a mixed group is blocked in sync, shared
`LOCAL_SYNC` frontiers, count/byte/zero-delay triggers, rotation-provided durability, nonterminal
validation errors, terminal append/sync propagation, graceful draining shutdown, and metric
conservation. ThreadSanitizer remains required evidence for this shared-state boundary where the
host toolchain supports it.

All current libFuzzer targets share a deterministic CI smoke runner and checked-in seed corpora. The
runner copies seeds to temporary writable directories, fixes run count, PRNG seed, maximum input
length, timeout, and entropic scheduling, and retains crash artifacts without modifying the source
corpus. Durable-format harnesses additionally execute a structurally valid fixture on every callback
before input-directed mutation. This guarantees basic success/corruption-path reachability but does
not turn a 1,000-run CI smoke into a sustained fuzz campaign; longer campaigns must record compiler,
sanitizers, corpus identity, flags, duration, and artifacts.

### WAL v1 implementation gate

The WAL implementation phases must introduce named suites covering:

- **golden format:** empty and multi-segment files, every fixed field, application envelope,
  alignment class, maximum accepted length, and cross-endian byte equality;
- **structural property:** independent encode/decode round trips, sequence continuity, exact CRC
  ranges, checked size arithmetic, and records that end exactly at the segment limit;
- **corruption and fuzz:** every bit/field/padding region, hostile length/complement combinations,
  unknown versions/types/kinds/flags, gaps, duplicate/spliced segments, and bounded allocation/read;
- **append failpoint:** every short-write prefix and hard error, with proof that the poisoned writer
  never appends a later record;
- **installation crash:** before/after WAL-directory creation and parent sync, temporary header
  write, file sync, rename, directory sync, prior-segment sync, recovery's writer-startup namespace
  barrier, and first successor append;
- **acknowledgment oracle:** reconcile requested/effective mode and sent, complete-write, sync-frontier,
  acknowledged, recovered, and applied identities; covered failures may lose no `LOCAL_SYNC` identity;
- **tail repair:** every final-header/payload/trailer truncation, forbidden middle/full-CRC cases,
  synchronization failures, repeated repair, and crash during repair; and
- **whole-log preflight/replay:** unknown semantic support fails before apply, repeated recovery is
  idempotent, and no state is query-visible before complete replay.

Fixtures and randomized failures record WAL format, generator version, seed, platform/filesystem,
fault point, durability mode, and an exact reproduction command. The physical codec implements the
golden-format, structural-property, corruption/truncation, and coverage-guided fuzz foundations. The
writer adds deterministic installation ordering, append failure, sync failure, and rotation
coverage. Discovery, physical scan, corruption classification, explicit repair, preflight/replay,
reopen, and inspector suites are implemented. The subprocess harness implements process-kill image
recovery and acknowledgment reconciliation across initial/successor installation, append,
synchronization, grouped completion, repair, reopen, corruption, locking, every covered-prefix WAL
unlink, and the reclamation directory-sync boundary. Power-cut execution,
filesystem/device qualification, and crashing recovery inside every one of its own synchronization
steps remain future evidence.

### Recovery idempotence and manifest installation

Recovery runs repeatedly and can itself crash. The oracle compares chosen manifest generation, referenced part hashes, applied commit position, identity table, logical rows, and orphan cleanup. Installation tests demand exactly old-complete or new-complete visibility.

### Compaction and snapshots

A generated bitemporal model supplies base/delta parts, duplicate retries, replacements, tombstones, and retention pins. All current and `FOR SYSTEM_TIME AS OF` snapshots match before and after compaction. Long queries pin heads/parts while flush and compaction publish new generations.

The implemented append-only lifetime slice assigns one shared pin to each selected immutable part
and carries it through tablet-only and Manifest publication epochs. A regression holds an older
same-Manifest tablet epoch plus a snapshot-loaded image across compaction and requires reclamation
to remain pending until both release. Snapshot loading after a newer generation is selected repeats
exact file/content/WAL/schema validation, while the query adapter proves the final chunk retains its
image, epoch memory charge, and reclamation pin after source destruction.

### Query execution

The scalar executor is the primary ChronosDB oracle for vector execution. Random plans vary chunk size, selection density, morsel scheduling, NULL, overflow, NaN, ASOF ties, grouping, and cancellation. The conventional supported SQL intersection is also compared with pinned DuckDB or PostgreSQL test versions; ChronosDB-specific temporal/live behavior uses its own model.

### Subscription handoff

The harness inserts a commit at every step of the [handoff protocol](../product/live-query-semantics.md#handoff-protocol), then disconnects during snapshot, buffer transfer, delivery, and token persistence. After sequence deduplication, no matching post-boundary source change may be absent. Slow-consumer tests prove bounded memory and explicit resume/expiry outcomes.

### Queue lifecycle

SPSC tests cover empty/full transitions, counter wraparound, publication/reclamation ordering, producer/consumer stalls, batch lifetime, queue-full action, shutdown, and ownership transfer. The selected implementation includes a machine-checkable or reviewable happens-before and progress argument; “lock-free” claims are tested against the claimed operations.

### Raft later

Before production integration, virtual-time simulation explores elections, replication, membership, snapshot installation, crashes, disk errors, partitions, duplication, delay, and reordering. Traces are seed-replayable and shrinkable. Safety checkers cover election safety, log matching, leader completeness, committed apply order, read-mode histories, and absence of uncommitted visibility.

## Test review gate

A phase cannot exit because code compiles or a happy-path test passes. The review reports exact commands, seeds/corpora, configurations, sanitizer coverage, skipped/unsupported checks, failures, and retained reproducers. Performance results are inadmissible until applicable correctness suites pass under the same semantic mode.
