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
> outcome-pointer, deterministic reference-model, lifetime, and concurrent single-owner tests. WAL
> submission orchestration, recovered/tablet retry state, retry retention, replay, and mutable
> heads remain unimplemented. Query
> and distributed harnesses also remain planned for their roadmap phases.

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
| 7 | Compaction equivalence | Property/differential generation of overlapping base/delta versions compares every retained snapshot before and after compaction; crash tests cover install. |
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
synchronization, grouped completion, repair, reopen, corruption, and locking. Power-cut execution,
filesystem/device qualification, and crashing recovery inside every one of its own synchronization
steps remain future evidence.

### Recovery idempotence and manifest installation

Recovery runs repeatedly and can itself crash. The oracle compares chosen manifest generation, referenced part hashes, applied commit position, identity table, logical rows, and orphan cleanup. Installation tests demand exactly old-complete or new-complete visibility.

### Compaction and snapshots

A generated bitemporal model supplies base/delta parts, duplicate retries, replacements, tombstones, and retention pins. All current and `FOR SYSTEM_TIME AS OF` snapshots match before and after compaction. Long queries pin heads/parts while flush and compaction publish new generations.

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
