# Correctness Strategy

> **Status: planned, not implemented.** This document turns the [architecture invariants](../architecture/invariants.md) into future verification obligations under [ADR 0012](../adr/0012-correctness-testing-and-performance-evidence.md). Frameworks, targets, and CI cadence remain deferred.

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
| 1 | Acknowledged durability | Failpoint crash and long stress reconcile sent/acknowledged/recovered identities for each mode; future distributed simulation removes minority replicas. |
| 2 | Complete manifest installation | Failpoint crash at every write/sync/rename/edit step plus fuzzed missing/corrupt files; oracle accepts only old or new complete generation. |
| 3 | Part immutability | Unit/property tests hash installed parts across flush, compaction, repair, and tiering; sanitizer/stress detects writes or identity reuse. |
| 4 | Per-tablet log order | Property and deterministic concurrency compare applied storage/live state after every position with a serial reference state machine. |
| 5 | No uncommitted Raft visibility | Deterministic distributed simulation tags every returned version and proves its index committed and applied despite partitions/divergent tails. |
| 6 | Stable query snapshots | Differential and deterministic concurrency hold queries across commits, seal, flush, schema edits, and compaction and compare with the captured descriptor model. |
| 7 | Compaction equivalence | Property/differential generation of overlapping base/delta versions compares every retained snapshot before and after compaction; crash tests cover install. |
| 8 | Idempotent recovery | Failpoint crash recovery-of-recovery repeats from identical images and compares manifest, applied position, files, and logical rows byte/logically. |
| 9 | Idempotent retry | Property, crash, and long stress retry matching/conflicting client batch identities around acknowledgments/restarts and compare with the identity model. |
| 10 | Integrity coverage | Codec property tests and fuzzing flip, truncate, splice, and corrupt framing/page metadata; sanitizers require bounded failure before unsafe access. |
| 11 | Safe reclamation | Deterministic concurrency pauses readers at dereferences while seal/compact/cancel/evict runs; ASan/TSan and stress verify reuse after final pin only. |
| 12 | Deterministic resume boundaries | Property/fuzz tokens, tampering, restart, retention expiry, and future topology changes; differential replay requires the same suffix or exact error. |
| 13 | Dual-time corrections | Bitemporal property/differential model covers late originals, replacements, tombstones, watermark crossings, history retention, and compaction. |
| 14 | Versioned formats | Golden fixtures, fuzzing, compatibility matrices, endian-cross checks, and unknown version/flag rejection for every durable/network release. |
| 15 | Bounded slow subscribers | Long stress and deterministic concurrency halt consumers, fill every bound, and verify explicit overflow while ingest and reclamation continue. |
| 16 | Complete mutable-row publication | Deterministic concurrency yields after every column write/publication step; poisoned memory, TSan, and scan assertions permit old boundary or complete row only. |
| 17 | Gap-free snapshot-to-stream | Differential/failpoint/concurrency tests commit at every handoff step, disconnect/restart, and compare snapshot plus deduplicated continuation with source log. |
| 18 | Optimization preserves guarantees | Differentially run reference and optimized paths through the same invariant, crash, corruption, sanitizer, and mode suites before benchmark acceptance. |

Every invariant has at least one executable future test type; implementation phases must refine these into named suites and trace them back to the invariant number.

## Initial subsystem principles

### Codecs and corruption

WAL, CSEG, manifest, checkpoint, resume-token, and network codecs require golden fixtures plus property round trips for every type and boundary. Fuzzers target lengths, offsets, version/flag combinations, checksummed and unchecked regions, compression limits, truncation, and cross-endian fixtures. A parser must validate enough framing to bound work before allocation or interpretation.

### Partial writes and crash consistency

The storage test environment can return short writes, delayed completion, sync errors, reordered completion where the platform permits it, and crashes after each state transition. WAL tail recovery, part installation, manifest edit, checkpoint advancement, and reclamation are tested separately. Corruption before a valid WAL end is an error; only a partial final record is an incomplete tail.

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
