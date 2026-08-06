# ChronosDB Documentation

ChronosDB is pre-alpha and in its architecture phase. These documents define intended contracts and implementation constraints; they do not imply that the described engine exists.

## Product

- [Vision](product/vision.md): problem, users, differentiators, principles, and success criteria.
- [Workloads](product/workloads.md): representative financial and observability data models and query patterns.
- [Data model](product/data-model.md): typed tables, physical policies, row versions, retention, and late-data classification.
- [SQL v1](product/sql-v1.md): bounded grammar and deterministic expression/query semantics.
- [Consistency and durability](product/consistency-and-durability.md): acknowledgment modes, snapshots, future read modes, and idempotency.
- [Live queries](product/live-query-semantics.md): gap-free historical-to-live handoff, change records, windows, and resumption.

## Architecture

- [Overview](architecture/overview.md): planes, components, data flows, and accepted versus deferred design areas.
- [Invariants](architecture/invariants.md): correctness properties every implementation must preserve.
- [WAL recovery](architecture/wal-recovery.md): accepted segment lifecycle, durability boundaries,
  failure classification, tail repair, and replay design.
- [Columnar ingestion](architecture/columnar-ingestion.md): schema-bound batches, WAL append command,
  retry interaction, ordered replay, and failure ownership.
- [Mutable-head publication](architecture/mutable-head-publication.md): single-writer ownership,
  batch-atomic visibility, snapshot pins, sealing, and future flush handoff.
- [Non-goals](architecture/non-goals.md): deliberately excluded or deferred scope.
- [Glossary](glossary.md): canonical terminology.

## Durable formats

- [WAL v1](formats/wal-v1.md): authoritative single-node WAL directory, segment, record, integrity,
  limits, and compatibility specification; its physical codec, writer, durability coordinator,
  locked discovery, verification, explicit tail repair, replay interface, and existing-history
  reopen path are implemented.
- [Columnar batch v1](formats/columnar-batch-v1.md): accepted self-describing immutable batch bytes,
  logical type registry, integrity coverage, and canonical validation rules; the standalone
  in-memory codec is implemented independently of WAL framing.
- [CSEG v1](formats/cseg-v1.md): accepted immutable sorted part layout, schema and system columns,
  granules, independently checked pages, compression, ordering, and compatibility rules;
  authoritative constants, nominal part identity, and checked canonical layout planning are
  implemented together with bounded raw/Zstandard page compression and the canonical metadata
  directory codec plus deterministic PLAIN payload encoding, stored-page CRC composition, and
  borrowed/owned physical decoding. Canonical owned part composition and borrowed prefix/exact
  structural decoding validate every stored page and alignment byte.
- [Production dependencies](dependencies/README.md): maintained external-library boundaries,
  version sources, licenses, and update/security ownership.

## Delivery

- [Roadmap](roadmap.md): implementation phases and evidence-based exit gates.
- [Building](development/building.md): supported toolchains, presets, tests, and sanitizer workflows.
- [Tooling](development/tooling.md): formatting, static analysis, dependencies, and the CI matrix.
- [Architecture Decision Records](adr/README.md): decision process and index.
- [ADR template](adr/template.md): required structure for new decisions.

## Verification and measurement

- [Correctness strategy](testing/correctness-strategy.md): implemented foundation checks and the
  future test taxonomy mapped to architecture invariants.
- [WAL crash harness](testing/wal-crash-harness.md): subprocess protocol, real-syscall crash
  boundaries, durability/recovery oracle, deterministic matrices, and evidence limitations.
- [Benchmark contract](benchmarks/benchmark-contract.md): mandatory run metadata, metrics, and comparison rules.
- [WAL benchmarks](benchmarks/wal-benchmarks.md): production-path WAL measurement harness, safety
  controls, correctness gate, artifact schema, and evidence limitations.
- [ChronosBench](benchmarks/chronosbench.md): planned correctness-checked workload scenarios.

## Reviews

- [Phase 1 foundation review](reviews/phase-1-foundation-review.md): adversarial audit and validation
  evidence for the implemented build and common binary foundation before WAL design.
- [WAL v1 exit review](reviews/wal-v1-review.md): storage, concurrency, recovery, portability,
  testing, installation, and measurement audit of the implemented WAL lifecycle.

## Learning

- [Project foundation](learning/project-foundation.md): rationale and extension guide for the Phase
  1A build graph.
- [Common binary foundations](learning/common-binary-foundations.md): ownership, bounds, encoding,
  failure, and CRC32C contracts for the Phase 1B primitives.
- [Durable POSIX I/O foundations](learning/posix-io.md): owned file/directory/lock handles,
  explicit-offset transfer loops, synchronization, atomic rename, fault injection, and portability.
- [WAL design](learning/wal-design.md): rationale, lifecycle, ownership, failure model, tradeoffs, and
  validation methodology for the WAL foundations.
- [WAL recovery implementation](learning/wal-recovery.md): scan passes, repair policy, replay-sink
  contract, reopening, bounded-memory behavior, and failure handling.
- [WAL commit coordination](learning/wal-commit-coordinator.md): bounded admission, single-worker
  ownership, mixed durability, group commit, shutdown, metrics, and synchronization argument.
- [Columnar ingestion design](learning/columnar-ingestion-design.md): batch/schema boundaries, WAL
  command flow, retry identity, publication proof, recovery model, and implementation questions.
- [Logical schema foundation](learning/schema-foundation.md): implemented UUID/identity/type/schema
  APIs, lineage evolution, historical projection, ownership, failure, and validation contracts.
- [Columnar memory model](learning/columnar-memory-model.md): implemented immutable borrowed/owned
  vectors, canonical buffers, schema-shaped batches, bounds, inspection, and validation contracts.
- [COLUMNAR_APPEND command codec](learning/columnar-append-command.md): implemented envelope,
  identity, SHA-256 preimage, owned encoding, borrowed decoding, and schema-binding contracts.
- [Retry reservation directory](learning/retry-reservation-directory.md): implemented bounded live
  identity state machine, ownership, linearization, failure behavior, and model-test contract.
- [Mutable-head generation](learning/mutable-head.md): implemented fixed-capacity generation,
  two-phase append ownership, batch-atomic release/acquire publication, owning snapshots, hidden row
  identity, failure behavior, sealing, and measurement boundary.
- [Tablet publication](learning/tablet-publication.md): implemented bounded generation rotation,
  joint rows/position/retry publication, owning tablet snapshots, retry-outcome handoff,
  backpressure, memory ordering, and measurement boundary.
- [Columnar append execution](learning/columnar-append-execution.md): implemented blocking
  single-tablet composition of canonical bytes, global retry reservation, bounded WAL durability,
  batch-atomic tablet publication, exact outcome commit, and fail-closed ownership.
- [Columnar append recovery](learning/columnar-append-recovery.md): implemented retained-lineage
  whole-WAL preflight/replay into fresh schema-bound tablet and retry state, ancestor duplicate
  handling, failure isolation, deterministic reopen, and continued writer ownership.

Future format, protocol, subsystem, operations, and learning documents should be linked here when they are added by their corresponding roadmap phase.
