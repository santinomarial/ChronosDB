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
- [Manifest installation and checkpointing](architecture/manifest-installation-and-checkpointing.md):
  accepted Phase 6 part/manifest durability ordering, head replacement, recovery, and WAL coverage.
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
  structural decoding validate every stored page and alignment byte; bounded full validation adds
  system-row semantics, event-time extrema, global physical ordering, and exact schema binding.
  Metadata-authenticated projected granule reads independently validate requested user pages plus
  every system page and synthesize nullable successor-schema tails. Complete read-only inspection
  validates all schema-independent semantics and reports owned value-free metadata.
- [CSEG v2](formats/cseg-v2.md): accepted temporal-history system registry for WAL/Raft source,
  logical identity, correction/tombstone semantics, receive/system time, and checked canonical
  layout; strict metadata/full-part codecs, bounded complete semantic validation, and schema-aware
  projected granule reading are implemented together with bounded single-lineage current/as-of
  winner resolution, while Manifest v2 and multi-part/vector integration remain in progress.
- [Manifest v1](formats/manifest-v1.md): accepted immutable database-wide generation bytes,
  installed-name grammar, tablet/part/retry recovery state, and checkpoint-aware WAL suffix contract;
  implementation is pending.
- [Manifest v2](formats/manifest-v2.md): accepted source-neutral WAL/Raft tablet boundaries, exact
  CSEG format/source/content binding, generalized retries, optional global WAL reclamation, and
  checked canonical layout with a strict checksummed codec; transition validation, exact CSEG
  admission, and installation remain in progress.
- [Resume Token v1](formats/resume-token-v1.md): authenticated live-subscription resume identity,
  compatibility, limits, and rejection rules.
- [Materialized View Checkpoint v1](formats/materialized-view-checkpoint-v1.md): bounded exact
  window/aggregate state, committed progress, IEEE-754 preservation, and CRC32C rejection rules.
- [Multiplexed Raft Persistent-State Record v1](formats/multiplexed-raft-log-v1.md): checksummed,
  group-tagged full-state records plus the implemented segmented append/sync/recovery envelope.
- [Raft Tablet Command v1](formats/raft-tablet-command-v1.md): exact committed
  `COLUMNAR_APPEND` payload, group/index identity, ordering, and retained-log recovery contract.
- [Metadata Command v1](formats/metadata-command-v1.md): canonical checksummed metadata-group
  commands for nodes, schemas, placements, and retention.
- [Temporal Mutation Command v1](formats/temporal-mutation-v1.md): checksummed columnar originals,
  corrections, replacements, tombstones, and dual-time metadata.
- [Raft Membership Command v1](formats/raft-membership-command-v1.md): canonical joint and final
  configuration entries with old/new quorum semantics.
- [Production dependencies](dependencies/README.md): maintained external-library boundaries,
  version sources, licenses, and update/security ownership.

## Network protocols

- [Native Protocol v1](protocol/native-v1.md): fixed checksummed 1.0 framing plus negotiated 1.1
  subscription delivery, finite limits, compatibility, and rejection rules.

## Delivery

- [Roadmap](roadmap.md): implementation phases and evidence-based exit gates.
- [Building](development/building.md): supported toolchains, presets, tests, and sanitizer workflows.
- [Tooling](development/tooling.md): formatting, static analysis, dependencies, and the CI matrix.
- [Deferred validation](development/deferred-validation.md): exact feature-pass validation and
  integration ledger by subsystem and category.
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
- [Phase 10 native-network baseline](benchmarks/native-network-phase-10.md): clean-commit portable
  codec/queue and Ubuntu epoll measurements with raw repetitions and applicability limits.
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
- [Sealed-head flush scheduling](learning/sealed-head-flush-scheduling.md): implemented bounded
  shard-to-storage reservations, immutable pin ownership, retry-safe consumer leases, receipt-gated
  completion, queue observability, and synchronization proof.
- [Columnar append execution](learning/columnar-append-execution.md): implemented blocking
  single-tablet composition of canonical bytes, global retry reservation, bounded WAL durability,
  batch-atomic tablet publication, exact outcome commit, and fail-closed ownership.
- [Columnar append recovery](learning/columnar-append-recovery.md): implemented retained-lineage
  whole-WAL preflight/replay into fresh schema-bound tablet and retry state, ancestor duplicate
  handling, failure isolation, deterministic reopen, and continued writer ownership.
- [Durable temporal history](learning/durable-temporal-history.md): implemented mutation bytes,
  committed physical-to-scalar application, atomic in-memory history, whole-WAL recovery, ownership,
  failure behavior, complexity, and remaining integration boundaries.
- [Manifest columnar startup recovery](learning/manifest-startup-recovery.md): implemented
  caller-catalog selected-state validation, Manifest-derived durable-prefix recovery, temporary
  cleanup, optional checkpoint-covered WAL reclamation, aggregate publication, and owning startup
  lifetime.
- [CSEG v1 storage](learning/cseg-storage.md): implemented layout, compression, metadata/page/part
  codecs, layered validation, projected reads, inspection, ownership, failure, and measurement
  contracts.
- [Manifest v1 codec](learning/manifest-codec.md): implemented nominal values, checked layout,
  canonical owned encoding, borrowed decoding, trust ladder, failure classes, and evidence boundary.
- [Append-only CSEG compaction](learning/append-only-compaction.md): accepted replacement boundary,
  structural Manifest authority, and implemented bounded full-row equivalence oracle.
- [Database storage publication](learning/database-storage-publication.md): implemented aggregate
  Manifest/head ownership, monotonic tablet refresh, atomic head-to-part replacement, snapshot
  lifetime, fail-closed behavior, and memory-ordering evidence.
- [Durable sealed-head flush coordination](learning/sealed-head-flush-coordinator.md): implemented
  queue-to-CSEG-to-Manifest orchestration, durable-resume handling, exact retirement, ownership,
  failure boundaries, and observability.
- [SQL v1 scalar reference engine](learning/sql-v1-reference-engine.md): implemented bounded lexer,
  AST, schema-stable binding, exact scalar semantics, temporal joins, aggregates, DDL/INSERT
  materialization, EXPLAIN, ownership, failure, fuzzing, and measurement boundaries.
- [Vector chunk foundation](learning/vector-chunk-foundation.md): implemented identity-free
  canonical physical owners, explicit order-preserving selections, checked chunk bounds, ownership,
  failure, fuzzing, and measurement boundaries for the first Phase 9 increment.
- [Query resource control](learning/query-resource-control.md): implemented query-wide memory
  reservations including shared last-owner credit, cooperative cancellation, concurrency and
  memory-ordering arguments, ownership, failure, race testing, and measurement boundaries.
- [Bounded parallel query scheduling](learning/bounded-parallel-query-scheduling.md): implemented
  whole-pipeline thread affinity, a fixed-capacity accounted-chunk merge, deterministic failure
  arbitration, cooperative join cleanup, hostile/failure/fuzz coverage, and overhead measurement.
- [Bounded external sort](learning/bounded-external-sort.md): implemented contiguous stable runs,
  ephemeral checksummed row bytes, exact cross-run ties, finite memory/disk ownership, corruption
  handling, cleanup, fuzzing, and I/O measurement boundaries.
- [Bounded physical strategy selection](learning/bounded-physical-strategy-selection.md):
  authoritative finite sort estimates, exact in-memory/external decisions, explicit source-order
  proof obligations, bounded serial/parallel composition, ownership, failure, and profiles.
- [Phase 9 vectorized query exit](learning/phase-9-vectorized-query-exit.md): accepted executable
  boundary, full-plan scalar/vector differential oracle, ownership and limit closure, deferred
  contracts, and reproducible end-to-end profile methodology.
- [Native Protocol v1 framing](learning/native-protocol-v1-framing.md): portable checksummed frame
  interfaces, validation-before-allocation, ownership, failure behavior, and verification strategy.
- [Native protocol request lifecycle](learning/native-protocol-request-lifecycle.md): negotiated
  limits, bounded active requests, monotonic identities, cancellation, and durability-explicit
  acknowledgements.
- [Bounded connection buffers](learning/bounded-connection-buffers.md): fragmented/coalesced input,
  immutable short-write ownership, finite admission, overload, and cleanup.
- [Reactor-to-shard SPSC routing](learning/reactor-shard-spsc-routing.md): single-owner ring
  handoff, acquire/release publication and reuse proof, saturation, and lifecycle rules.
- [Bounded Linux epoll reactor](learning/bounded-epoll-reactor.md): single-owner readiness,
  admission, response identity, overload, timeout, and disconnect cleanup.
- [Native query result batches](learning/native-query-result-batches.md): self-describing SQL output
  schema, canonical row-major cells, borrowing, limits, and completion semantics.
- [Network security boundary](learning/network-security-boundary.md): loopback plaintext restriction,
  authenticator ownership, principal propagation, and fail-closed TLS mode.
- [Native client session](learning/native-client-session.md): bounded portable partial I/O,
  monotonic request generation, response validation, cancellation, and cleanup.
- [Segmented Multi-Raft persistent log](learning/raft-persistent-log.md): shared physical segments,
  append/sync frontiers, durable runtime batching, rotation, recovery, repair, ownership, and
  reclamation boundary.
- [Joint-consensus membership](learning/joint-consensus-membership.md): canonical membership
  commands, dual-quorum elections and commits, learner behavior, recovery, and application no-ops.
- [Committed Raft tablet application](learning/raft-tablet-application.md): committed-only command
  decoding, shared row/retry publication, applied-index ordering, and retained-log reconstruction.
- [Windowed materialized-view state](learning/windowed-materialized-view.md): committed/event-time
  separation, corrections, watermarks, exact checkpoints, durable recovery, and retention frontier.
- [Multi-tablet subscription order](learning/multi-tablet-subscription-order.md): canonical source
  vectors, coordinator admission order, component-wise acknowledgement, replay, and expiry.
- [Plan-bound subscription snapshot execution](adr/0096-plan-bound-subscription-snapshot-execution.md):
  exact manager/storage boundary validation, physical snapshot batches, END_STREAM/READY ordering,
  and fail-closed cancellation.
- [Schema-bound subscription plan identity](adr/0097-schema-bound-subscription-plan-identity.md):
  bounded `SUBSCRIBE SELECT` parsing/lowering, deterministic schema-bound fingerprints, and exact
  manager registration/resume compatibility.
- [Exact multi-tablet subscription checkpoints](adr/0098-exact-multi-tablet-subscription-checkpoints.md):
  canonical latest/expiry vectors, retained admission-order capture, strict suffix validation, and
  token-based replay after logical coordinator reconstruction.
- [Multi-tablet Subscription Checkpoint v1](formats/multi-tablet-subscription-checkpoint-v1.md):
  frozen portable coordinator bytes, exact identity/source/change layout, limits, CRC32C, and
  validation order.
- [Durable multi-tablet subscription checkpoint generations](adr/0100-durable-subscription-checkpoint-generations.md):
  lock ownership, exact next-generation installation, synchronized no-replace publication, and
  fail-closed reopen/latest selection.
- [Durable multi-tablet subscription owner](adr/0101-durable-multi-tablet-subscription-owner.md):
  exact coordinator recovery, checkpoint generation ownership, post-checkpoint replay boundary,
  and retention-frontier publication only after durable install.
- [Exact multi-tablet subscription snapshots](adr/0102-exact-multi-tablet-subscription-snapshot.md):
  complete vector/storage boundary validation, one global physical pipeline, and guarded
  QUERY_RESULT/END_STREAM/READY transition.
- [Subscription Plan Definition v1](formats/subscription-plan-definition-v1.md): exact SQL and
  database/table/schema/fingerprint bytes, CRC32C, immutable naming, and recovery validation.
- [Durable subscription plan registry](adr/0103-durable-subscription-plan-registry.md): synchronized
  definition installation and exact catalog-bound reprepare before resumed execution.
- [Durable metadata Raft state](learning/durable-metadata-state.md): checksummed commands,
  commit-order catalog application, quorum proof, and retained-log reconstruction.
- [Phase 10 network exit review](reviews/phase-10-network-review.md): implemented transport
  boundary, Linux portability and validation evidence, nonblocking risks, and the phase decision.
- [Feature completion pass review](reviews/feature-completion-pass.md): truthful Phase 11–17
  architecture slices, focused evidence, limitations, risks, and recommended next order.
- [Shared snapshot publication credit](learning/shared-snapshot-publication-credit.md): implemented
  one-query/one-epoch publication admission across part images, backed chunks, complete tablet scans,
  and ASOF aliases while retaining exact local image/output credit and last-owner cleanup.
- [Physical operator foundation](learning/physical-operator-foundation.md): implemented accounted
  chunk ownership, explicit pull/end/error lifecycle, allocation-free SQL Boolean filtering,
  allocation-free stable column-subset projection and global LIMIT, scalar-differential and
  deterministic projection/prefix properties, cancellation, early release, fuzzing, and measurement
  boundaries.
- [Physical pipeline plan](learning/physical-pipeline-plan.md): implemented bounded immutable unary
  plans, exact physical-shape propagation and source enforcement, composed scalar differential
  execution, exact bounded SQL ORDER BY stage shapes, hostile fuzzing, allocation/ownership failure
  behavior, and plan-overhead measurement.
- [Lifetime-pinned vector backing](learning/pinned-vector-backing.md): implemented uniform physical
  views over direct or shared immutable storage, conservative backing/ordinal accounting, coupled
  pin-credit lifetime, projection behavior, fuzzing, and ownership-overhead measurement.
- [CSEG storage](learning/cseg-storage.md): includes allocation-free projected-granule planning,
  exact raw-versus-owned decoded-byte requirements, borrowed-plan lifetime, allocation-failure
  behavior, fuzzing, and planning-overhead measurement.
- [Pinned CSEG scan source](learning/cseg-scan-source.md): implemented single-part physical pulls,
  pre-decode query admission, raw/decompressed backing ownership, pin/credit lifetime, deterministic
  granule properties, allocation-failure testing, fuzzing, and scan measurement.
- [Snapshot-bound CSEG loading](learning/snapshot-bound-cseg-loading.md): implemented per-part
  publication lifetime pins, held-predecessor validated loading, complete epoch/image accounting,
  safe query scan adaptation, and reclamation-through-chunk ownership.
- [Pruned multi-part snapshot CSEG scan](learning/pruned-snapshot-cseg-scan.md): implemented bounded
  canonical part planning, two-stage no-false-negative event-time pruning, selected-only image
  loading, query-accounted sequential composition, explicit CSEG-only visibility, and failure and
  measurement boundaries.
- [Complete append-only snapshot tablet scan](learning/complete-snapshot-tablet-scan.md): implemented
  exact durable/sealed/active source composition for one held aggregate epoch with uniform
  predicates, suffix shape, bounded ownership, and flush-boundary multiset evidence.
- [Snapshot physical pipeline instantiation](learning/snapshot-physical-pipeline.md): implemented
  checked schema/suffix source selection, snapshot-bound image loading and complete-source
  composition for reusable physical plans, with end-to-end SQL execution and failure evidence.
- [Snapshot-bound ASOF execution](learning/snapshot-bound-asof-execution.md): implemented exact
  same-epoch multi-source binding, source-shape inference, partial-construction cleanup, and
  end-to-end checked ASOF plan execution.
- [Mutable-head scan source](learning/mutable-head-scan-source.md): implemented exact-publication
  head pinning, bounded canonical bitmap/offset materialization, schema-tail NULL synthesis,
  query-accounted pull ownership, and exact event-time filtering.
- [Shared vector row-version suffix](learning/vector-row-version-suffix.md): implemented opt-in
  common WAL/sequence/row/operation columns for CSEG and head sources with checked layout,
  source-specific ownership, query accounting, exact DEDUP-keyed ORDER BY tie consumption, helper
  preservation, fuzzing, and measurement.
- [Exact timestamp-range vector filter](learning/exact-timestamp-range-filter.md): implemented
  edge-safe open/closed `TIMESTAMP_NS` truth, allocation-free stable selection compaction,
  query-accounted pull and bounded-plan integration, fuzzing, and measurement boundaries.
- [Source-column output materialization](learning/source-column-output-materialization.md):
  implemented accounted reordered/duplicate source output and canonical selected-row compaction.
- [Typed-constant output materialization](learning/typed-constant-output-materialization.md):
  implemented all-type and typed-NULL canonical expansion under exact admission.
- [Checked vector expressions](learning/vector-expression-programs.md): implemented bounded
  fixed-width arithmetic, comparison, Boolean, CAST, COALESCE, and time-bucket programs plus
  borrowed STRING/SYMBOL casts, ASCII case output, byte-order comparisons, and NULL predicates with
  scalar differential evidence.
- [Bound SELECT physical lowering](learning/bound-select-physical-lowering.md): implemented the
  exact single-source WHERE/projection/aggregate/ORDER/LATEST/LIMIT bridge into physical pipelines.
- [Bounded physical LATEST BY](learning/bounded-latest-by.md): implemented exact typed grouping,
  timestamp, physical-key, and row-version winner ties before WHERE with bounded accounted sort
  ownership and allocation-free adjacent compaction.
- [Bounded physical ASOF join](learning/bounded-asof-join.md): implemented an exact two-input,
  query-accounted temporal lookup primitive with SQL equality, complete winner ties, left null
  extension, and explicit match presence.
- [Checked ASOF physical plans](learning/checked-asof-physical-plan.md): implemented exact
  left-deep preparation, binary handoff, final-pipeline shape, ownership, and configuration bounds.
- [Bound ASOF physical lowering](learning/bound-asof-physical-lowering.md): implemented source-aware
  join-expression preparation, nullable left joins, post-join stages, and exact joined identities.
- [Streaming ungrouped vector aggregates](learning/streaming-ungrouped-aggregates.md): implemented
  fixed-state global COUNT/SUM/AVG/MIN/MAX/variance over accounted chunk streams with exact empty,
  NULL, numeric, ownership, cancellation, and canonical-output behavior.

Future format, protocol, subsystem, operations, and learning documents should be linked here when they are added by their corresponding roadmap phase.
