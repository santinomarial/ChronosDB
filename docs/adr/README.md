# Architecture Decision Records

Architecture Decision Records (ADRs) preserve the context and reasoning behind consequential project choices. An ADR is required when a decision changes a durable or network contract, an invariant, component ownership, concurrency or memory-ordering strategy, recovery/consistency semantics, security boundary, production dependency, supported platform, compatibility policy, or a choice that would be expensive to reverse. Routine local implementation details that follow accepted specifications do not need an ADR.

## Numbering and filenames

Use a four-digit, monotonically increasing repository-wide number followed by a short lowercase hyphenated title:

```text
0013-short-decision-title.md
0014-another-decision-title.md
```

Reserve the number when opening the ADR. Numbers are never reused, even when a proposal is rejected. The index below lists records in number order.

## Statuses

- **proposed:** under review and not an implementation authority.
- **accepted:** approved and authoritative for work in its scope.
- **superseded:** replaced by one or more named ADRs; retained for history.
- **rejected:** considered and deliberately not selected; retained with rationale.
- **deprecated:** once accepted, but no longer recommended or valid for new work; migration may still be underway.

## Process

1. Copy [the template](template.md), allocate the next number, and fill every section. Use `None` with a reason rather than deleting a section.
2. Link the affected [invariants](../architecture/invariants.md), specifications, benchmarks, prototypes, and issue discussions that actually exist. Do not invent references.
3. Review alternatives, failure modes, compatibility, operations, and validation evidence before changing status to `accepted`.
4. Record implementation follow-ups outside the ADR while keeping links back to the decision.
5. When the decision changes, add a new ADR and update both records' statuses and cross-references.

An accepted ADR is not silently rewritten after implementation begins. Correct typographical errors or add clearly labeled retrospective notes without changing the original decision. A semantic change, new tradeoff, or reversed decision requires a new ADR so the project's history remains reviewable.

## Index

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-project-scope-and-workloads.md) | Project scope and workloads | accepted |
| [0002](0002-language-platform-and-portability.md) | Language, platform, and portability | accepted |
| [0003](0003-single-node-first-development-order.md) | Single-node-first development order | accepted |
| [0004](0004-thread-ownership-and-ingress-concurrency.md) | Thread ownership and ingress concurrency | accepted |
| [0005](0005-columnar-heads-and-immutable-cseg-parts.md) | Columnar heads and immutable CSEG parts | accepted |
| [0006](0006-wal-durability-and-group-commit.md) | WAL durability and group commit | accepted |
| [0007](0007-event-time-system-time-and-row-versioning.md) | Event time, system time, and row versioning | accepted |
| [0008](0008-custom-sql-and-vectorized-execution.md) | Custom SQL and vectorized execution | accepted |
| [0009](0009-network-reactor-strategy.md) | Network reactor strategy | accepted |
| [0010](0010-tablets-raft-and-multiplexed-log-storage.md) | Tablets, Raft, and multiplexed log storage | accepted |
| [0011](0011-dependency-and-build-versus-buy-policy.md) | Dependency and build-versus-buy policy | accepted |
| [0012](0012-correctness-testing-and-performance-evidence.md) | Correctness testing and performance evidence | accepted |
| [0013](0013-wal-v1-format-and-recovery.md) | WAL v1 format and recovery | accepted |
| [0014](0014-logical-types-schema-identity-and-evolution.md) | Logical types, schema identity, and evolution | accepted |
| [0015](0015-columnar-batch-v1-and-wal-append-command.md) | Columnar batch v1 and WAL append command | accepted |
| [0016](0016-cseg-v1-layout-integrity-and-compression.md) | CSEG v1 layout, integrity, and compression | accepted |
| [0017](0017-manifest-generations-installation-and-checkpoints.md) | Manifest generations, part installation, and checkpoints | accepted |
| [0018](0018-append-only-cseg-compaction-and-manifest-replacement.md) | Append-only CSEG compaction and Manifest replacement | accepted |
| [0019](0019-rebuildable-pruning-delta-planning-and-part-reclamation.md) | Rebuildable pruning, delta planning, and part reclamation | accepted |
| [0020](0020-bounded-vector-chunk-representation.md) | Bounded vector chunk representation | accepted |
| [0021](0021-query-resource-accounting-and-cooperative-cancellation.md) | Query resource accounting and cooperative cancellation | accepted |
| [0022](0022-pull-based-physical-operator-lifecycle.md) | Pull-based physical operator lifecycle | accepted |
| [0023](0023-bounded-physical-pipeline-plan.md) | Bounded physical pipeline plan | accepted |
| [0024](0024-lifetime-pinned-vector-chunk-backing.md) | Lifetime-pinned vector chunk backing | accepted |
| [0025](0025-allocation-free-cseg-projected-read-planning.md) | Allocation-free CSEG projected read planning | accepted |
| [0026](0026-pinned-in-memory-cseg-scan-source.md) | Pinned in-memory CSEG scan source | accepted |
| [0027](0027-snapshot-bound-cseg-images-and-part-lifetime-pins.md) | Snapshot-bound CSEG images and part-lifetime pins | accepted |
| [0028](0028-pruned-multi-part-snapshot-cseg-scan.md) | Pruned multi-part snapshot CSEG scan | accepted |
| [0029](0029-query-accounted-mutable-head-scan-source.md) | Query-accounted mutable-head scan source | accepted |
| [0030](0030-exact-timestamp-range-vector-filtering.md) | Exact timestamp-range vector filtering | accepted |
| [0031](0031-exact-prune-then-filter-snapshot-cseg-scans.md) | Exact prune-then-filter snapshot CSEG scans | accepted |
| [0032](0032-exact-event-time-mutable-head-scans.md) | Exact event-time mutable-head scans | accepted |
| [0033](0033-accounted-source-column-output-materialization.md) | Accounted source-column output materialization | accepted |
| [0034](0034-accounted-typed-constant-vector-outputs.md) | Accounted typed-constant vector outputs | accepted |
| [0035](0035-bounded-checked-vector-expression-programs.md) | Bounded checked vector expression programs | accepted |
| [0036](0036-bound-select-to-physical-pipeline-lowering.md) | Bound SELECT to physical pipeline lowering | accepted |
| [0037](0037-fixed-width-vector-casts-and-scalar-functions.md) | Fixed-width vector casts and scalar functions | accepted |
| [0038](0038-borrowed-variable-width-vector-materialization.md) | Borrowed variable-width vector materialization | accepted |
| [0039](0039-borrowed-text-predicate-vector-kernels.md) | Borrowed text-predicate vector kernels | accepted |
| [0040](0040-streaming-ungrouped-vector-aggregates.md) | Streaming ungrouped vector aggregates | accepted |
| [0041](0041-bound-global-aggregate-physical-lowering.md) | Bound global aggregate physical lowering | accepted |
| [0042](0042-query-accounted-bounded-grouped-aggregates.md) | Query-accounted bounded grouped aggregates | accepted |
| [0043](0043-bound-grouped-aggregate-physical-lowering.md) | Bound grouped aggregate physical lowering | accepted |
| [0044](0044-query-accounted-bounded-physical-sort.md) | Query-accounted bounded physical sort | accepted |
| [0045](0045-shared-vector-row-version-suffix.md) | Shared vector row-version suffix | accepted |
| [0046](0046-exact-bounded-sql-order-by-lowering.md) | Exact bounded SQL ORDER BY lowering | accepted |
| [0047](0047-exact-append-only-snapshot-tablet-scan.md) | Exact append-only snapshot tablet scan | accepted |
| [0048](0048-snapshot-tablet-physical-pipeline-instantiation.md) | Snapshot tablet physical pipeline instantiation | accepted |
| [0049](0049-query-accounted-variable-width-extrema.md) | Query-accounted variable-width extrema | accepted |
| [0050](0050-canonical-query-accounted-group-hashing.md) | Canonical query-accounted group hashing | accepted |
| [0051](0051-exact-bounded-latest-by-physical-lowering.md) | Exact bounded LATEST BY physical lowering | accepted |
| [0052](0052-query-accounted-bounded-asof-join.md) | Query-accounted bounded ASOF join | accepted |
| [0053](0053-checked-left-deep-asof-physical-plan.md) | Checked left-deep ASOF physical plan | accepted |
| [0054](0054-bound-asof-select-physical-lowering.md) | Bound ASOF SELECT physical lowering | accepted |
| [0055](0055-snapshot-bound-multi-source-asof-instantiation.md) | Snapshot-bound multi-source ASOF instantiation | accepted |
| [0056](0056-shared-query-credit-and-bounded-parallel-scheduling.md) | Shared query credit and bounded parallel scheduling | accepted |
| [0057](0057-bounded-checksummed-external-sort.md) | Bounded checksummed external sort | accepted |
| [0058](0058-shared-snapshot-publication-query-credit.md) | Shared snapshot publication query credit | accepted |
| [0059](0059-bounded-physical-strategy-selection.md) | Bounded physical strategy selection | accepted |
| [0060](0060-native-protocol-v1-framing.md) | Native Protocol v1 framing | accepted |
| [0061](0061-native-protocol-handshake-and-request-lifecycle.md) | Native protocol handshake and request lifecycle | accepted |
| [0062](0062-bounded-connection-buffer-ownership.md) | Bounded connection buffer ownership | accepted |
| [0063](0063-bounded-reactor-shard-spsc-routing.md) | Bounded reactor-to-shard SPSC routing | accepted |
| [0064](0064-bounded-linux-epoll-reactor.md) | Bounded Linux epoll reactor ownership and overload | accepted |
| [0065](0065-self-describing-query-result-batches.md) | Self-describing Protocol v1 query result batches | accepted |
| [0066](0066-authentication-and-tls-integration-boundary.md) | Authentication and TLS integration boundary | accepted |
| [0067](0067-bounded-native-client-session.md) | Bounded native client session | accepted |
| [0068](0068-live-handoff-and-resume-token-v1.md) | Bounded live handoff and Resume Token v1 | accepted |
| [0069](0069-deterministic-raft-and-multiplexed-state-record.md) | Deterministic Raft and multiplexed state records | accepted |
| [0070](0070-feature-pass-logical-boundaries.md) | Temporal, distributed, cold-tier, and runtime feature-pass boundaries | accepted |
| [0071](0071-segmented-multi-raft-persistence.md) | Segmented Multi-Raft persistence and recovery | accepted |
| [0072](0072-explicit-wal-and-raft-commit-identities.md) | Explicit WAL and Raft commit identities | accepted |
| [0073](0073-committed-raft-tablet-application.md) | Committed Raft tablet application and retained-log recovery | accepted |
| [0074](0074-quorum-sync-proof-boundary.md) | Raft quorum-synchronization proof boundary | accepted |
| [0075](0075-durable-metadata-raft-commands.md) | Durable metadata Raft commands and recovery | accepted |
| [0076](0076-joint-consensus-raft-membership.md) | Joint-consensus Raft membership | accepted |
| [0077](0077-snapshot-membership-checkpoints.md) | Raft snapshot membership checkpoints | accepted |
| [0078](0078-two-stage-raft-snapshot-installation.md) | Two-stage Raft snapshot installation | accepted |
| [0079](0079-temporal-mutation-command-v1.md) | Temporal Mutation Command v1 | accepted |
| [0080](0080-cseg-v2-temporal-system-columns.md) | CSEG v2 temporal system columns | accepted |
| [0081](0081-cseg-v2-temporal-snapshot-resolution.md) | CSEG v2 temporal snapshot resolution | accepted |
| [0082](0082-source-neutral-manifest-v2-layout.md) | Source-neutral Manifest v2 layout | accepted |
| [0083](0083-manifest-v2-temporal-wal-recovery.md) | Manifest v2 temporal WAL recovery composition | accepted |
| [0084](0084-verified-temporal-checkpoint-overlap.md) | Verified temporal checkpoint overlap | accepted |
