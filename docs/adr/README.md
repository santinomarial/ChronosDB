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
