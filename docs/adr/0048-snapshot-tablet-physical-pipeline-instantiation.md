# ADR 0048: Snapshot Tablet Physical Pipeline Instantiation

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB storage-query and query-execution maintainers

## Context

ADR 0047 provides one exact append-only tablet source, while ADRs 0036, 0041, 0043, and 0046
produce reusable checked physical pipelines from bound SQL. Callers previously had to reproduce the
pipeline's implicit source contract, choose row-version suffix mode, project source columns, load
durable images, compose mutable heads, and then instantiate the plan. A suffix mistake can change
ORDER BY tie semantics or shift every downstream ordinal.

SQL lowering returns source-spanned `SqlDiagnostic` values. Snapshot planning, image loading, and
physical construction return ordinary storage/query `Status` values. Combining both operations in
one convenience function would erase that useful diagnostic distinction.

## Decision

- `instantiate_snapshot_tablet_pipeline` is the checked boundary between one already-lowered
  `PhysicalPipelinePlan` and one exact `DatabaseStorageSnapshot` tablet.
- The plan input must contain every destination-schema column in schema ordinal order. It may then
  contain exactly the shared four-column non-null row-version suffix. No other width, type, or
  nullability is accepted.
- The connector infers suffix mode from that checked plan shape and applies it uniformly to CSEG and
  head limits. A caller cannot independently select conflicting source modes.
- The connector plans the durable subset without predicate pushdown, validates and loads selected
  images from the same held snapshot, creates the complete tablet source, and instantiates the
  reusable plan. `WHERE` remains in the physical pipeline, so source integration cannot weaken SQL
  truth. Later pushdown requires a separately checked equivalence decision.
- SQL parse/bind/lower remains a separate step so `SqlDiagnostic` spans and categories are retained.
- All schema ordinals are currently materialized. Projection pushdown is deferred until the lowerer
  exposes a checked required-source-column contract.
- Existing finite planning, image-validation, CSEG, head, and composition limits are grouped in
  `SnapshotTabletPipelineLimits`. New allocation failures are classified as resource exhaustion;
  source or plan failure destroys all adopted images, pins, operators, and query credit.

This decision changes no durable format, SQL semantics, snapshot visibility, or thread-affinity
contract.

## Consequences

Supported bound SQL can now execute over the exact current append-only storage publication without
manual source wiring. Base ORDER BY pipelines automatically receive the DEDUP identity and commit
suffix they require; aggregate and unordered plans keep their narrower source shape. Hidden columns
remain removed by the already-checked physical plan.

Construction is eager and may perform synchronous part I/O. It does not push predicates or
projections, share publication credit, schedule children in parallel, or spill.

## Alternatives considered

- **Let callers configure suffix mode:** duplicates a semantic decision and permits a pipeline/source
  mismatch before runtime.
- **Inspect pipeline stages for ORDER BY:** stage presence does not define source shape; aggregate
  ORDER BY deliberately needs no base-row suffix.
- **Fuse SQL lowering and source construction:** collapses source-spanned SQL diagnostics into a
  less precise cross-layer error type.
- **Push WHERE into storage immediately:** only timestamp predicates have an accepted exact pruning
  adapter, and extracting general bound expressions would introduce optimizer policy here.

## Affected invariants

This decision supports invariants [6, 11, 16, and 18](../architecture/invariants.md). One held epoch
supplies every source, owned pins survive all borrowed chunks, mutable publications remain immutable
to readers, and physical source selection cannot weaken the checked SQL pipeline.

## Validation plan

- Unit tests execute bound WHERE/ORDER BY/LIMIT and aggregate plans over authenticated head-backed
  snapshots, verify exact values and hidden-suffix removal, and reject hostile schema/suffix/limit
  shapes without leaked credit.
- Allocation-failure injection covers every new owned construction allocation.
- The CSEG scan fuzzer varies valid and malformed pipeline shapes, suffix inference, limits,
  cancellation, and pull boundaries over an authenticated snapshot.
- A microbenchmark measures complete connector construction and bounded execution. Public-header,
  installation, external-consumer, sanitizer, and repository-wide checks cover the exported API.

## Migration or rollback considerations

There is no persisted-state migration. Rollback removes only the connector API. Any replacement
must preserve exact schema/suffix validation, held-epoch provenance, finite admission, diagnostic
separation, and complete ownership cleanup.

## Unresolved questions

Checked predicate/projection pushdown, mapped/asynchronous providers, shared pin credit, parallel
scheduling, spill, joins, and correction/delete visibility remain separate work.

## References

- [ADR 0023](0023-bounded-physical-pipeline-plan.md)
- [ADR 0036](0036-bound-select-to-physical-pipeline-lowering.md)
- [ADR 0045](0045-shared-vector-row-version-suffix.md)
- [ADR 0047](0047-exact-append-only-snapshot-tablet-scan.md)
- [Complete snapshot tablet scan](../learning/complete-snapshot-tablet-scan.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
