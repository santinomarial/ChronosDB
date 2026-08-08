# ADR 0055: Snapshot-Bound Multi-Source ASOF Instantiation

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB storage-query and query-execution maintainers

## Context

ADR 0054 lowers bound SQL ASOF queries into a checked `PhysicalAsofPlan`, and ADR 0048 connects one
unary physical plan to one complete append-only tablet source. Callers still had to construct every
ASOF source independently. That left the common aggregate snapshot epoch, SQL source order,
per-source row-version suffix mode, and partial-construction cleanup outside a checked boundary.

## Decision

- `instantiate_snapshot_asof_plan` accepts one held `DatabaseStorageSnapshot`, a checked
  `PhysicalAsofPlan`, and exactly one `SnapshotTabletSourceBinding` per SQL source in source order.
- Every binding borrows a schema lineage during instantiation and identifies its tablet,
  destination schema, and finite source limits. Returned operators retain or pin everything needed
  by later pulls; they do not borrow the binding array or lineage.
- The first binding is checked against the first left-preparation input. Each later binding is
  checked against its corresponding right-preparation input. The existing exact schema plus
  optional shared row-version suffix rule remains authoritative.
- All durable-part planning, authenticated image loading, and mutable-head composition use the same
  supplied aggregate snapshot. Independently acquired per-source epochs are not accepted.
- Source creation is eager and serial. Failure at any later source destroys earlier sources and
  releases their query reservations and pins through RAII. The checked ASOF plan then adopts all
  sources atomically for execution.

This decision adds no cross-tablet snapshot acquisition protocol, durable format, visibility rule,
parallel scheduler, shared pin charge, or spill behavior.

## Consequences

Supported bound ASOF SQL can now run directly over complete current append-only tablet publications
without caller-managed source wiring. Every source receives the suffix demanded by lowering, and
the final plan continues to hide row identities, join helpers, and presence bits.

Construction may repeat validation and conservative pin charges when multiple aliases reference
the same physical tablet. Sharing those charges requires a separate ownership contract; semantic
correctness does not depend on it.

## Alternatives considered

- **Accept prebuilt independent sources:** cannot prove they represent one aggregate epoch.
- **Infer source order from bindings:** aliases and repeated tables make source order semantic, so
  the checked plan order remains authoritative.
- **Acquire a snapshot per source:** can combine rows that never coexisted and violates the query
  snapshot contract.
- **Store lineage references in runtime operators:** extends an unnecessary borrowed lifetime; the
  existing scan factories finish lineage-dependent construction before returning.

## Affected invariants

This decision supports invariants [6, 11, 16, and 18](../architecture/invariants.md): all sources
share one captured epoch, retained bytes are admitted before use, hidden identity remains internal,
and failure/cancellation releases owned resources.

## Validation plan

- Execute a three-source bound ASOF chain with exact ORDER BY/LIMIT and hidden identity removal.
- Reject wrong source counts, absent schemas, and late-source finite limits without leaked credit.
- Inject failure at every new owned allocation, fuzz malformed bindings/limits/cancellation, and
  benchmark two-source snapshot construction plus execution.
- Cover the public header, installed external consumer, ASan/UBSan, TSan, and repository checks.

## Migration or rollback considerations

There is no persisted-state migration. Rollback removes only this connector and binding type.

## Unresolved questions

Correction/delete row-version resolution, shared publication credit, parallel scheduling,
optimizer selection, spill, and nonlocal snapshot acquisition remain separate work.

## References

- [ADR 0048](0048-snapshot-tablet-physical-pipeline-instantiation.md)
- [ADR 0053](0053-checked-left-deep-asof-physical-plan.md)
- [ADR 0054](0054-bound-asof-select-physical-lowering.md)
- [Snapshot-bound ASOF execution](../learning/snapshot-bound-asof-execution.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
