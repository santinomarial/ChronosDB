# ADR 0233: Native Whole-Table ASOF Query Composition

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB query, Manifest, single-node, and native-service maintainers

## Context

Checked SQL ASOF lowering and one-epoch snapshot ASOF instantiation already existed, but the native
service always invoked the unary lowerer and rejected every bound SELECT with more than one source.
The existing ASOF storage binding also named one tablet per SQL source. Using it directly from the
service would silently omit rows when a bound table has more than one local tablet.

## Decision

`SnapshotTableSourceBinding` names a nonempty canonical tablet vector, retained schema lineage,
destination schema, and finite scan limits for one SQL source. The new whole-table ASOF connector:

- validates and concatenates every tablet below that source's checked preparation pipeline;
- instantiates every source from one aggregate `DatabaseStorageSnapshot` and one shared publication
  reservation; and
- instantiates the checked left-deep ASOF plan only after every complete source exists.

The original one-tablet connector remains source compatible and delegates to the whole-table
boundary. `SingleNodeDatabase` resolves bound table/schema identities to its retained lineage and
canonical local placements, captures one Manifest snapshot, and invokes that connector. The native
service selects `lower_bound_sql_asof_select` only for a bound ASOF query; unary SELECT behavior is
unchanged.

This does not add arbitrary joins, distributed sources, correction/delete visibility, or a new SQL
surface. Native result row, batch, byte, and query-memory bounds continue to apply after composition.

## Consequences

The accepted SQL v1 ASOF surface can now execute through the native service across complete local
tables. CSEG parts and mutable heads from every source belong to one captured database epoch, and
global join, aggregate, sort, and limit semantics are evaluated once above each whole-table source.
Repeated aliases of the same table construct independent scans but share the aggregate publication
reservation.

Construction remains eager and synchronous. Per-source multi-tablet scans are serial; parallel
scheduling requires separate evidence and must preserve the same checked shapes and epoch.

## Validation

Focused query coverage executes a two-source ASOF aggregate where both aliases span two tablets and
proves all five left rows participate. Native service coverage writes six rows, forces a mixed
CSEG/head publication, executes the ASOF aggregate, restarts through the shutdown checkpoint, and
proves the same result from recovered storage. Existing hostile source-count/schema/limit tests and
all native unary cases remain passing.

## References

- [ADR 0054](0054-bound-asof-select-physical-lowering.md)
- [ADR 0055](0055-snapshot-bound-multi-source-asof-instantiation.md)
- [ADR 0221](0221-global-multi-tablet-vector-source.md)
- [ADR 0231](0231-manifest-snapshot-native-query-source.md)
- [Snapshot-bound ASOF execution](../learning/snapshot-bound-asof-execution.md)
