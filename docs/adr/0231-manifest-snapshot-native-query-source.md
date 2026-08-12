# ADR 0231: Manifest-Snapshot Native Query Source

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB single-node, query, Manifest, and service maintainers

## Context

After live flush was enabled, the native SELECT adapter still instantiated pipelines directly from
`TabletState`. A flushed sealed head was correctly retired from that owner, so queries would omit
its Manifest-selected CSEG rows even though restart and aggregate publication retained them.

## Decision

`SingleNodeDatabase::instantiate_table_pipeline()` acquires one aggregate
`DatabaseStorageSnapshot` and invokes the existing multi-tablet snapshot pipeline with the retained
Manifest storage owner, canonical tablet placements, schema lineage, and checked physical plan.
Native SELECT uses this boundary for its sole supported table source.

Snapshot CSEG planning now accepts a tablet that exists only in aggregate head publication and
constructs a bounded empty durable plan. This lets one composition handle new/head-only tablets,
mixed CSEG-plus-head tablets, and fully durable tablets without pretending that an absent Manifest
tablet descriptor is corruption.

Every native write invokes storage maintenance after WAL application. Before draining any sealed
queue, that operation refreshes each local tablet in the aggregate publication, ensuring a query
acquires the latest complete active/sealed head boundary even when no rotation occurred. Direct
low-level callers of the exposed append primitives must invoke that owner maintenance boundary
before expecting aggregate query visibility.

## Consequences

Native SQL now reads durable CSEGs followed by the sealed and active heads from one exact aggregate
epoch, so flush substitution is neither duplicated nor omitted. Existing pruning, validation,
schema projection, query-memory accounting, and global multi-tablet unary-plan semantics are reused.

The current native adapter still supports exactly one SQL table source; ASOF and distributed source
binding remain separate service composition work. CSEG images are loaded eagerly by the existing
bounded snapshot pipeline.

## Validation

All database/service cases pass through the aggregate query source. The live-flush test verifies
`count(*) = 6` both before and after restart while storage is split into a four-row CSEG and a
two-row WAL-backed head. Existing head-only query, empty result, overflow, DDL, ingest, and SQL INSERT
cases remain passing.

## References

- [ADR 0048](0048-snapshot-tablet-physical-pipeline-instantiation.md)
- [ADR 0230](0230-live-single-node-sealed-head-flush.md)
- [Snapshot physical pipeline](../learning/snapshot-physical-pipeline.md)
