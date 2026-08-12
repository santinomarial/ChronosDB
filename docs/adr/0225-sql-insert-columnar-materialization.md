# ADR 0225: SQL INSERT Columnar Materialization

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB query and ingestion maintainers

## Context

SQL v1 can bind `INSERT ... VALUES` and evaluate it into owned, schema-ordinal scalar rows. WAL
append execution accepts the canonical immutable columnar batch model instead. Service dispatch
needs one checked ownership boundary between those representations; duplicating physical encoding
inside the daemon would create a second, weaker append format.

## Decision

`materialize_sql_v1_insert_batch()` transposes a `MaterializedSqlInsert` into one
`OwnedColumnarBatch` retaining the exact bound schema. It validates every row width, scalar logical
type, and nullability relationship before returning ownership. Fixed-width values use the canonical
little-endian physical representation, Boolean and validity values use canonical bitmaps, and
variable-width values use checked u32 prefix offsets. Decimal coefficients and UUID bytes preserve
their canonical 16-byte representations.

The caller supplies `ColumnarBatchLimits`. Row and column bounds fail before buffer construction;
the canonical batch constructor performs exact logical- and retained-buffer accounting before it
accepts the result. Allocation and container-length failures become resource-exhausted statuses.
This function performs no WAL write and allocates no durable request identity.

## Consequences

Native SQL INSERT can reuse the same append executor, validation, retry directory, mutable-head
publication, and WAL durability modes as encoded native ingest. There is one in-memory columnar
representation at that boundary, and the bound schema remains pinned for the batch lifetime.

Materialization currently constructs a complete batch before canonical retained-capacity accounting.
SQL binder row/value limits still cap shape, while large individual variable values may allocate
before the final byte-limit rejection. Earlier exact preflight accounting and allocation-failure
campaigns remain qualification work.

## Validation

Focused statement-binder coverage materializes a multi-row, partially specified INSERT, converts
cells back through the public scalar decoder, checks schema identity and typed NULLs, and verifies
row-limit rejection. The query target and public-header self-containment build with the new source.

## References

- [ADR 0005](0005-columnar-heads-and-immutable-cseg-parts.md)
- [ADR 0015](0015-columnar-batch-v1-and-wal-append-command.md)
- [ADR 0220](0220-native-protocol-ingest-service-adapter.md)
