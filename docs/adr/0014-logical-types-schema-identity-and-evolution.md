# ADR 0014: Logical Types, Schema Identity, and Evolution

- **Status:** accepted
- **Date:** 2026-08-03
- **Owners:** ChronosDB catalog, ingestion, and storage maintainers

## Context

The first mutable-table implementation needs one meaning for every value and one stable way to
identify tables, columns, schemas, and tablets across rename, WAL replay, snapshots, and eventual
CSEG flush. Names and ordinal positions are insufficient durable identities. Unconstrained schema
changes would also make old batches depend on mutable catalog state and make deterministic replay
ambiguous.

## Accepted decision

ChronosDB v1 has the logical types `BOOL`, the four signed and four unsigned integer widths,
`FLOAT32`, `FLOAT64`, `DECIMAL(p,s)`, `TIMESTAMP_NS`, `DATE`, `SYMBOL`, `STRING`, `BINARY`, and
`UUID`, with the semantics in the [data-model contract](../product/data-model.md). The
[columnar-batch v1 format](../formats/columnar-batch-v1.md) assigns stable type codes and exact
value encodings. An implementation may initially reject a specified type it does not yet support,
but it may never reinterpret its code.

`TableId`, `ColumnId`, `SchemaId`, and `TabletId` are distinct nominal types whose durable value is
an opaque, nonzero 128-bit identifier. New identifiers are generated with collision-resistant UUID
generation, are never inferred from a name or ordinal, and are never reused. Their durable byte
form is the 16-byte UUID network order used by the batch and command specifications. Renaming a
table or column does not change its identity; table display-name changes are catalog metadata and
do not alter row shape.

Each table has a linear sequence of immutable schema versions. A schema contains its `TableId`, a
unique `SchemaId`, a positive `uint64` schema-version number, an optional parent `SchemaId`, and an
ordered set of column definitions. A column definition contains a stable `ColumnId`, bound name,
logical type and parameters, nullability, and declared roles. The event-time, physical-order,
partition, shard, and deduplication declarations refer to columns by `ColumnId`, not by name or
ordinal. Hidden system columns are not client schema columns.

An initial schema is version 1. Each successor has exactly one parent for the same table and uses
`parent.version + 1`; the active lineage cannot branch. Published versions are never edited or
reassigned. The catalog retains every schema needed by a WAL record, head, snapshot, installed
part, backup, or compatibility window.

Initial in-place evolution is deliberately narrow:

- a column may be renamed while preserving its `ColumnId`, ordinal, type, nullability, and roles;
- one or more new nullable columns with no default may be appended after all existing columns;
- the two changes may occur in one successor version; and
- a name used anywhere in a table lineage is not reassigned to another `ColumnId`.

Dropping or reordering a column; changing type, type parameters, nullability, event-time role,
physical ordering, partitioning, shard key, or deduplication key; adding a non-null column or a
default; and changing a column identity are not v1 evolution. They require a new table identity or
a future migration specification and ADR.

A table schema is valid only when column identities and bound names are unique, all type parameters
are valid, exactly one non-null `TIMESTAMP_NS` column is the event-time column, every referenced key
column exists, and the routing declaration guarantees that all versions of a deduplication identity
reach one tablet. Deduplication and shard-key columns are non-null. A schema-shaped batch contains
every user column exactly once in schema ordinal order; omission is not a defaulting mechanism.

An old schema version remains meaningful after an allowed successor is installed. A reader binding
the successor projects an added column as `NULL` for rows written under an ancestor and resolves a
rename by `ColumnId`. Mutable heads are schema-bound; switching the active ingest schema seals the
old active head before a batch under the successor can be published. A new write must use the
active ingest schema; a matching retry under a retained ancestor may still return its original
outcome without appending rows.

## Detailed rationale

Opaque identities separate durable meaning from user-facing names and physical placement.
Immutable, linear schema versions let a WAL command name exactly the interpretation used at ingest
and let snapshots pin it. Restricting initial evolution to rename and nullable tail addition makes
cross-version projection deterministic without casts, backfill, default evaluation, or mutation of
old storage.

The full logical type registry is frozen now so independent batch, head, and query implementations
cannot assign conflicting meanings. This does not claim every type is implemented in the first
code increment.

## Alternatives considered

- **Names as identities:** renames and historical binding become ambiguous, and name reuse can
  reinterpret durable data.
- **Ordinals as identities:** insertion, reordering, or dropped columns can silently bind old bytes
  to a different meaning.
- **Schema content hash as the only identity:** identical definitions in different catalog
  histories become indistinguishable and renames create awkward identity semantics. Content may be
  fingerprinted separately, but `SchemaId` is authoritative.
- **Immediate widening and drop support:** useful eventually, but requires cast, historical
  projection, compaction, and migration rules that do not exist in this phase.
- **Defaults for added columns:** replay would depend on a versioned expression engine and exact
  evaluation time. Nullable addition has the unambiguous historical value `NULL`.

## Consequences

- Catalog storage must preserve nominal identity types and complete immutable schemas.
- Names remain useful for binding but never identify durable column data.
- A schema change can increase head turnover because it seals the old schema-bound generation.
- Unsupported type codes fail explicitly; they are never treated as bytes or another type.
- More permissive evolution requires a later versioned migration contract.

## Affected invariants

This decision governs invariants [4, 6, 8, 9, 10, 13, 14, and 16](../architecture/invariants.md):
ordered deterministic application, stable snapshots, idempotent recovery and retry, safe typed
decoding, temporal identity, versioned formats, and complete publication.

## Validation plan

- Unit and property tests will cover every type boundary, decimal parameter, nullability rule,
  identity comparison, duplicate name/identity, and invalid role/key reference.
- Generated schema histories will accept only linear rename and nullable-tail-add transitions and
  compare old-to-new projection with a reference model.
- Batch and future CSEG golden fixtures will prove that identity and type codes are independent of
  host byte order and native object layout.
- Replay and snapshot tests will pin old schemas across schema activation, head sealing, and
  recovery.

## Deferred decisions

Catalog durable encoding and installation, table creation protocol, name authorization, default
expressions, schema migration/backfill, drop and widening rules, partition/tablet-map evolution and
routing epochs, generated row identities, and distributed schema agreement are deferred. The final
implementation choice for UUID generation and any commodity cryptography dependency must follow
[ADR 0011](0011-dependency-and-build-versus-buy-policy.md).

## Migration or reversal implications

Once durable batches or CSEG parts use these identities and type codes, changing their meaning
requires a new format or explicit converter. A future schema feature adds a versioned transition;
it cannot mutate an accepted schema or reinterpret an old batch.

## References

- [Data model](../product/data-model.md)
- [Columnar batch v1](../formats/columnar-batch-v1.md)
- [Columnar ingestion architecture](../architecture/columnar-ingestion.md)
- [ADR 0005](0005-columnar-heads-and-immutable-cseg-parts.md)
- [ADR 0007](0007-event-time-system-time-and-row-versioning.md)
