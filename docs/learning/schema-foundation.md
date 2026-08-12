# Logical Schema Foundation

> **Status: implemented foundation.** The `chronos_schema` target implements identities, logical
> types, immutable column/table schemas, v1 successor validation, an in-memory linear lineage, and
> ancestor projection metadata. It does not implement the columnar-batch codec, catalog
> persistence, mutable heads, SQL binding, or WAL application commands.

## Purpose and authority

This library turns the accepted Phase 4 vocabulary into reusable C++ values before any byte codec
or mutable table state exists. Its normative inputs are
[ADR 0014](../adr/0014-logical-types-schema-identity-and-evolution.md),
[columnar-batch v1](../formats/columnar-batch-v1.md), and the
[columnar-ingestion architecture](../architecture/columnar-ingestion.md). The implementation must
not broaden their evolution rules.

The CMake target is `chronos_schema`, exported as `chronos::schema`. It depends publicly only on
`chronos::common` and adds no production dependency.

## Public interfaces

| Header | Main contract |
| --- | --- |
| `chronos/common/uuid.hpp` | Neutral 16-byte `common::Uuid` value preserving exact UUID network-order bytes. Nil is a valid neutral value. |
| `chronos/schema/identity.hpp` | Distinct nonzero `TableId`, `ColumnId`, `SchemaId`, and `TabletId` wrappers plus positive, overflow-checked `SchemaVersion`. |
| `chronos/schema/logical_type.hpp` | Exact type codes 1–18, names, decimal parameter validation, and unknown-code rejection. |
| `chronos/schema/utf8.hpp` | Allocation-free UTF-8 scalar-value validation. |
| `chronos/schema/column_definition.hpp` | Owned immutable ID, exact UTF-8 bound name, logical type, and nullability. |
| `chronos/schema/table_schema.hpp` | Immutable schema identity/version/parent, schema-ordinal columns, ordered role/key declarations, lookup, and direct v1 compatibility validation. |
| `chronos/schema/schema_lineage.hpp` | Single-writer linear history, lineage-wide reuse protection, stable shared schema pins, and historical projection plans. |

Factories return `common::Result<T>` and reject invalid construction. Successfully constructed
identifiers, logical types, columns, and schemas therefore carry their local invariants. There are
no public setters. Copying one of these values copies the immutable definition; it does not create a
new identity.

## Identity representation

`common::Uuid` is deliberately neutral because `UUID` column values may include the nil value. It
stores exactly `std::array<std::byte, 16>` in network order and never overlays integers or native
UUID structs.

The four schema identifier types are different template specializations and cannot be implicitly
converted. Their factories reject a nil UUID, so a valid identifier object is always nonzero.
`SystemUuidGenerator` supplies nonnil uninterpreted UUID bytes from the Linux/macOS operating-system
entropy source, while `UuidGenerator` preserves deterministic injection. Assigning those bytes to a
specific catalog/storage identity remains the owning subsystem's responsibility under ADR 0014.

`SchemaVersion` similarly has no zero/default state. Version 1 is the initial value; `next()` checks
`uint64` exhaustion rather than wrapping.

## Type and UTF-8 validation

The logical registry uses the durable type codes from columnar-batch v1. Only `DECIMAL` accepts
parameters: precision is `1..38`, and scale is `0..precision`. Every other type requires both
parameters to be zero. Unknown durable codes report `NOT_SUPPORTED`; an invalid in-process enum or
parameter combination reports `INVALID_ARGUMENT`.

The UTF-8 validator accepts only Unicode scalar-value encodings. It rejects isolated continuation
bytes, C0/C1 overlong leads, overlong three/four-byte forms, UTF-16 surrogate values, values above
U+10FFFF, obsolete five/six-byte forms, and every truncation. Generic UTF-8 permits U+0000, but a
column bound name must be nonempty and cannot contain U+0000. Names compare by exact validated UTF-8
bytes; this layer performs no normalization or SQL case folding. A future binder must supply its
canonical bound spelling.

## Schema validation

`TableSchema::create` validates in a deterministic order:

1. version 1 is parentless; later versions name a different parent;
2. there are `1..4096` columns;
3. current column IDs and exact names are unique;
4. event time identifies one non-null `TIMESTAMP_NS` column;
5. physical ordering, partition references, and shard keys are nonempty;
6. every role/key reference exists and no list contains duplicates;
7. shard and deduplication columns are non-null;
8. physical ordering and partition declarations include event time; and
9. when a deduplication key exists, the shard key is its subset, which guarantees co-routing of all
   versions of one logical identity under the declared key model.

Role/key vectors preserve declaration order. The column vector preserves schema ordinal order.
Neither is sorted by `ColumnId`.

## Successors and lineage history

`validate_v1_successor` compares two already-valid schemas and accepts only a direct transition:

- table identity is unchanged;
- parent is the predecessor schema ID and version increments by one;
- the predecessor column prefix has the same IDs, ordinal positions, types, parameters, and
  nullability;
- role and key declarations are byte-for-value identical; and
- every added tail column is nullable.

Names are the only existing-column field allowed to change. There is no default-expression field,
so an added column necessarily has no default.

`SchemaLineage` adds the history checks that a pairwise comparison cannot provide. It appends only
to the current version, rejects every prior `SchemaId`, and scans all retained versions before
allowing a name. A historical name can return to its original `ColumnId`, but it can never be
assigned to another identity. The no-drop prefix rule also keeps every earlier `ColumnId` present;
the lineage performs an additional historical-ID check for new tail columns.

All validation completes before the lineage vector changes. A failed append leaves size and current
schema unchanged.

## Projection metadata

Historical projection is defined only through a validated lineage. For each descendant schema
ordinal, `SchemaProjection` records either the identical ancestor ordinal or “synthesize NULL.”
Because v1 forbids drop, reorder, type change, and non-null additions, no cast, default evaluation,
or name lookup is necessary. Renames resolve naturally through stable `ColumnId`.

A destination-to-ancestor request is rejected, and unknown schema identities report `NOT_FOUND`.
An identity projection is valid and maps every ordinal to itself.

## Ownership and concurrency

Column and schema values own their strings and vectors. Accessors return const references or spans
valid for the owning value's lifetime. A lineage stores each schema in a
`shared_ptr<const TableSchema>`; returned pointers remain valid after later appends or after the
lineage releases its own reference.

`SchemaLineage` is a cold, single-writer in-memory primitive and is not internally synchronized.
Callers must serialize mutation and safe access to the lineage object. Pinned immutable schema
objects need no synchronization for concurrent reads. No lock-free or memory-ordering claim is made.

## Failure behavior and complexity

Invalid caller definitions return `INVALID_ARGUMENT`; unknown logical type codes return
`NOT_SUPPORTED`; missing projection schemas return `NOT_FOUND`; and version exhaustion returns
`OUT_OF_RANGE`. The library performs no I/O and has no recovery side effects.

Schema construction and lineage append use straightforward scans. With `C` current columns, `V`
versions, and `H` historical column definitions:

- schema uniqueness validation is `O(C²)` and bounded by 4096 columns;
- role validation is `O(C × K)` for referenced keys;
- direct successor validation is `O(C²)` because it checks predecessor-name ownership;
- history/name validation is `O(C × H)`;
- schema lookup is `O(V)`; and
- projection construction is `O(C_descendant)`.

These are cold catalog operations selected for reviewability. A measured need may later justify
indexes without changing semantics.

## Verification and measurement

The schema test target includes self-contained-header checks and deterministic unit/property-style
coverage for exact UUID bytes, nominal typing, version overflow, every logical code, all decimal
precision/scale pairs, hostile UTF-8, schema roles and key references, every allowed/forbidden v1
transition, failed-append atomicity, historical reuse, stable pins, and generated multi-version
projections. Normal warnings, clang-tidy, ASan/UBSan, and TSan configurations apply through the same
CMake helpers as other ChronosDB targets.

No performance number is claimed. Before optimizing these cold paths, measure schema width, lineage
length, construction/append latency, lookup latency, allocations, and projection construction with
the normal benchmark metadata contract.

## Likely review and interview questions

**Why can `Uuid` be nil while an ID cannot?** The UUID logical value domain and an object identity
have different validity rules. Strong factories prevent an invalid object ID without narrowing the
reusable value type.

**Why not identify columns by name or ordinal?** Names can change and ordinals describe layout.
Stable IDs preserve meaning; ordinals remain authoritative only for schema-shaped batch order.

**Why is an added column nullable?** Historical rows have no stored value. NULL is deterministic
without a backfill or versioned default-expression engine.

**Why does projection not inspect names?** Renames preserve `ColumnId`; a name-based projection could
silently bind historical bytes to the wrong column.

**Why keep shared schema pointers?** Snapshots and future heads need immutable schema lifetime that
survives catalog-lineage growth.

**Why is the UUID generator separate from schema IDs?** Entropy acquisition is platform policy;
deciding whether one generated value names a table, schema, tablet, part, or request belongs to the
owning catalog/service operation.
