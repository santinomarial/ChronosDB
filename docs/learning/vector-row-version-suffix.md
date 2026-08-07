# Shared Vector Row-Version Suffix

## Purpose and phase boundary

ChronosDB stores the same physical row provenance in both durable CSEG granules and live mutable
heads. The Phase 9 query sources can append that provenance to a vector chunk through one opt-in
contract, so downstream operators no longer have to infer identity from scan arrival order.

This is a physical, in-memory interface. It does not make system columns visible to SQL, combine
parts and heads, select a winning version, apply deletes, or change a durable byte. Exact bounded
base-row ORDER BY now consumes WAL ID, record sequence, and row ordinal from this suffix after the
schema DEDUP KEY; operation remains available for later visibility work but is not an ordering tie.

## Public interface

`chronos/query/row_version.hpp` defines:

- `RowVersionScanMode::kOmit`, the source-compatible default;
- `RowVersionScanMode::kAppend`, which enables the suffix;
- `VectorRowVersionColumnKind`, the four fixed fields;
- `VectorRowVersionLayout`, checked ordinals relative to a caller's user-column count; and
- helpers for checked output width and exact logical types.

Both `CsegScanLimits` and `HeadScanLimits` carry the mode. User columns always keep caller order and
precede the suffix:

| Suffix offset | Meaning | Logical type | Nullable |
| --- | --- | --- | --- |
| 0 | WAL ID | `UUID` | no |
| 1 | record sequence | `UINT64` | no |
| 2 | row ordinal inside the command batch | `UINT32` | no |
| 3 | operation code | `UINT8` | no |

The layout helpers reject overflow and invalid enum values. Suffix columns count against the
configured vector width.

## CSEG ownership and lifetime

CSEG v1 requires four system pages in every granule whether a query requests them or not. Projected
reading authenticates, decodes, validates, and accounts for those pages. Append mode merely exposes
their existing `PhysicalColumnView`s after the user projection.

The returned `AccountedVectorChunk` retains the projected-granule backing, its immutable part pin,
and its query reservation. A suffix view is valid exactly as long as that chunk owner is alive.
There is no separate system-value allocation or copy.

## Mutable-head ownership and lifetime

The head snapshot retains race-safe row storage and one immutable publication boundary. Its internal
metadata is not already in canonical vector buffers, so append mode plans and materializes:

- 16 WAL-ID bytes per row;
- 8 little-endian record-sequence bytes;
- 4 little-endian row-ordinal bytes; and
- 1 operation byte.

All four columns are non-null and independently owned by the returned chunk. Materialization calls
`HeadSnapshot::row_metadata` only within the captured row boundary; a later head publication cannot
change the values observed by the scan.

## Exact event-time helpers

An exact event-time factory may need a projected event-time column solely to evaluate the filter.
Its child shape is:

```text
requested user columns, temporary event-time helper, row-version suffix
```

After filtering, `ColumnSubsetOperator` removes the helper and preserves:

```text
requested user columns, row-version suffix
```

Both the temporary and final widths are checked before source construction.

## Accounting and failure behavior

CSEG projected-granule logical bytes already include mandatory system pages in either mode. Append
mode adds exposed-column ordinal capacity to retained accounting. Mutable-head append mode adds 29
logical bytes per row plus owned-column containers and conservative allocation overhead. It reserves
query credit before any output allocation.

Invalid mode/type values are `INVALID_ARGUMENT`; checked width or configured-limit failures are
`RESOURCE_EXHAUSTED`. A pull failure requests cooperative cancellation and releases temporary state
through RAII. Existing source and output ownership rules do not change.

## Complexity and performance evidence

CSEG suffix exposure is O(1) configuration work after the mandatory page decode. Head suffix
materialization is O(rows) and copies 29 bytes per row. Neither path allocates per row. Paired scan
microbenchmarks vary row count and report bytes and pull allocations in omit and append modes; they
are regression baselines, not product throughput claims.

## Testing strategy

Deterministic tests freeze column order, types, raw values, cross-chunk ordinals, empty user
projections, helper removal, and narrow limits. Allocation-failure executables sweep every owned
allocation. Scan fuzzers drive both modes with hostile projections and bounds. Sanitizers validate
the head copy path and CSEG borrowed lifetime. The installed-consumer test proves the public layout
API and exported query target remain usable outside the source tree.

## Likely interview questions

**Why is the suffix opt-in?** Mutable-head exposure copies 29 bytes per row, and most existing
operators do not yet need identity. The default also preserves every established source shape.

**Why not synthesize a single opaque row ID?** The four fields already have frozen durable meanings.
Keeping exact typed components avoids another representation and supports later version policies
without lossy packing.

**Why does CSEG still decode hidden pages in omit mode?** CSEG v1 makes those pages mandatory and the
projected reader validates the complete granule system shape. Hiding a column is not permission to
skip format integrity.

**Does this complete a tablet scan?** No. It supplies a common identity shape. Snapshot composition,
version resolution, visibility, ordering lowering, and delete behavior remain separate work.
