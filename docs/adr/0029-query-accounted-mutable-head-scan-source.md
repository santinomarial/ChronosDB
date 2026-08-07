# ADR 0029: Query-Accounted Mutable-Head Scan Source

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-execution, mutable-head, schema, and snapshot maintainers

## Context

ADR 0028 composes the durable CSEG subset of one tablet from one exact aggregate database epoch,
but correctly refuses to call that a complete tablet scan while snapshot-visible mutable heads
exist. `HeadSnapshot` already provides an acquire-observed, immutable, owning view of one active or
sealed head generation. Query execution still lacks a bounded physical source for that view.

Mutable-head storage intentionally differs from canonical immutable query vectors. To prevent a
writer publishing one row from racing a reader of an adjacent row, validity and Boolean state use
one independently addressable byte per row. Variable offsets are native in-memory `uint32_t`
objects. `PhysicalColumnView`, by contrast, requires packed LSB-first bitmaps and exact
little-endian offset bytes. Treating head spans as canonical query vectors would be wrong on both
shape and portability grounds.

The first head source also must not silently expose incomplete row-version semantics. A head owns
hidden WAL position, row ordinal, operation, and row-version identity for later base/delta merging,
while the existing CSEG scan exposes projected user columns only. Adding an isolated user-column
source is useful, but composing it with parts before the hidden-system-column contract is accepted
would make duplicate/version visibility ambiguous.

## Decision

- `HeadScanOperator::create()` accepts one owning `HeadSnapshot`, one retained `SchemaLineage`, a
  destination schema, exact target tablet, caller-ordered unique destination user ordinals, and
  finite `HeadScanLimits`.
- Creation requires the snapshot schema to be exactly value-equal to the matching retained lineage
  schema, requires source/destination table and tablet agreement, and builds the accepted
  ancestor-to-descendant `SchemaProjection`. Existing v1 columns keep their exact type/nullability;
  a destination-only nullable tail is synthesized as NULL.
- The source reserves a conservative charge before adopting the snapshot. The charge includes the
  complete generation/publication pin reported by `HeadSnapshot`, projection/ordinal storage,
  source objects, and allocator allowances. An empty head still validates and reserves its source
  configuration, then releases it on the first successful end pull.
- Each pull emits at most `limits.chunk.maximum_rows`, preserving head commit/row order. The source
  plans the exact canonical logical buffers without allocation, checks logical and conservative
  retained limits, and reserves output credit before allocating or copying any output buffer.
- Materialization packs byte-per-row validity and Boolean values into canonical LSB-first bitmaps,
  copies fixed-width canonical bytes, converts native variable offsets into rebased little-endian
  bytes, copies only the selected variable-value range, and creates validated
  `OwnedPhysicalColumn` values. Destination-only columns receive canonical all-null buffers.
- Output chunks own every canonical byte. They do not retain the head pin, so the final successful
  pull may release the source snapshot before returning its chunk. Earlier chunks and the source
  can coexist only while both reservations remain charged.
- Empty user projection remains valid and returns bounded identity selections carrying row
  cardinality. An empty head returns sticky end rather than a zero-row physical vector, because the
  canonical vector model requires a nonzero physical row domain.
- `next()` is thread-affine, polls cooperative cancellation, rejects a foreign query resource
  identity, and returns one chunk, sticky end, or error. Local errors request shared cancellation;
  RAII releases partial buffers, output credit, source credit, and the head pin.
- The source reads only the row and variable-byte boundary captured by its `HeadSnapshot`. Later
  appends or sealing cannot enlarge that boundary, move storage, or change output.

The source exposes user columns only. It does not expose hidden row-version/system columns, apply
exact SQL predicates, prune head rows, merge generations or parts, resolve versions, compose a
complete tablet snapshot, schedule morsels, or spill. No durable, network, schema, CSEG, Manifest,
or WAL format changes, and no dependency is added.

## Detailed rationale

Copying into canonical chunks is required by the accepted memory models, not an optional
optimization choice. Packed mutable bitmaps would permit adjacent-row data races; byte-per-row
query vectors would violate the canonical physical interface; borrowing native offsets would
serialize host representation into a supposedly portable view. A bounded materialization step is
the smallest correct bridge.

The query budget first charges the complete fixed-capacity head generation because one old snapshot
can keep all of that storage alive even when only a few rows are visible. Output chunks then charge
only their owned canonical allocations. Unlike the CSEG source, no duplicate storage pin is needed
inside output chunks because no borrowed head byte escapes.

Using `maximum_rows` as the materialization batch width keeps output bounded without changing the
captured head boundary. It also gives later scheduling a natural morsel input while making no
parallelism claim today. Projection order matches the CSEG source and permits pushdown without
inventing durable identities for physical columns.

## Alternatives considered

- **Borrow head columns directly:** violates canonical bitmap and offset representation and cannot
  represent a row slice without rebasing variable offsets.
- **Compact mutable-head storage in place:** would reintroduce adjacent-row races unless performed
  only after sealing under a separate immutable representation and publication proof.
- **Return one whole generation:** lets a configured head capacity dictate query batch width and
  can exceed generic vector limits.
- **Retain the head pin in every copied output:** is safe but pessimistically charges storage that
  the output no longer references.
- **Expose row-version metadata ad hoc:** would create a different hidden-column shape from CSEG and
  prematurely constrain the required part/head merge contract.
- **Immediately concatenate heads with CSEG parts:** returns physical rows but cannot yet implement
  exact base/delta and row-version visibility semantics.

## Consequences

ChronosDB can now feed one exact active or sealed head publication into the existing vector
pipeline using canonical, query-accounted chunks. Schema evolution, chunk boundaries, old-snapshot
lifetime, cancellation, and allocation failure have explicit behavior.

The path necessarily copies head data and repacks bitmaps/offsets. It scans one generation at a
time and exposes no hidden metadata, so callers must not treat it as a complete logical tablet
source. Later work may add a sealed immutable backing only with an equivalent race/lifetime proof
and measurement showing materialization is a relevant bottleneck.

## Affected invariants

This decision supports invariants [6, 11, 16, and
18](../architecture/invariants.md). Reads stay inside one acquire-observed publication boundary;
the source pin prevents generation reclamation; no reader observes unpublished adjacent-row
writes; and canonicalization weakens neither schema nor memory-order guarantees.

## Validation plan

- Unit tests cover fixed, variable, Boolean, nullable, synthesized-tail, reordered, and zero-column
  projections; multi-chunk output; empty heads; sticky end; exact source/tablet/schema rejection;
  foreign query use; budget failure; and source/output lifetime release.
- A frozen-type property compares all 18 logical types and nullable shapes against their published
  head cells. A deterministic boundary property varies row and chunk widths and compares every
  emitted cell with the pinned snapshot.
- An old snapshot is scanned after later appends to prove its row and byte frontiers do not grow.
  Existing deterministic mutable-head interleavings plus ThreadSanitizer retain the underlying
  release/acquire evidence.
- Exhaustive allocation failure covers source projection/state construction and every canonical
  output allocation. `chronos_head_scan_fuzz` varies hostile projections, limits, cancellation,
  destination schema, chunk boundaries, pulls, and cell access over valid published heads.
- A microbenchmark measures one four-column head-to-canonical pull at 64, 1,024, and 65,536 rows,
  reporting bytes and allocations with source construction excluded. Public-header, install, and
  external-consumer checks cover the exported API.

## Migration or rollback considerations

There is no persisted state. Rollback removes `HeadScanOperator` without changing mutable-head or
query-vector bytes. Any replacement must retain exact publication pinning, pre-allocation credit,
canonical bitmap/offset conversion, schema-tail NULL synthesis, bounded chunks, and deterministic
cleanup.

## Unresolved questions

Canonical hidden system columns shared by CSEG and heads, row-version/base-delta merge semantics,
complete aggregate snapshot composition, exact vector predicates, typed expression outputs,
shared snapshot credit, parallel scheduling, mapped providers, and spill remain later Phase 9 work.

## References

- [ADR 0014](0014-logical-types-schema-identity-and-evolution.md)
- [ADR 0020](0020-bounded-vector-chunk-representation.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0024](0024-lifetime-pinned-vector-chunk-backing.md)
- [ADR 0028](0028-pruned-multi-part-snapshot-cseg-scan.md)
- [Mutable-head publication](../architecture/mutable-head-publication.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
