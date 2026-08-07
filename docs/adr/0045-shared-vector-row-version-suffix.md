# ADR 0045: Shared Vector Row-Version Suffix

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB storage-query and query-execution maintainers

## Context

Every CSEG v1 granule already contains four authenticated system pages, and every mutable-head row
already retains the corresponding publication metadata. The two query sources previously exposed
only caller-projected user columns. That made their chunks easy to consume independently, but it
left no common physical shape for deterministic base-row ordering, part/head composition, or later
row-version resolution.

Changing either durable representation is unnecessary and prohibited. Always exposing system
columns would also widen existing callers and force mutable-head scans to copy 29 additional bytes
per row when no downstream operator needs version identity. The query boundary therefore needs one
explicit, source-independent, opt-in shape.

## Decision

- `RowVersionScanMode::kOmit` remains the default for CSEG and mutable-head scans.
  `RowVersionScanMode::kAppend` appends exactly four non-null columns after all requested user
  columns, in this order:
  1. WAL ID as `UUID`;
  2. record sequence as `UINT64`;
  3. row ordinal as `UINT32`; and
  4. operation as `UINT8`.
- `VectorRowVersionLayout` is the checked public description of that suffix. Column-count arithmetic
  rejects overflow and invalid scan-mode or column-kind enum values.
- CSEG scans expose the already-decoded mandatory system pages through the chunk backing. The
  suffix therefore borrows the same authenticated owner as the projected granule and does not copy
  system values.
- Mutable-head scans materialize canonical query columns from `HeadSnapshot::row_metadata` for the
  exact acquire-observed publication boundary. UUID bytes are copied verbatim; integers use
  canonical little-endian query storage. The returned chunk owns these buffers and no view points
  into mutable-head storage.
- Exact event-time scan factories may temporarily append a user helper column. Removing that helper
  preserves the row-version suffix and restores it immediately after the caller's requested user
  columns.
- Row-version columns participate in vector column, logical-byte, retained-byte, allocation, and
  query-credit limits. For CSEG output, mandatory system-page logical bytes were already counted;
  exposing them only widens the ordinal container. For head output, all four buffers are planned and
  reserved before materialization.
- The suffix is an in-memory query contract. It does not alter CSEG v1, Columnar Batch v1, WAL v1,
  manifest bytes, mutable-head publication fields, schema identity, or user-visible SQL columns.

## Consequences

CSEG and head chunks can now carry the same physical row identity without changing default scan
shapes. Empty user projections can produce a row-version-only stream, which is useful for merge and
ordering stages. Existing callers remain source-compatible and retain their previous output shape
unless they opt in.

Head scans pay explicit copy and memory costs when the suffix is enabled; CSEG scans reuse mandatory
decoded pages. A configured maximum-column limit that admitted a full-width user projection may
need four additional columns when opt-in mode is used.

This decision supplies identity columns but does not define tablet-wide source composition,
visibility resolution, duplicate suppression, SQL exposure, or the exact hidden-key sequence used
by `ORDER BY`. Those consumers require separate accepted decisions and differential evidence.

## Validation plan

- Unit tests freeze suffix order, types, nullability, canonical values, empty-user projection,
  chunk-boundary behavior, exact-event-time helper removal, invalid enums, overflow, and finite
  column limits.
- Allocation-failure sweeps cover the newly materialized head columns and aggregate helper
  projection; ownership assertions require all query credit to be released.
- CSEG and head scan fuzzers derive opt-in mode from hostile inputs while retaining bounded limits.
  ASan/UBSan and ThreadSanitizer cover materialization and borrowed backing lifetimes.
- Paired microbenchmarks compare omit and append modes. The head benchmark measures materialization;
  the CSEG benchmark measures borrowed exposure with authenticated system-page work still enabled.
- Public-header self-containment, installation, and external-consumer tests cover the exported API.

## Unresolved questions

The exact SQL `ORDER BY` hidden-key lowering, multi-part/head composition, base/delta version
resolution, delete semantics, parallel merge, spill, and optimizer cost rules remain later Phase 9
work.

## References

- [CSEG v1 format](../formats/cseg-v1.md)
- [Mutable-head publication](../architecture/mutable-head-publication.md)
- [ADR 0026](0026-pinned-in-memory-cseg-scan-source.md)
- [ADR 0029](0029-query-accounted-mutable-head-scan-source.md)
- [ADR 0044](0044-query-accounted-bounded-physical-sort.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
