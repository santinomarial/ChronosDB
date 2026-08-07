# ADR 0047: Exact Append-Only Snapshot Tablet Scan

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB storage-query, Manifest-publication, and query-execution maintainers

## Context

The aggregate database publication atomically selects durable Manifest parts and every later
sealed/active mutable head for a tablet. Existing query factories scan all selected durable parts
or one head independently, with identical optional row-version suffixes and exact event-time
filtering. Treating either source as a complete tablet would omit visible rows.

The currently accepted write and durable formats contain only append-row operations. Corrections,
tombstones, and winner selection are Phase 13 concepts with no accepted mutation encoding or
visibility algorithm. Inventing such rules in Phase 9 would be less exact than concatenating the
already disjoint append-only publications.

## Decision

- `create_snapshot_tablet_scan` consumes one exact `DatabaseStorageSnapshot`, its matching bounded
  CSEG plan and selected images, one retained lineage, and finite CSEG/head/composition limits.
- The factory re-proves database, WAL, generation, table, and tablet provenance. It requires the
  CSEG and head sources to use the same row-version mode and validates the published tablet identity.
- One durable aggregate child is followed by every sealed head in publication order and the active
  head. This is deterministic physical traversal for the serial implementation, not SQL result
  order. Without `ORDER BY`, only the emitted multiset is contractual.
- The plan's exact event-time predicate is applied by the existing CSEG prune-then-filter path and
  independently by every head exact-filter path. Helper columns remain private to each child; the
  caller sees one uniform projection and optional row-version suffix.
- Current durable and head validators accept only append-row operations. The aggregate publication
  proves that head rows are strictly later than the durable boundary and that flush replacement
  atomically substitutes one exact sealed generation. Consequently concatenation emits every
  current visible append exactly once; no version winner or key merge is needed for the accepted
  operation set.
- A finite head-count limit and retained-configuration limit are checked before child construction.
  The sequential parent reserves query credit before allocating/adopting its child container;
  children retain their existing independent source, image, snapshot, and output charges.
- Pull remains thread-affine and cooperative. Child error or foreign query ownership cancels the
  query; completed children and final parent state release credit immediately.

Future correction/delete operations are unsupported until their durable and in-memory encodings,
snapshot winner rules, scalar oracle, and merge operator are accepted. This decision changes no
durable bytes, WAL/Manifest/CSEG format, publication atomic, or SQL ordering contract.

## Consequences

ChronosDB now has an exact complete tablet source for the full currently accepted append-only
storage surface, including durable-only recovery epochs and snapshots with sealed plus active
heads. Bound SQL source selection is still a separate planning integration.

The source is eager and serial and conservatively duplicates aggregate publication credit in its
durable children. It performs no physical key merge and makes no arrival-order promise. Mapped or
asynchronous providers, shared pin credit, and parallel morsel scheduling need later decisions.

## Alternatives considered

- **Continue requiring callers to compose sources:** risks missing a head, applying asymmetric
  predicates, or mixing epochs.
- **Merge by event time or physical key:** neither defines current visibility, and unordered SQL
  does not require it.
- **Invent correction/tombstone semantics:** rejected because no accepted operation representation
  exists and Phase 13 owns bitemporal behavior.
- **Share one publication charge immediately:** the resource API has no divisible lifetime credit;
  independent conservative reservations are safe and already established.

## Affected invariants

This decision supports invariants [6, 11, 13, 16, and 18](../architecture/invariants.md). One
aggregate epoch supplies every child, publication pins survive every borrowed read, exact filters
preserve truth, and failures unwind all query ownership.

## Validation plan

- Unit and independent-multiset tests cover durable, sealed, and active rows, shared suffix values,
  exact head-only predicates, helper removal, durable-only epochs, hostile mode/identity/limit
  shapes, cancellation, and sticky end.
- Allocation-failure sweeps cover the new parent reservation/container and reuse exhaustive CSEG
  and head child construction/materialization sweeps.
- The CSEG/head fuzz targets continue to vary the two authenticated child paths and suffix modes;
  composition fuzz coverage exercises hostile bounded source shapes.
- A microbenchmark measures complete head-only aggregate serial pull overhead with source
  construction excluded. Public-header, install, external-consumer, sanitizer, and
  full repository checks cover the exported API.

## Migration or rollback considerations

There is no persisted-state migration. Rollback removes the aggregate factory and limits. Any
replacement must retain exact epoch provenance, all published heads, uniform child shape, exact
predicate behavior, finite admission, cancellation, and complete pin/credit cleanup.

## Unresolved questions

SQL source selection, future correction/delete row-version resolution, generated logical identity,
shared publication credit, mapped/asynchronous providers, parallel scheduling, and spill remain
later Phase 9 or Phase 13 work.

## References

- [ADR 0028](0028-pruned-multi-part-snapshot-cseg-scan.md)
- [ADR 0029](0029-query-accounted-mutable-head-scan-source.md)
- [ADR 0031](0031-exact-prune-then-filter-snapshot-cseg-scans.md)
- [ADR 0032](0032-exact-event-time-mutable-head-scans.md)
- [ADR 0045](0045-shared-vector-row-version-suffix.md)
- [Aggregate database storage publication](../learning/database-storage-publication.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
