# ADR 0026: Pinned In-Memory CSEG Scan Source

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB query-execution, CSEG, and storage-snapshot maintainers

## Context

ADR 0024 permits a `VectorChunk` to borrow canonical physical columns through one immutable
lifetime backing. ADR 0025 exposes allocation-free projected-granule planning so query credit can
be acquired before CSEG page decode. The remaining narrow integration is a real physical source
that owns the encoded bytes, reader metadata, projection configuration, decoded granule, and query
credit through every pull/error/cancellation path.

A bare `ByteView` cannot satisfy this boundary: raw CSEG pages borrow it, and LIMIT may destroy the
source while its last returned chunk remains live. The source also cannot assume the generic 2,048
row chunk default because frozen CSEG v1 granules contain as many as 65,536 rows. Database-wide
snapshot acquisition, pruning, multiple part order, and mutable heads are separate planning
concerns and should not be hidden inside a single-part codec adapter.

## Decision

- `CsegPartPin` couples one complete immutable in-memory CSEG byte view to a trusted opaque shared
  owner and a conservative retained-byte charge. The owner keeps the exact bytes alive and
  immutable; its charge includes the full owner allocation and any external file/snapshot pin it
  represents. Null owners, empty images, and charges below the visible image length are invalid.
- `CsegScanOperator::create` accepts the query resource identity, part pin, retained schema lineage,
  destination schema, target tablet, caller-ordered unique destination ordinals, and finite reader
  and chunk limits. It reserves source credit before opening the projected reader or adopting the
  pin. The conservative source charge covers the part owner, an encoded-size upper bound for CSEG
  descriptor arrays, metadata-validation scratch, schema projection entries, ordinal capacity,
  source objects, and allocator allowances.
- Durable descriptor sizes are compile-time lower bounds for the corresponding decoded descriptor
  objects. This makes the complete encoded image length a conservative metadata-allocation bound;
  destination-only nullable tail projection entries are charged separately.
- One pull checks query identity and cancellation, plans exactly one granule without allocation,
  checks logical and retained chunk limits, and reserves its conservative output charge before any
  page checksum, decompression, or synthesized-buffer allocation.
- The output charge includes the complete part-pin charge, planned owned decoded bytes, result
  containers, the identity selection, the backing ordinal map, backing objects, and allocator
  allowances. Charging the pin again while the source also owns it is deliberate conservative
  accounting: returned chunks may outlive the source, and multiple retained chunks cannot share one
  untracked credit.
- A private immutable backing owns the `ProjectedCsegGranule` and a copy of `CsegPartPin`. It exposes
  only projected destination user columns. Its logical count nevertheless includes requested and
  synthesized user buffers plus all four decoded mandatory system pages. Its retained count adds
  owned decode/synthesis capacities, result-container capacities, backing overhead, and the part
  pin. Raw page bytes remain zero-copy views into the pinned image.
- The source emits one CSEG granule per chunk. `CsegScanLimits` therefore defaults the finite row
  bound to the frozen 65,536-row CSEG maximum and uses explicit logical/retained byte ceilings.
  Callers may choose smaller bounds, in which case an oversized granule fails before decode. This
  does not change the generic `VectorChunk` defaults.
- The final successful pull releases source metadata/pin credit before returning; the output
  backing remains sufficient for every cell. Successful end is sticky. Wrong-query use, local
  corruption, limits, and allocation failures fail without a chunk and request cooperative
  cancellation. A pre-cancelled pull does no page work and leaves live ownership until ordinary
  unwind.
- Source construction and output allocation/container failures are converted to
  `RESOURCE_EXHAUSTED`. Runtime `VectorChunk` and `AccountedVectorChunk` validation prevents an
  underreported backing or undercharged result from escaping.

`CsegPartPin` is an internal trusted-provider boundary like `VectorChunkBacking`; it cannot prove
that an arbitrary C++ owner actually owns a supplied span. A storage adapter must construct it from
an immutable loaded image and retain the applicable database snapshot/retention owner. This
increment does not acquire an aggregate database snapshot or claim multi-part query visibility.

No CSEG byte, registry, checksum, schema rule, dependency, or durable/network format changes.

## Detailed rationale

Keeping the part pin in both source state and each output backing makes lifetime correctness local.
LIMIT may destroy the child immediately, a caller may retain several chunks, and cancellation may
unwind either side first; ordinary shared ownership still keeps every raw page valid. Conservative
duplicate charging trades some admission headroom for an auditable invariant and can later be
replaced only by a reviewed shared-credit transfer mechanism.

Granule-sized physical row domains avoid copying or rebasing packed validity bits and variable
offsets solely to meet a smaller default. They remain bounded by the durable format and caller byte
limits. A later morsel scheduler may divide granule work or materialize smaller outputs after it has
an explicit gather/builder contract.

## Alternatives considered

- **Return copied owned query columns:** is lifetime-safe but doubles raw-page movement and retains
  decoded and copied buffers simultaneously.
- **Let the source own a bare byte span:** fails when LIMIT or caller destruction releases the source
  before the output chunk.
- **Charge only decompressed pages:** ignores raw image pins, result containers, selection, and
  ordinal maps.
- **Share one untracked pin reservation among all chunks:** avoids duplicate charge but lets a chunk
  outlive the reservation owner.
- **Force every CSEG granule into 2,048-row chunks:** requires packed-bitmap slicing and variable
  offset rebasing/copying not supplied by the current canonical view API.
- **Acquire the database snapshot and scan every part here:** combines catalog/snapshot composition,
  pruning, head/part ordering, and physical decode in one source and obscures their distinct proofs.

## Consequences

`chronos_query` now has its first real storage-backed physical source and publicly depends on
`chronos_cseg`. It can feed the existing shape boundary, Boolean filter, stable projection, and
LIMIT pipeline without copying raw CSEG payloads. Source and output credit/pins release by RAII on
success, corruption, exhaustion, cancellation, or downstream destruction.

The opaque pin contract remains trusted, allocator allowances are conservative rather than a
portable exact allocator-metadata measurement, and duplicate pin charging can reduce concurrency.
An aggregate scan plan must still acquire one stable database snapshot, create exact part pins,
compose all selected parts and heads in deterministic order, apply safe pruning, and define shared
snapshot-credit policy.

## Affected invariants

This decision supports invariants [6, 9, 10, 11, and 18](../architecture/invariants.md). Typed schema
binding precedes output, selected/system page integrity remains mandatory, every borrowed page has
an owning pin, credit follows every output lifetime, and zero-copy behavior weakens no checksum or
visibility rule.

## Validation plan

- Unit tests cover raw/Zstandard output, values, explicit end, cross-query rejection, pre-cancel,
  row/logical/retained limits, reservation-before-corruption ordering, corruption cancellation,
  LIMIT composition, and pin/credit release after source destruction.
- Fixed-seed properties compare every row across raw/Zstandard and varied row/granule boundaries.
- A dedicated failing allocator injects every source-open and output allocation failure until
  success and proves `RESOURCE_EXHAUSTED` plus exact credit cleanup.
- A coverage-guided fuzzer combines arbitrary and authenticated mutated CSEG images, hostile
  projections, cancellation, pulls, and selected-cell access.
- Microbenchmarks isolate one raw/Zstandard granule pull at 64, 1,024, and 65,536 rows and report
  bytes and allocations. Public-header, installation, external-consumer, and sanitizer checks cover
  the exported API and transitive CSEG target.

## Migration or rollback considerations

There is no persisted state. Rollback removes the source and pin APIs without changing CSEG files or
the core physical operator protocol. Any replacement must reserve before page allocation, keep raw
bytes alive through returned chunks, count hidden system pages, preserve sticky end/cancellation
cleanup, and enforce exact query-resource identity.

## Unresolved questions

Aggregate database snapshot acquisition, Manifest loaded-image adaptation, part-specific versus
epoch-wide retention credit, safe pruning composition, multi-part ordering, mutable-head sources,
visibility/version resolution, parallel morsels, scheduler publication, typed outputs, and spill
remain later Phase 9 work.

## References

- [ADR 0008](0008-custom-sql-and-vectorized-execution.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0022](0022-pull-based-physical-operator-lifecycle.md)
- [ADR 0024](0024-lifetime-pinned-vector-chunk-backing.md)
- [ADR 0025](0025-allocation-free-cseg-projected-read-planning.md)
- [CSEG v1](../formats/cseg-v1.md)
- [CSEG scan source](../learning/cseg-scan-source.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
