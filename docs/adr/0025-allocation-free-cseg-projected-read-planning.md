# ADR 0025: Allocation-Free CSEG Projected Read Planning

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB CSEG, query-execution, and resource-control maintainers

## Context

The accepted CSEG v1 projected reader authenticates metadata at open and applies an aggregate
decoded-buffer limit before page decoding. Its first implementation nevertheless allocated
temporary projection vectors while validating a read request, and it kept the computed byte
requirement internal to `read_granule`. A future storage scan therefore could not inspect exact
granule work before reserving query credit, even though ADR 0021 requires allocation admission to
precede retained query storage.

ADR 0024 supplies the lifetime-pinned `VectorChunk` backing boundary but deliberately does not wire
a CSEG scan. The missing prerequisite is a storage-layer planning value that validates one exact
projection without touching page bodies or allocating result storage. It must preserve selective
page validation, mandatory system-page semantics, schema-lineage synthesis, and every frozen CSEG
v1 byte.

## Decision

- `CsegProjectedReaderView::plan_granule` validates the granule ordinal, projected-column bound,
  destination ordinals, uniqueness, lineage projection, nullable-tail synthesis sizes, and the
  aggregate decoded-buffer limit before any result allocation. A fixed 4,096-bit stack bitmap
  implements uniqueness checking without heap allocation.
- `CsegProjectedGranuleReadPlan` is a small borrowed value. It retains the originating reader
  address and the caller's ordinal span; both must remain alive, unmoved, and immutable through
  execution. It reports granule range, source-page count, synthesized-column count, total decoded
  page count, and exact canonical decoded bytes partitioned into owned and borrowed bytes.
- Every requested existing user page contributes its authenticated descriptor's uncompressed
  length. A raw page is borrowed; a Zstandard page is owned after decode. Every synthesized nullable
  column contributes its exact canonical validity/offset/value bytes as owned storage. All four
  system pages are always included and classified by their descriptor compression.
- Planning reads only already authenticated metadata and pinned schema projection state. It does
  not checksum, decompress, interpret, or claim validity for a page body.
- `read_granule(plan)` rejects a plan from another reader, revalidates the borrowed request without
  allocation, and then performs the same selective page and system-semantic validation as the
  existing read path. The ordinal-taking overload uses the same planner internally. Temporary
  projection vectors are removed; output vectors are populated by bounded repeated passes over the
  borrowed ordinal span.
- Output allocation failures and container length failures become `RESOURCE_EXHAUSTED` results.
  No partially constructed granule escapes.
- The decoded-byte plan is not a complete query-memory reservation. It excludes container and
  allocator bookkeeping, Zstandard context workspace, encoded-part/snapshot pin policy, scheduler
  queues, and a future `VectorChunkBacking` object. The CSEG scan owner must conservatively add and
  document those charges before allocation. This increment does not claim complete Phase 9 memory
  accounting.

No CSEG byte, descriptor, registry, checksum, schema rule, part acceptance rule, or dependency
changes.

## Detailed rationale

Keeping the plan borrowed avoids allocating the very ordinal copy whose admission the plan is
supposed to precede. Query physical plans already own immutable projection ordinals, so their
lifetime naturally covers a granule plan and immediate execution. Revalidation at execution keeps
hostile C++ callers fail-closed for out-of-range or newly oversized borrowed requests; the immutable
borrow remains the semantic contract.

Partitioning decoded bytes by ownership matters because raw pages can become zero-copy vector
views while compressed and synthesized buffers require storage owned by the granule. Counting the
four system pages remains mandatory even when no user column is projected: row identity and
operation semantics are not optional query outputs.

The fixed bitmap is intentionally tied to the accepted 4,096-column schema/CSEG maximum. It is
bounded stack state, avoids quadratic duplicate checks, and introduces no speculative dynamic
container. A future format with a larger column registry must revisit this runtime policy rather
than silently overflowing the v1 planner.

## Alternatives considered

- **Keep planning internal to `read_granule`:** preserves behavior but cannot admit a scan before
  decode and retains temporary pre-admission allocation.
- **Return an owned ordinal vector:** gives independent lifetime but allocates before the caller can
  reserve for the plan.
- **Use quadratic duplicate validation:** is allocation-free but makes the maximum 4,096-column
  projection unnecessarily expensive when the frozen bound permits a fixed bitmap.
- **Count only selected user pages:** undercounts mandatory system decoding and could admit work
  that fails the existing aggregate limit after allocation.
- **Treat raw pages as owned decoded bytes:** is conservative for one total but obscures the
  lifetime-pinning boundary needed by the next scan increment.
- **Implement the CSEG physical scan simultaneously:** would combine schema projection, storage
  pinning, query admission, operator lifecycle, cancellation, and corruption behavior in one
  review. The explicit planner keeps that later integration auditable.

## Consequences

Callers can inspect exact decoded canonical bytes before result allocation and can distinguish the
portion requiring owned page/synthesis buffers from raw borrowed page bytes. Existing callers retain
the ordinal-taking read API and receive the same values and corruption behavior. Valid planning is
linear in projected columns plus four system descriptors, uses constant bounded stack memory, and
does not read page bodies.

Plans are not independent owners. Moving the reader or mutating/destroying the ordinal storage
invalidates a plan, just as mutating encoded bytes invalidates existing borrowed CSEG views. A scan
that needs a longer lifetime must own stable projection configuration and the encoded-part pin.

## Affected invariants

This decision supports invariants [6, 10, 11, 14, and 18](../architecture/invariants.md). Planning
preserves schema-stable projection, includes every mandatory integrity/semantic page, makes borrowed
lifetime explicit, changes no versioned byte, and does not weaken page validation for speed.

## Validation plan

- Unit and deterministic property tests compare plan counts/bytes with authenticated descriptors,
  raw/Zstandard ownership, synthesized nullable tails, direct reads, and caller-order results.
- Hostile tests cover duplicate/out-of-range/oversized requests, foreign-reader plans, empty user
  projections, and mandatory system bytes.
- A dedicated test allocator proves valid planning performs zero `new`/`new[]` calls and injects
  failure at every output allocation until successful execution.
- The existing CSEG part fuzzer plans and executes selected and system-only reads. Sanitizers cover
  borrowed plan and output lifetimes.
- A microbenchmark isolates planning across row counts, compression policies, and empty/one-column
  projections and reports observed planning allocations. Public-header, install, and external
  consumer checks cover the new value and member function.

## Migration or rollback considerations

There is no persisted state and the existing ordinal-taking read API remains available. Rollback
removes only the public planning value and restores internal temporary projection vectors. Any
replacement required by a storage scan must still validate all request bounds and system pages
before allocation and expose a conservative admission requirement.

## Unresolved questions

CSEG file/snapshot pin ownership, complete retained/container/provider accounting, conversion of a
projected granule into `VectorChunkBacking`, scan cancellation, pruning composition, multi-part
ordering, mutable-head scans, and scheduler publication remain later Phase 9 work.

## References

- [ADR 0016](0016-cseg-v1-layout-integrity-and-compression.md)
- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0024](0024-lifetime-pinned-vector-chunk-backing.md)
- [CSEG v1](../formats/cseg-v1.md)
- [CSEG storage implementation](../learning/cseg-storage.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
