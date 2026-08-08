# ADR 0058: Shared Snapshot Publication Query Credit

- **Status:** accepted
- **Date:** 2026-08-07
- **Owners:** ChronosDB query-execution and storage-publication maintainers

## Context

A `DatabaseStorageSnapshot` owns one immutable aggregate publication. Every selected
`SnapshotPartImage`, CSEG pin, and mutable-head snapshot retains that same publication so storage
cannot be reclaimed while a source or borrowed chunk remains alive. Earlier scan adapters charged
the complete publication independently through every image, source, and returned CSEG chunk. That
was safe but made query admission depend on part, chunk, and ASOF-alias fanout rather than the bytes
actually retained once.

ADR 0056 provides a copyable `QuerySharedMemoryReservation` whose query credit is released only
when its final copy is destroyed. Applying it to storage pins requires an exact split between the
one shared publication and each image's independently owned bytes; otherwise deduplication could
allow an output to retain uncharged memory.

## Decision

- `SnapshotPartImage` reports its aggregate-publication retained bytes separately from its owned
  image/object/allocation bytes. Their saturating sum remains the conservative legacy total.
- A complete tablet scan reserves the exact `DatabaseStorageSnapshot::retained_buffer_bytes()`
  once. Every durable child, mutable-head child, sequential parent, and backed CSEG output receives
  a copy of that shared reservation when it retains the publication.
- Multi-source snapshot ASOF instantiation creates one such reservation before constructing its
  sources and copies it across every alias. Repeated tables or parts from the same held snapshot do
  not repeat publication credit.
- CSEG pins explicitly declare complete and shared retained bytes. The shared amount cannot exceed
  the complete amount. Shared scan factories require an exact same-query reservation for that
  amount; ordinary factories retain their prior independently sufficient accounting.
- Mutable-head shared factories require a same-query reservation covering the head snapshot's
  publication bytes. Head output is fully materialized and does not borrow its input publication,
  so it retains only its local output reservation after the source is released.
- `AccountedVectorChunk` may own local and shared reservations together. Both must belong to the
  caller's query and their checked sum must cover the chunk's complete retained bytes. Filtering
  and projection transfer both credits with the backing.
- Per-image bytes, decoded/decompressed CSEG buffers, head materialization, selections, operator
  state, output containers, and allocator allowances remain local query reservations. Sharing never
  converts independently retained work into aggregate-publication credit.
- Invalid ownership, insufficient shared coverage, or mismatched publication accounting is
  rejected before an operator escapes. Allocation and budget failures remain
  `RESOURCE_EXHAUSTED`; cancellation and RAII cleanup rules are unchanged.

This decision changes no durable or network format and adds no dependency.

## Consequences

Admission for one held aggregate epoch is now proportional to one publication plus actual selected
images, source state, and live output buffers. A CSEG output can safely outlive its source because
its backing owns both the publication token and a copy of the shared query credit. Head output
releases publication credit eagerly because it owns copied canonical bytes.

The public shared factories are primarily composition boundaries. Callers that cannot prove one
exact aggregate publication continue to use the conservative factories and cannot manufacture a
smaller charge from unrelated pins.

## Affected invariants

This decision supports invariants [9, 10, 11, and 14](../architecture/invariants.md): retained
memory is admitted before ownership escapes, shared credit follows the exact lifetime owner,
failures unwind deterministically, and no cancellation or LIMIT path detaches a pin from credit.

## Validation plan

- Unit tests hold outputs from several parts and chunks and prove the query pays one publication
  while every local retained byte remains covered. Head tests prove copied output no longer retains
  publication credit.
- Hostile tests reject incomplete splits, insufficient reservations, foreign query ownership, and
  mismatched snapshot/image provenance.
- Allocation-failure injection sweeps the new shared control blocks and all composed scan/ASOF
  construction paths with zero leaked credit.
- Fuzzing varies local/shared chunk-credit splits and storage scan shapes. ASan/UBSan and applicable
  TSan runs cover shared lifetime and cleanup.
- Microbenchmarks compare shared reservation fanout with independently reserving the same
  publication at 1, 8, and 64 owners.

## Unresolved questions

Mapped/asynchronous providers still need provider-specific owned-buffer accounting. Cross-query
publication caches cannot share query credit. Correction/delete row-version resolution and
parallel scan scheduling remain separate semantic and scheduling work.

## References

- [ADR 0021](0021-query-resource-accounting-and-cooperative-cancellation.md)
- [ADR 0024](0024-lifetime-pinned-vector-chunk-backing.md)
- [ADR 0027](0027-snapshot-bound-cseg-images-and-part-lifetime-pins.md)
- [ADR 0047](0047-exact-append-only-snapshot-tablet-scan.md)
- [ADR 0055](0055-snapshot-bound-multi-source-asof-instantiation.md)
- [ADR 0056](0056-shared-query-credit-and-bounded-parallel-scheduling.md)
- [Architecture invariants](../architecture/invariants.md)
- [Phase 9 roadmap](../roadmap.md#phase-9--vectorized-execution-and-parallel-scheduling)
