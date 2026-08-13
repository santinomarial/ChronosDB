# ADR 0367: Bounded distributed vector Fragment v2 ownership

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0365](0365-schema-bound-distributed-vector-fragment.md),
  [ADR 0355](0355-compatible-multi-tablet-vector-snapshot.md)

## Context

Distributed Vector Fragment v2 had an exact bounded codec but no partial-I/O owner. Callers would
have to buffer a frame as large as 4,344,224 bytes before validation, and short writes had no
canonical progress owner. The compatible multi-tablet vector snapshot also retained only v1
dispatches, so it could not prove that one schema-light result identity applied to every tablet in
the pinned Manifest generation.

Copying the result schema into every retained dispatch is not acceptable. The format permits 4,096
columns and the plan permits 4,096 fragments, so per-tablet duplication would multiply a large
caller-owned descriptor vector by the tablet count without adding authority.

## Decision

Fragment v2 gains a single-owner, nonmovable streaming reader and a move-only write cursor. The
reader retains only the fixed 64-byte header until magic, header CRC32C, version, hard lengths,
caller outer and nested frame limits, and reserved bytes pass. It then allocates exactly the
declared frame, consumes no coalesced successor, exact-decodes both nested values before
publication, and makes frame failures sticky. The cursor owns canonical encoded bytes and advances
only by a checked acknowledgement; a moved-from cursor is complete.

`CompatibleDistributedVectorSnapshotV2` owns one existing `CompatibleDistributedVectorSnapshot`
and one result schema. Its binder first validates the standalone schema, then delegates every
count, order, uniqueness, projection-budget, authority, and Manifest-epoch check to the v1
compatible binder. It derives each tablet's projected physical shapes from the same exact schema
binding used by that dispatch and validates the shared result schema against every plan-ordered
fragment before publishing the owner.

The owner exposes the pinned snapshot, v1 authority dispatches, and shared result schema separately.
Pairing any exposed dispatch with that schema is its authorized Fragment-v2 value. The schema is
owned once; it is never inferred from table identity and is not duplicated across retained
tablets.

## Alternatives considered

- **Retain one owning Fragment-v2 value per tablet:** rejected because it duplicates all result
  names and logical descriptors up to the maximum tablet count.
- **Trust validation against only the first tablet:** rejected because schema evolution or a
  mismatched projection could make later fragments produce a different physical shape.
- **Let transports accumulate arbitrary byte vectors:** rejected because header-first hard limits,
  sticky failure, coalesced-byte ownership, and allocation classification would become caller
  conventions.
- **Change Fragment v2 bytes:** rejected because the accepted wrapper already contains the required
  integrity and length fields.

## Consequences

Fragment-v2 request bytes can now be read and written incrementally without preallocating from an
unchecked length. A multi-tablet caller can retain one Manifest pin and one exact result schema for
all plan-ordered requests. Materializing a request for transport may copy the shared schema into
that one outbound value, but the compatible owner does not retain per-tablet copies.

Cluster request/response carriage, schema-bound result coordination, worker execution,
authenticated lifecycle, and process integration remain separate work.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): Fragment v2 bytes and nested versions remain
  unchanged and explicit.
- [Invariant 6](../architecture/invariants.md): hard and caller bounds pass before declared-length
  allocation; exact nested decoding remains fail-closed.
- [Invariant 10](../architecture/invariants.md): one caller-owned result schema is proved against
  every physical output shape without fabricated catalog identity.
- [Invariant 11](../architecture/invariants.md): all fragments remain pinned to one exact Manifest
  publication.
- [Invariant 14](../architecture/invariants.md): route, proof, projection, and output schema cross
  the worker boundary explicitly.
- [Invariant 18](../architecture/invariants.md): reader, cursor, and compatible snapshot ownership
  and thread-affinity assumptions are explicit.

## Validation plan

Tests enumerate every partial-read split, coalesced frames, sticky damage, future-version and v1/v2
confusion, lower outer and nested bounds, all short-write states, and moved-from cursor behavior.
Allocation-failure injection covers encode, exact decode, and reader ownership. Compatible binding
tests retain a two-tablet Manifest pin, prove one schema against both dispatches, encode both exact
pairings, and reject a nullable-shape mismatch. Header self-containment, installed consumption,
ASan/UBSan, relevant static analysis, and a deterministic libFuzzer smoke campaign are required
before completion.

## Migration or rollback considerations

No durable or wire bytes change. Existing exact v2 callers remain source-compatible. Rollback
removes the new owners and returns callers to exact whole-frame APIs; Fragment v1 and v2 encoded
values require no conversion.

## References

- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
- [Distributed Vector Result Schema v1](../formats/distributed-vector-result-schema-v1.md)
- [ADR 0366](0366-schema-bound-distributed-vector-result-exchange.md)
