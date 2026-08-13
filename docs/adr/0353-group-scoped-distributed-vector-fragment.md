# ADR 0353: Group-scoped distributed vector fragment

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0352](0352-canonical-distributed-vector-plan-intent.md)

## Context

Vector Plan Intent v1 has no query, schema, snapshot, route, or read authority. Reusing Aggregate
Fragment v1 would give its required single aggregate-input field a false meaning and would still
require a separate group envelope. The new vector path can include group scope from its first
version without changing any accepted aggregate/grouped bytes.

## Decision

Distributed Vector Fragment Dispatch v1 is one checksummed frame containing query/database/table/
tablet/schema/group identity, snapshot generation, serving node, applied/observed positions,
placement epoch, exact consistency proof, optional event-time bounds, a unique destination-schema
projection, and one exact nested Vector Plan Intent v1.

The 232-byte header CRC covers every identity, length, count, route, proof, flag, and predicate
before they drive interpretation. Exact outer length is derived from bounded projection and nested
plan lengths. The complete outer CRC passes before projection allocation; nested plan integrity and
limits pass independently. Nested input indices are validated against the exact projection count.
The maximum outer frame is 84,264 bytes.

The codec validates structural proof relationships, not runtime provenance. A decoded frame cannot
be executed until a production binder derives it from one coherent committed metadata/Manifest
authority and a worker revalidates current local group, route, schema, position, and storage state.
CRC32C is not peer authentication.

## Consequences and validation

The vector path now has non-confusable, group-scoped request bytes without abusing the frozen
aggregate format. Two focused tests round-trip bounded-stale authority, projection, event bounds,
multi-key grouped plan, ordering, and LIMIT, then reject truncation, unknown outer version, nested
plan damage despite a repaired outer CRC, lower projection limits, out-of-projection plan indices,
lag contradiction, and nil group identity. Public header and installed-consumer checks cover the
surface.

Committed-authority construction, compatible metadata/group-backed binding, fragment partial I/O,
and exact node-routed request framing are implemented separately. Worker execution, response
coordination, authenticated transport, and process integration remain incomplete. No Phase 16 exit
gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Vector Fragment Dispatch v1](../formats/distributed-vector-fragment-dispatch-v1.md)
- [Distributed Vector Plan Intent v1](../formats/distributed-vector-plan-intent-v1.md)
- [Authority-bound distributed fragment construction](0166-authority-bound-distributed-fragment-construction.md)
