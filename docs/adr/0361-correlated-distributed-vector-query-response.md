# ADR 0361: Correlated distributed vector query response

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0350](0350-canonical-distributed-vector-batch-exchange.md),
  [ADR 0360](0360-distinct-distributed-vector-query-request.md)

## Context

The vector request had no distinct response/failure frame. Reusing aggregate/grouped response bytes
would not safely admit the variable all-type vector exchange or preserve protocol separation.

## Decision

`CHDVRSP1` binds reverse route, query, tablet, fixed status code, optional advisory leader hint, and
zero or one exact Distributed Vector Exchange v1 payload. Success requires one payload; failure
requires none. Header CRC protects payload kind and length before slicing, and payload plus complete
CRCs remain independent of the nested exchange's own integrity. Nested query/tablet identity must
exact-match the response header. The bounded maximum is 16,777,192 bytes.

The exact codec returns value-owned exchange bytes and changes no existing format. It defines no
stream sequencing beyond retaining the nested positive sequence/terminal values; coordination,
partial I/O, authentication, and socket ownership remain separate.

## Consequences and validation

Two focused response cases round-trip terminal-only success, every failure status, and an advisory
leader hint, then reject encoder-side correlation mismatch, unknown payload kind, checksum-valid
header/payload correlation mismatch, and nested damage under recomputed outer checksums. The four
request/response cases, header self-containment, and installed consumption cover both exact codec
directions.

General vector worker execution remains blocked on an explicit output-schema identity contract.
Partial-I/O, authenticated receiver/sender ownership, coordination, and process integration remain
incomplete. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Vector Query Transport v1](../formats/distributed-vector-query-transport-v1.md)
- [Canonical distributed vector-batch exchange](0350-canonical-distributed-vector-batch-exchange.md)
- [Distinct distributed vector query request](0360-distinct-distributed-vector-query-request.md)
