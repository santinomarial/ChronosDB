# ADR 0360: Distinct distributed vector query request

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0168](0168-authenticated-distributed-query-transport.md),
  [ADR 0353](0353-group-scoped-distributed-vector-fragment.md)

## Context

Vector dispatch bytes lacked a node-routed cluster request. Reusing aggregate or grouped request
magic would make different nested payload bounds and semantics ambiguous.

## Decision

Distributed Vector Query Transport v1 begins with a distinct `CHDVREQ1` request. Its fixed 80-byte
header binds nonzero distinct source/target nodes and the exact vector dispatch length and CRC.
Header CRC validation precedes all declared-length use; the complete frame and nested dispatch keep
independent integrity checks. The bounded maximum request is 84,348 bytes.

The exact codec owns the decoded dispatch and changes neither Vector Fragment Dispatch v1 nor any
existing transport. CRC is not authentication. Response framing is implemented separately;
partial-I/O ownership, receiver authorization, retries, TLS, TCP, coordination, and execution
require later contracts.

## Consequences and validation

Two focused cluster cases round-trip a grouped SUM/order/LIMIT dispatch and reject route aliasing,
truncation, reserved bytes, a checksum-valid future version, nested damage under recomputed outer
checksums, and grouped-protocol confusion. Header self-containment and installed consumption cover
the public codec.

The unresolved vector result-schema identity contract blocks general worker execution and is
documented rather than fabricated. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Vector Query Transport v1](../formats/distributed-vector-query-transport-v1.md)
- [Distributed Vector Fragment Dispatch v1](../formats/distributed-vector-fragment-dispatch-v1.md)
- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
