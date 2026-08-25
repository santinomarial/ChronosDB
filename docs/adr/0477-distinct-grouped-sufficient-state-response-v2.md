# ADR 0477: Distinct grouped sufficient-state response v2

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, cluster, protocol, and security maintainers
- **Extends:** [ADR 0387](0387-definition-bound-vector-aggregate-query-response-v2.md),
  [ADR 0470](0470-canonical-multi-key-grouped-sufficient-state-exchange.md), and
  [ADR 0476](0476-portable-pinned-grouped-sufficient-state-execution-owner.md)

## Context

The portable grouped execution owner accepted canonical grouped-state frames, but no node-routed
response format could carry one such frame without type confusion. Reusing the row response would
label sufficient states as Native rows; reusing the ungrouped response would omit key authority and
accept the wrong nested format. A later authenticated carrier also needs header-first allocation,
exact route/correlation identity, advisory leader hints, and query-accounted variable decode.

## Decision

Grouped sufficient-state requests reuse the exact schema-bound Fragment-v2 `CHDVREQ2` request.
Responses use a distinct `CHDVGRP2` major-2 envelope. Its fixed 112-byte header carries total length,
source/target nodes, query/tablet identity, status, payload kind/length/CRC, optional leader/placement
hint, reserved-zero fields, and header CRC. One successful response nests exactly one checksummed
Distributed Vector Grouped Aggregate Exchange v1 frame; failure responses carry no payload. A final
CRC covers the complete outer frame.

Every API requires the exact ordered grouped key and aggregate authority. Exact decode additionally
requires a query resource context, so variable key payload and nested extrema retain credit for the
decoded response lifetime. Nested query/tablet identity must equal the outer envelope. Hard grouped
frame bounds and caller limits are enforced before payload allocation.

The header-first reader owns at most one exact frame, consumes at most one response, leaves a
coalesced successor caller-owned, and makes frame failure sticky. The move-only write cursor owns
validated bytes and exposes only the unwritten suffix. This increment defines no receiver,
authentication, socket, retry, scheduling, or coordinator policy.

## Consequences

The outer frame is 116 through 67,108,980 bytes. Outer header, payload, complete-frame, and nested
checksums are intentionally redundant trust boundaries. Decode performs linear work in frame bytes
and may retain query-accounted variable state. One caller serializes reader/cursor mutation, so no
inter-thread memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): a distinct versioned checksummed frame identifies
  grouped query responses.
- [Invariant 6](../architecture/invariants.md): outer, nested, key, aggregate, state, and query-memory
  bounds are independent.
- [Invariant 10](../architecture/invariants.md): decode cannot occur without exact key and aggregate
  authority.
- [Invariant 14](../architecture/invariants.md): route, query, tablet, and nested correlation are
  exact.
- [Invariant 18](../architecture/invariants.md): reader/cursor bytes and decoded reservations have
  explicit owners.

## Validation

Functional cases prove the frozen magic, successful variable-key COUNT round trip, correlated
failure and leader hint, ungrouped-response type confusion, future version rejection, nested damage,
authority drift, correlation rejection, lower reader bounds, all frame splits, coalesced suffixes,
sticky damage, and move-only short writes. Allocation injection sweeps encode, exact decode, and
reader frame ownership and proves decoded key credit returns to zero. All five focused cases pass
under normal and ASan/UBSan builds. The complete cluster suite passes 228 of 228 and the complete
cluster allocation-failure suite passes 29 of 29. Header self-containment, formatting, and
whitespace checks pass. LLVM 18 static analysis remains blocked by its incompatibility with the
installed macOS 26 libc++ headers and reported no project-local finding before those compiler
errors.

## Migration and rollback

This adds a wire type without changing `CHDVREQ2`, row responses, ungrouped aggregate responses, or
the nested grouped exchange. Rollback must disable grouped remote state responses; it must not route
them through an existing response magic.

## Unresolved questions

- Authenticated receiver-side authority binding and all-or-none worker stream publication.
- Finite sender retry, mutual-TLS/TCP attempt ownership, and multi-tablet scheduling.
- Process integration and compatibility qualification across deployed versions.

## References

- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
