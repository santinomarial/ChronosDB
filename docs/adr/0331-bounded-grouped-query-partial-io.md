# ADR 0331: Bounded grouped query partial I/O

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0169](0169-bounded-distributed-query-carrier-lifecycle.md),
  [ADR 0330](0330-distinct-grouped-float64-query-transport.md)

## Context

Exact grouped request/response codecs do not assign ownership across fragmented reads, coalesced
frames, or short writes. A carrier must not allocate from an unchecked peer length, consume a
successor frame, retry after damage inside the same byte stream, or duplicate a written prefix when
connection ownership moves.

## Decision

Separate grouped request and response readers retain fixed arrays at their protocol maxima: 16,816
and 252 bytes. Each reader fills only the fixed header first, verifies its magic and CRC, and accepts
the declared complete length only when it is one of the bounded shapes permitted by the grouped
protocol. One call consumes at most one frame and reports the exact consumed prefix, leaving any
coalesced suffix caller-owned. Complete bytes pass through the exact codec. Header or complete-frame
failure is sticky.

`DistributedGroupedQueryFrameWriteCursor` accepts only one exact valid grouped request or response,
owns that vector, and exposes only its unwritten suffix. Advancement is checked before mutation. It
is move-only, and a moved-from cursor is complete so the write obligation has one owner.

These types are single-owner and unsynchronized. The embedding must serialize access and replace a
failed reader with the failed connection rather than attempt byte resynchronization.

## Consequences and validation

Peer input cannot expand retained memory beyond the two fixed maxima. Each byte is copied at most
once into reader storage and complete decoding remains linear. The cursor retains exactly one
already-bounded vector.

Three focused stream cases enumerate every request split and every split of partial, terminal-only,
and failure responses; prove coalesced successor ownership; reject a checksum-valid oversized
request at the header boundary; prove sticky failure; and cover short-write, over-advance,
move-from-complete, damaged-frame, request, and response cursor behavior. Together with the two
codec cases, all five grouped transport cases pass. The installed-consumer gate references both
readers and the cursor.

ADR 0332 subsequently supplies authenticated receiver/service dispatch. Production service
adaptation, multi-response closure, retry/correlation state, TLS/TCP ownership, packaged
multi-tablet grouped execution, and broad fault/measurement evidence remain incomplete. No Phase 16
exit gate is claimed.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Query Transport
  v1](../formats/distributed-grouped-float64-query-transport-v1.md)
- [Bounded distributed query carrier lifecycle](0169-bounded-distributed-query-carrier-lifecycle.md)
- [Distinct grouped FLOAT64 query transport](0330-distinct-grouped-float64-query-transport.md)
