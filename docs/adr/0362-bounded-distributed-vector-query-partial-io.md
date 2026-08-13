# ADR 0362: Bounded distributed vector query partial I/O

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0360](0360-distinct-distributed-vector-query-request.md),
  [ADR 0361](0361-correlated-distributed-vector-query-response.md)

## Context

Exact vector query codecs left header accumulation, hostile declared lengths, coalesced successor
ownership, and short-write continuation to each future carrier. Responses may approach 16 MiB, so
fixed maximum-frame storage would also impose an unsuitable per-connection footprint.

## Decision

The request and response readers are noncopyable, nonmovable connection-owned state machines. Each
retains only its fixed header, validates header CRC plus all physical length relationships and hard/
caller frame bounds, then allocates exactly the declared frame. One consume call advances at most
one frame and reports the exact consumed caller prefix. Protocol failure after retained input is
sticky; invalid configured bounds consume nothing.

`DistributedVectorQueryFrameWriteCursor` accepts only an exact request or response, owns those
bytes, exposes only the pending suffix, and rejects over-acknowledgement without state change. Move
transfers the sole write obligation and leaves the source complete. These objects define no socket,
TLS, deadline, retry, or authentication policy.

## Consequences and validation

One focused case enumerates every split of request and response frames, consumes two coalesced
requests only through reported prefixes, retains sticky header damage, rejects lower caller bounds,
and proves short-write suffix, overrun rollback, and moved-from ownership. All five vector transport
cases, header self-containment, and installed consumption cover the public state machines.

Vector coordination is implemented separately. Authenticated receiver/sender ownership, worker
result-schema identity, execution, and process integration remain incomplete. No Phase 16 exit gate
is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Vector Query Transport v1](../formats/distributed-vector-query-transport-v1.md)
- [Distinct distributed vector query request](0360-distinct-distributed-vector-query-request.md)
- [Correlated distributed vector query response](0361-correlated-distributed-vector-query-response.md)
