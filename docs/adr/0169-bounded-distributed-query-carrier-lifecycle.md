# ADR 0169: Bounded distributed query carrier lifecycle

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0168](0168-authenticated-distributed-query-transport.md)

## Context

Canonical request and response bytes do not by themselves handle nonblocking stream fragmentation,
coalescing, short writes, loss, or retry. A carrier must never allocate from an unchecked
declared length, consume bytes belonging to the next frame, duplicate a written prefix, or merge a
response from another attempt. Retrying against a hinted leader by mutating a proof-bound fragment
could also change its declared snapshot or placement semantics silently.

## Decision

Separate request and response readers retain fixed arrays at their protocol maxima. Each reader
fills only its fixed header first, verifies magic and header CRC, and validates the declared total
against the format bound before accepting the remaining bytes. One call consumes at most one frame
and reports the exact consumed prefix; a coalesced suffix stays caller-owned. Complete frames pass
through the exact codec. Any header or frame failure is sticky, so a connection closes or replaces
the reader instead of resynchronizing inside untrusted bytes.

`DistributedQueryFrameWriteCursor` accepts only a complete valid request or response, owns those
bytes, and exposes the unwritten suffix. Checked advancement cannot pass the suffix. It is move-only
and a moved-from cursor is complete, preserving one write obligation through connection handoff.

`DistributedQuerySender` owns one immutable group-scoped dispatch. It permits one outstanding
attempt, exact-matches the response's reverse route/query/tablet, retains a successful terminal
exchange, and retries only `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `IO_ERROR`, or an equivalent reported
transport failure. Attempts and exponential backoff are finite and capped; the embedding supplies
monotonic time. Other statuses and exhaustion are terminal.

A leader hint is exposed but never rewrites the sender's dispatch or target. Following it requires
the coordinator to obtain new admission, placement, barrier, and snapshot evidence and explicitly
construct a new sender. This prevents a retry policy from silently changing query consistency.

## Consequences and validation

Each request reader has a 16,772-byte frame buffer and each response reader a 244-byte frame buffer,
independent of peer input. Processing is linear in consumed bytes. The cursor owns one already bounded vector. A
sender retains one bounded dispatch, one optional result, and constant retry metadata; each attempt
encodes one independently owned request for asynchronous carrier use.

Tests enumerate every split point for request, success-response, and failure-response frames;
exercise coalesced request and mixed-length response frames; prove sticky damage and oversized
declared-length rejection at the header boundary; and verify short-write, over-advance, and move
ownership. Sender tests cover correlation mismatch without state mutation, exact-dispatch retry,
capped exponential backoff, advisory leader hints, transport failure, terminal status, attempt
exhaustion, and successful result retention.

Socket/TLS connection admission, readiness registration, deadlines, cancellation delivery, and
multi-node fault simulation remain separate integration work.

ADR 0173 subsequently supplies the outbound single-attempt TLS readiness and deadline owner while
leaving inbound serving, connection establishment, cancellation, and multi-node simulation
separate.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## Migration and rollback

These lifecycle owners do not alter Distributed Query Transport v1 bytes. An embedding may replace
them with equivalent carrier state only if it preserves exact framing, bounded retention, single
write ownership, exact correlation, finite retries, and explicit authority rebinding.

## References

- [Distributed Query Transport v1](../formats/distributed-query-transport-v1.md)
- [Bounded distributed exchange partial I/O](0162-bounded-distributed-exchange-partial-io.md)
- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
- [Bounded outbound distributed-query TLS carrier](0173-bounded-outbound-distributed-query-tls-carrier.md)
