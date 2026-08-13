# ADR 0330: Distinct grouped FLOAT64 query transport

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0168](0168-authenticated-distributed-query-transport.md),
  [ADR 0327](0327-group-scoped-grouped-float64-dispatch.md),
  [ADR 0328](0328-proof-revalidated-grouped-float64-worker.md)

## Context

The grouped dispatch and worker result now have canonical inner bytes, but ungrouped Query Transport
v1 carries only one 128-byte terminal aggregate. Grouped execution can emit multiple 136-byte
partials or a distinct 64-byte terminal-only value, so changing the frozen ungrouped payload shape
would create ambiguous compatibility.

## Decision

Distributed Grouped FLOAT64 Query Transport v1 uses distinct `CHDGREQ1` and `CHDGRSP1` magics. Its
request wraps one exact group-scoped grouped dispatch under the same bounded source/target,
header/payload/complete-integrity pattern as the ungrouped protocol.

Each response correlates reverse route, query, tablet, status, optional advisory leader hint, and
one explicit payload kind. Successful kind 1 wraps one grouped partial; successful kind 2 wraps the
empty-stream terminal; failures carry no payload. The nested value must exact-match the response
query and tablet. Sequence and tablet-stream closure remain the grouped coordinator's authority.

Fixed-header integrity is checked before peer lengths or payload kinds control slicing. Exact
physical lengths are 116, 180, or 252 response bytes; requests are bounded at 16,816 bytes. Inner
dispatch/exchange/terminal integrity and semantic checks remain authoritative. CRC32C is not
authentication.

This decision implements exact value-owned codecs only. It does not yet define stream readers,
write ownership, authenticated receiver dispatch, multi-response connection closure, retries, TLS,
or TCP lifecycle ownership.

## Consequences and validation

Ungrouped transport bytes and behavior remain unchanged, and neither protocol decoder accepts the
other's magic. Codec work and owned memory are linear and bounded by the declared frame maxima.
Malformed type/length combinations fail before nested decoding; malformed nested bytes retain their
status classification.

Two focused cases exact-round-trip the request, grouped partial, terminal-only value, failure, all
status numbers, canonical signed-zero key, correlation, and advisory hint. They reject ungrouped
confusion, checksum-valid future versions, nested request/response damage, mismatched correlation,
and checksum-valid payload-kind substitution. The public header is self-contained and the
installed-consumer gate references both directions.

Authenticated receiver/service ownership, bounded partial I/O, sender/coordinator integration,
packaged multi-tablet grouped execution, and broad fault/measurement evidence remain incomplete.
No Phase 16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Query Transport
  v1](../formats/distributed-grouped-float64-query-transport-v1.md)
- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
- [Proof-revalidated grouped FLOAT64 worker](0328-proof-revalidated-grouped-float64-worker.md)
