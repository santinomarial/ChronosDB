# ADR 0539: Header-first grouped reducer-job control transport

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0537](0537-grouped-shuffle-reducer-job-control-envelope.md) and
  [ADR 0538](0538-bounded-grouped-shuffle-reducer-job-service.md)

## Context

The reducer-job codec accepted only complete byte spans. A nonblocking carrier still needed to
retain fragmented headers and payloads, reject hostile declared lengths before allocation, expose
coalesced suffixes, and keep partial writes alive without borrowing an encoded temporary. Repeating
those responsibilities independently in the standalone TLS carrier and shared query-control
endpoint would create inconsistent admission and partial-I/O behavior.

## Decision

Add one reusable request reader that stores the fixed 128-byte `CHDVGJC1` header inline. After the
complete header arrives, it checks magic, version, header integrity, canonical action fields,
identity, numeric route, nested lengths, hard bounds, and caller bounds before allocating exactly
the declared frame. It then exact-decodes the complete request through the canonical codec. Header
or complete-frame failure is sticky; a successful frame resets the reader for explicit reuse.

`CHDVGJR1` is fixed at 100 bytes, so its response reader uses only inline storage and performs the
canonical exact decode at completion. Both readers return the number of input bytes consumed. A
caller therefore sees any coalesced suffix and can reject a second frame on a one-exchange
connection without dropping bytes silently.

Add distinct move-only request and response write cursors. Each owns its canonical encoded frame,
exposes only the unwritten suffix, rejects completion beyond that suffix, and transfers the sole
remaining write obligation on move. The moved-from cursor is complete.

## Consequences

Every reducer-job carrier can share the same bounded fragmentation and partial-write boundary. No
payload allocation occurs before a complete checksummed header has passed both hard and deployment
limits, and fixed responses allocate no stream buffer.

This transport layer does not open sockets, authenticate principals, impose handshake/exchange
deadlines, or dispatch the job service. Those remain the next mutual-TLS and daemon-composition
milestones.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): one reader publishes only a complete exact-decoded
  request or correlated response.
- [Invariant 10](../architecture/invariants.md): header integrity is checked before declared
  lengths can influence allocation; complete integrity remains required before decode publication.
- [Invariant 11](../architecture/invariants.md): fragmented reads and partial writes have explicit
  move-only ownership.
- [Invariant 14](../architecture/invariants.md): readers accept only the frozen 1.0 request and
  response magics and layouts.
- [Invariant 15](../architecture/invariants.md): inline headers, exact allocation, caller limits,
  and consumed-byte accounting bound stream influence.

## Validation plan

Fragment a PREPARE across its header and payload, require no declared frame before the full header,
and preserve a coalesced suffix. Reject header damage and a caller frame limit before payload
allocation. Fragment a fixed response, verify exact correlation, exercise partial request and
response writes, and prove that moving a cursor transfers only the pending suffix. Inject the first
request-frame allocation failure and require sticky resource exhaustion. Run complete cluster,
allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes an unadvertised stream-ownership helper; no
socket carrier may remain if it depends on these readers or cursors.

## Unresolved questions

- Add one mutual-TLS client/server exchange with exact response-correlation validation.
- Dispatch authenticated reducer-job control through the shared query-control endpoint.

## References

- [Job-control format](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v1.md)
- [Authenticated shared query-control endpoint](0467-authenticated-shared-query-control-endpoint.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
