# ADR 0067: Bounded native client session

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** networking subsystem
- **Supersedes:** none

## Context

Server tests built frames by hand, which could agree with server bugs and did not exercise an
independent client lifecycle. Phase 10 requires a client/server test implementation with the same
fragmentation, short-write, finite-limit, request identity, and result-stream obligations.

## Decision

`NativeClientSession` is a portable movable owner around bounded `ConnectionBuffers`. It queues one
hello, negotiates a payload ceiling, assigns positive strictly increasing request IDs only after
output admission succeeds, retains a finite active-request vector, and owns partial input/output.
It validates server-only direction, response identity and request kind, ingest requested durability,
canonical query batches, `END_STREAM` before `QUERY_END`, errors, and PONG.

Cancellation is locally terminal after its canonical frame is admitted; repeating a cancellation
for any issued ID remains legal. A later result for that identity fails the client closed. Any input
decode or lifecycle error clears retained buffers and active requests. Request-ID exhaustion and
output/in-flight saturation fail without reuse.

## Alternatives considered

- **Hand-written packet helpers only:** rejected because they do not model client lifecycle.
- **Blocking socket client in the core API:** rejected because transport policy and portable
  protocol state are separate.
- **Reuse server state with inverted types:** rejected because client durability and stream
  obligations differ.

## Consequences

The same client can drive real Linux sockets or deterministic byte-stream tests. Socket polling and
timeouts remain caller-owned. The client is not a connection pool or production CLI.

## Validation

Tests cover split/coalesced frames, partial output, query and ingest completion, cancellation,
wrong direction, premature end, exhaustive creation/request allocation failure, installed use, and
real Linux epoll interoperability.

## References

- [Native Protocol v1](../protocol/native-v1.md)
- [ADR 0062](0062-bounded-connection-buffer-ownership.md)
- [ADR 0065](0065-self-describing-query-result-batches.md)

