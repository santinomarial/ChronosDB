# ADR 0529: Deadline-bound grouped shuffle result TCP client

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0528](0528-finite-immutable-grouped-shuffle-result-retry.md)

## Context

The immutable result retry owner and already-connected TLS carrier do not establish a network
connection. A reducer needs one owner that preserves attempt identity while a nonblocking connect
is in progress, bounds that phase independently, and does not confuse transport write completion
with coordinator acceptance.

## Decision

Add a move-only, single-event-loop-thread TCP result client. `begin` validates borrowed security
dependencies, the exact result schema, stream limits, endpoint-to-authentication address binding,
attempt identity, the authority-derived reducer, and the stream-encoded reducer/coordinator route
before opening a descriptor. It starts a nonblocking TCP connect with a positive saturating
deadline. Readiness first completes the connect, then constructs the mutual-TLS result carrier on
the same owned descriptor.

The TLS carrier is destroyed before its borrowed TCP descriptor. Any connect, TLS, authentication,
stream, receipt, allocation, or deadline failure closes the descriptor and becomes sticky. The TCP
client reports complete only after the nested carrier validates the exact correlated result
receipt. Retry policy and address rotation remain outside this one-attempt owner.

## Consequences

A reducer can now drive a complete result-return attempt over a real IPv4 TCP connection without a
blocking call or an unbounded connect. A coordinator still needs bounded listener admission, and
the result scheduler must feed failures and receipt success back to the retry owner.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): the attempt route and authority-derived reducer are
  checked before connect.
- [Invariant 11](../architecture/invariants.md): the socket is owned; authority, schema, TLS
  context, authenticator, and authorizer lifetimes are borrowed and documented.
- [Invariant 15](../architecture/invariants.md): connect, handshake, exchange, frames, and bytes all
  have finite limits.
- [Invariant 18](../architecture/invariants.md): only the exact application receipt completes an
  attempt.

## Validation plan

Drive a real loopback TCP connection through mutual TLS, authentication, a multi-frame result, and
the exact receipt. Exercise exact connect-deadline expiry, preconnect endpoint and route rejection,
sticky failure, descriptor closure, header isolation, and owner-allocation failure. Run cluster,
allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire format changes. Rollback removes only one-attempt TCP establishment; callers may
continue using the already-connected TLS carrier.

## Unresolved questions

- Add bounded coordinator-side TCP admission and retained completion capacity.
- Deduplicate exact completed partition attempts at the coordinator.
- Schedule all required remote partition results under one query deadline.

## References

- [Result retry decision](0528-finite-immutable-grouped-shuffle-result-retry.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
