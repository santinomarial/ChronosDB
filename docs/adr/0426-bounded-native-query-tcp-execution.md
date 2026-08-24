# ADR 0426: Bounded native finite-query TCP execution

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB networking, native-client, and query maintainers
- **Extends:** [ADR 0425](0425-deadline-bound-native-query-tcp-client.md)

## Context

The native finite-query TCP client exposes one descriptor, readiness interest, and active phase
deadline. An embedding still had to bound `poll(2)` by that deadline and a whole-operation budget,
map error readiness without bypassing TLS, retain route/attempt observations, and destroy the
client on cancellation without exposing a partial query result.

## Decision

`NativeQueryTcpExecution` is a move-only, single-threaded poll owner for one exact
`NativeQueryTcpClient`. `begin` rejects an already-expired optional monotonic operation deadline
before the client can open a socket. It retains the complete query, routing, authentication, and
carrier lifecycle through that client.

`poll_once` accepts a nonnegative POSIX-representable caller maximum. The kernel wait is the minimum
of that value, the current connect/handshake/exchange deadline, and the optional operation deadline.
The operation deadline is checked both before and after `poll`; equality cancels before processing
response readiness. The client is driven even after a zero-readiness timeout so its own exact phase
deadline cannot depend on socket activity. `EINTR` is retried through a later call; invalid
descriptors and other poll failures are sticky. Error/hangup readiness is presented only in the
direction currently requested by the carrier.

Completion retains the client and exposes only its complete terminal result. Explicit cancellation
and operation-deadline expiry destroy the client, erase all result ownership, and retain one sticky
`CANCELLED` status. Cancellation after completion and polling a completed owner are idempotent.
Metrics preserve poll calls, readiness events, attempts, redirects, active-client state, and the
last exact route across cleanup.

## Consequences

An embedding can execute one redirected authenticated finite query without writing readiness or
deadline glue. Memory is the bounded client plus one PIMPL and constant poll storage. Each call
performs one constant-size kernel poll and at most one carrier operation. One owner thread
serializes all calls, so no inter-thread memory-ordering argument applies. No protocol or durable
bytes change.

The execution owner does not parse routes, load credentials, format query results, provide a CLI,
or authorize a server to collapse multi-group query leadership into one redirect. Those policies
remain above and below this scheduling layer.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): scheduling never rebuilds SQL or exposes a partial
  result.
- [Invariant 11](../architecture/invariants.md): cancellation and failure release the active client
  while retaining only value diagnostics, route, and metrics.
- [Invariant 14](../architecture/invariants.md): polling changes no Protocol 2 bytes or request IDs.
- [Invariant 18](../architecture/invariants.md): operation and carrier deadlines are both enforced;
  polling cannot bypass authentication or sticky failure.

## Validation

A real-loopback test drives two mutual-TLS query servers exclusively through the execution owner,
fragments each application byte, follows one redirect, publishes one complete result, and retains
two attempts, one redirect, and the final route. A cancellation test proves invalid poll bounds do
not mutate a running owner, cancellation is sticky, the client is released, and no result remains.
The complete network sanitizer suite and installed public-header consumer remain required gates.

## Migration and rollback

This additive owner changes no existing API, wire format, or durable state. Rollback returns poll
scheduling to the embedding while retaining the underlying query client.

## References

- [Deadline-bound native finite-query TCP client](0425-deadline-bound-native-query-tcp-client.md)
- [Bounded native QUORUM_SYNC TCP execution](0416-bounded-native-quorum-ingest-tcp-execution.md)
- [Whole-query TCP cancellation and deadline](0179-whole-query-tcp-cancellation-deadline.md)
- [Native Protocol v2](../protocol/native-v2.md)
