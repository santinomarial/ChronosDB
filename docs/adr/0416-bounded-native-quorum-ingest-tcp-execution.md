# ADR 0416: Bounded native QUORUM_SYNC TCP execution

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** ChronosDB networking, native-client, and ingest maintainers
- **Extends:** [ADR 0415](0415-deadline-bound-native-quorum-ingest-tcp-client.md)

## Context

The native QUORUM_SYNC TCP client exposed one descriptor, readiness interest, and active phase
deadline, but every embedding still had to translate those values into a correctly bounded kernel
wait. Ad hoc loops could oversleep a connect/TLS/exchange deadline, ignore error readiness, accept a
receipt after the caller's whole-operation budget, leak a connection after cancellation, or lose
the terminal route and attempt counts.

## Decision

`NativeQuorumIngestTcpExecution` is a move-only, single-threaded `poll(2)` owner for one exact
`NativeQuorumIngestTcpClient`. `begin` rejects an already-expired optional monotonic operation
deadline before constructing the client or opening a socket. A successful owner starts the client
once and retains the complete transport-independent request, route, authorization, and carrier
lifecycle beneath it.

`poll_once` accepts one nonnegative caller maximum wait representable by POSIX `poll`. Its actual
wait is the minimum of that bound, the client's active connect/handshake/exchange deadline, and the
optional whole-operation deadline. The operation deadline is checked before and after the kernel
wait; equality is expired and cancels before response processing. Every active client is driven
after the wait even without readiness so the carrier's own deadline can expire exactly. `EINTR`
does not fabricate failure; other poll errors and invalid descriptors fail sticky. Error or hangup
readiness is mapped only onto the direction the TLS/connect state currently requests, allowing the
underlying owner to classify the actual transport outcome.

Completion publishes only the TCP client's validated receipt. Explicit cancellation and operation
deadline expiry close the client immediately, retain no result, and expose one sticky `CANCELLED`
status. Cancellation after completion is a no-op; polling a completed owner is idempotent. Poll
calls, readiness events, total attempts, followed redirects, active-client state, and the last exact
route remain observable across terminal cleanup.

The execution never interprets transport loss as replay authority and adds no retry or backoff. The
only reconnect remains the bounded authenticated Protocol 2 redirect inside the composed client.

## Consequences

An embedding can now drive one redirected QUORUM_SYNC operation without writing socket-readiness or
deadline glue. Memory remains the single client's bounded ownership plus one execution PIMPL and
constant poll storage. Each call performs one constant-size kernel poll and at most one carrier
operation. The whole-operation deadline can shorten but never extend a phase deadline. One owner
thread serializes all methods, so no inter-thread memory-ordering argument applies.

The owner does not parse deployment text, own TLS credentials or contexts, resolve DNS, spawn a
thread, manage multiple operations, or add a `chronosctl` command. Strict native route
configuration and packaged command/process composition remain separate work. No durable or network
bytes change.

## Affected invariants

- [Invariant 1](../architecture/invariants.md): only the composed client's exact validated
  QUORUM_SYNC receipt is successful.
- [Invariant 9](../architecture/invariants.md): scheduling never creates replay authority after an
  ambiguous transport outcome.
- [Invariant 11](../architecture/invariants.md): cancellation and failure release the active client
  while retaining only value-owned diagnostics, route, and metrics.
- [Invariant 14](../architecture/invariants.md): poll scheduling changes no Protocol 2 bytes or
  request identities.
- [Invariant 18](../architecture/invariants.md): whole-operation and phase deadlines are both
  enforced; the scheduler cannot bypass the carrier's authentication or failure rules.

## Validation

A focused real-loopback test drives two mutual-TLS native servers exclusively through the execution
owner, fragments every application byte, follows one redirect, verifies the exact append at both
destinations, and publishes the final receipt with two attempts, one redirect, and no active
client. Negative tests prove explicit cancellation is sticky, invalid poll bounds do not mutate a
running owner, an already-expired operation opens no socket, and a requested 100-millisecond wait is
cut short by a 30-millisecond operation deadline. Header self-containment, installed consumption,
warnings-as-errors, clang-tidy, ASan/UBSan, formatting, and the full serialized suite are required
before completion.

## Migration and rollback

Single-operation embeddings may replace manual polling with this owner while retaining their route,
TLS-context, and authorization lifetimes. Rollback restores caller-side readiness/deadline glue and
changes no server, durable, or wire state.

## References

- [Deadline-bound native QUORUM_SYNC TCP client](0415-deadline-bound-native-quorum-ingest-tcp-client.md)
- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
- [Whole-query TCP cancellation and deadline](0179-whole-query-tcp-cancellation-deadline.md)
- [Native Protocol v2](../protocol/native-v2.md)
