# ADR 0312: Finite multi-address Raft observation acquisition

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0309](0309-deadline-bound-raft-observation-tcp-client.md),
  [ADR 0305](0305-bounded-dns-multi-address-query-routing.md)

## Context

The one-attempt observation TCP client deliberately owned no retry or address-selection policy. A
caller with several resolved addresses could accidentally reset its retry budget per address,
change the authenticated target between attempts, sleep past a carrier deadline, or retain a failed
attempt while starting another.

## Decision

`RaftObservationTcpAcquisition` is a move-only single-threaded poll owner for one exact observation
request and one finite ordered IPv4 address snapshot. Its route has one node ID, one TLS context,
and at most 1024 unique nonzero endpoints. The route node must equal the immutable request target.

Attempt `n` uses endpoint `(n - 1) modulo endpoint_count`. One positive retry budget, capped at
1024 attempts, governs the logical acquisition rather than each address. Only `UNAVAILABLE`,
`IO_ERROR`, and `RESOURCE_EXHAUSTED` outcomes retry, with positive capped exponential backoff.
Every attempt derives peer-authentication address metadata from the selected endpoint while
retaining the same source, target, group, correlation identity, TLS context, and node authorizer.

The acquisition owns at most one TCP client. It opens no descriptor during construction, publishes
only a complete correlated observation, and destroys a failed or cancelled attempt before backoff
or terminal publication. Poll waits are shortened to the active connect, handshake, or exchange
deadline exposed by the underlying client. Metrics distinguish attempts, retries, failed attempts,
successful attempts, and active ownership.

## Consequences and validation

Resource ownership is constant beyond the bounded address vector and one protocol attempt. Total
retry delay and network work are finite under the configured attempt and timeout limits. Address
rotation cannot change observation authority, expand the budget, combine partial responses, or
weaken certificate/principal checks.

A focused real loopback test closes the first candidate listener, serves the second candidate over
mutual TLS, and proves exactly two attempts, one retry, one failed attempt, one completed attempt,
one service invocation, and one correlated result. A second test rejects duplicate candidates and a
route/request node mismatch, then proves cancellation releases the sole active descriptor. These
tests require approved host execution where sandbox policy forbids loopback bind.

Blocking DNS resolution remains a pre-acquisition embedding responsibility. Leader/follower
fan-out, complete pair validation and selection, packaged bounded-stale construction, broader
multi-node faults, and process integration remain incomplete.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Deadline-bound Raft observation TCP client](0309-deadline-bound-raft-observation-tcp-client.md)
- [Bounded DNS and multi-address distributed query routing](0305-bounded-dns-multi-address-query-routing.md)
- [Authenticated Raft observation transport](0306-authenticated-raft-observation-transport.md)
