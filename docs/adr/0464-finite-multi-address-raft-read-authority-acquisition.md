# ADR 0464: Finite multi-address Raft read-authority acquisition

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster transport, networking, query, and Raft maintainers
- **Extends:** [ADR 0463](0463-deadline-bound-raft-read-authority-tcp.md)

## Context

One TCP client owned one address attempt but no retry policy. A caller could accidentally reset its
budget for every address, change the target or correlation between retries, retain a failed
descriptor during backoff, or wait beyond the active connect/handshake/exchange deadline.

## Decision

`RaftReadAuthorityTcpAcquisition` is a move-only single-threaded poll owner for one immutable request
and one finite ordered IPv4 route snapshot. The route contains one target node, one TLS context, and
at most 1,024 unique nonzero endpoints. Its node must equal the request target.

Attempt `n` uses endpoint `(n - 1) modulo endpoint_count`. One positive attempt budget, capped at
1,024, governs the whole logical acquisition. Only `UNAVAILABLE`, `IO_ERROR`, and
`RESOURCE_EXHAUSTED` retry, with positive capped exponential backoff. Each attempt derives the peer
authentication address from its selected endpoint while preserving source, target, group,
correlation, TLS context, authorizer, and carrier bounds.

The owner retains at most one TCP client. A failed or cancelled client is destroyed before backoff
or terminal state. Poll waits are shortened to the current connect, handshake, exchange, or backoff
deadline. Only one complete canonical authority is published; response prefixes from failed attempts
never combine. Metrics distinguish attempts, retries, failures, completion, and active ownership.

## Consequences

Memory remains constant beyond the bounded route vector and one protocol attempt. Total network work
and delay are finite under the explicit attempt, timeout, and backoff bounds. Address rotation cannot
silently rebind authority or expand the logical budget.

DNS and committed-route resolution remain pre-acquisition responsibilities. Concurrent all-group
fan-out, whole-attempt cancellation, daemon integration, and Native query admission remain separate.
No durable, consensus, or wire bytes change.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): retry changes only the address for one exact target;
  source, group, correlation, certificate policy, and authority proof remain immutable.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 11](../architecture/invariants.md): attempts, addresses, timeouts, backoff, descriptors,
  and result ownership are finite and explicit.
- [Invariant 15](../architecture/invariants.md): each selected address is bound into the mutual-TLS
  authentication request before any authority bytes are written.
- [Invariant 18](../architecture/invariants.md): retry never downgrades consistency or combines
  partial proof from different attempts.

## Validation

A real loopback test closes the first candidate listener, serves the second over mutual TLS, and
proves exactly two attempts, one retry, one failure, one completion, and one authority-service call.
A second case rejects duplicate candidates and a route/request target mismatch, then proves
cancellation releases the only active descriptor and remains sticky. Broader suite, sanitizer,
format, and static-analysis evidence is recorded with the implementing commit. Before commit, all
218 normal cluster tests and all 28 cluster allocation-failure tests passed with loopback permission.
All 12 focused authority tests passed under ASan/UBSan with leak detection disabled because Apple's
sanitizer runtime does not support LeakSanitizer. The new production source passed
repository-pinned clang-tidy 18; all changed C++ files passed clang-format 18; and the diff passed
whitespace review.

## Migration or rollback considerations

No format migration. The route is an in-memory snapshot and can be discarded on rollback. A future
daemon must rebuild it from committed metadata rather than persist or silently extend an attempt.

## Unresolved questions

- Define canonical committed endpoint/TLS-context resolution for the daemon's authority route set.
- Bound concurrent all-group polling and cancel every sibling when one group fails.

## References

- [Deadline-bound Raft read-authority TCP endpoints](0463-deadline-bound-raft-read-authority-tcp.md)
- [Finite multi-address Raft observation acquisition](0312-finite-multi-address-raft-observation-acquisition.md)
- [Raft Read Authority Transport v1](../formats/raft-read-authority-transport-v1.md)
