# ADR 0493: Deadline-bound mutable grouped sufficient-state TCP client

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0432](0432-bounded-mutable-vector-query-tcp-ownership.md),
  [ADR 0480](0480-deadline-bound-grouped-sufficient-state-tcp-client.md), and
  [ADR 0492](0492-bounded-mutable-grouped-sufficient-state-mutual-tls.md)

## Context

The mutable grouped TLS client accepted an already connected descriptor. Callers still had to keep
the exact mutable attempt, grouped authority, and query resources attached while nonblocking TCP
connect was pending, prove `SO_ERROR` completion, and transfer them atomically into TLS.

## Decision

Add a move-only, single-thread-affine outbound owner for one immutable mutable-grouped attempt.
Before opening a socket, `begin` exact-decodes `CHDMREQ1`, exact-matches its target, validates
grouped authority against the mutable fragment, validates all carrier limits and positive connect
timeout, and requires the authentication IPv4 address to match the remote endpoint.

The owner retains the nonblocking `TcpSocket`, attempt bytes, both authority vectors, and query
resource context while connecting. Write readiness calls `finish_connect`; only proved success
permits TLS construction and one-way transfer into the mutable grouped TLS client. Connect and
carrier failures are sticky. Failure destroys TLS before closing the descriptor, and declaration
order preserves the same teardown order normally.

The TLS context, authenticator, and node authorizer are borrowed and outlive the client. Retry,
address rotation, listener admission, multi-tablet scheduling, cancellation, and process lifecycle
remain outside this one-attempt owner.

**Retrospective (2026-08-25):** [ADR 0494](0494-bounded-mutable-grouped-sufficient-state-tcp-server.md)
adds the complementary finite listener, stable accepted-session lifetime, metrics, and ordered
shutdown. Production stack ownership and all-tablet scheduling remain separate.

## Consequences

No connection can start from invalid mutable/grouped authority, and no successful writable socket
can bypass `SO_ERROR`, mutual TLS, certificate authentication, or target authorization. Retained
memory is one attempt, authority, shared query resources, connection state, and the private bounded
TLS response prefix.

One event-loop thread serializes mutation, so no synchronization or inter-thread memory-ordering
algorithm is introduced.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): acquisition preserves exact `CHDMREQ1` authority.
- [Invariant 6](../architecture/invariants.md): endpoint, target, mutable proof, grouped authority,
  and query resources stay attached through TLS transfer.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 14](../architecture/invariants.md): request and `CHDVGRP2` bytes remain versioned,
  checksummed, and exactly correlated.
- [Invariant 15](../architecture/invariants.md): connect/handshake/exchange time and every grouped
  count/byte/nested limit are finite and validated before acquisition.
- [Invariant 18](../architecture/invariants.md): reverse-safe socket/TLS teardown and borrowed
  dependency lifetimes are explicit.

## Validation

A real IPv4 loopback test progresses nonblocking connect into mutual TLS, authenticates both peers,
binds and executes the exact mutable grouped worker once, and publishes the complete two-group
stream. A focused negative case proves exact sticky connect expiry and authority/limit rejection
before acquisition. Deterministic allocation injection walks validation and owner construction
until success and classifies every earlier failure as resource exhaustion.

The complete cluster and cluster-allocation suites pass 257 and 35 tests respectively. Both client
cases and the allocation case pass under ASan/UBSan with leak detection disabled. The full
warning-as-error build, changed-file LLVM 18 formatting, and whitespace validation pass. LLVM 18
static analysis reports no project-local finding but cannot complete because the installed macOS
26 libc++ requires compiler builtins unavailable to LLVM 18. The repository-wide format check
retains only the pre-existing violation in the unchanged grouped TLS v2 header self-containment
test.

## Migration and rollback

This is additive and changes no durable or wire bytes. Rollback removes the outbound TCP owner while
retaining the connected-session carrier and transport policy.

## Unresolved questions

- Production ownership of the mutable grouped worker, receiver, and server.
- Multi-address retry, cancellation, and all-tablet mutable grouped scheduling.
- Partitioned shuffle/skew policy and computed pre-group programs.

## References

- [Bounded mutable grouped sufficient-state mutual TLS](0492-bounded-mutable-grouped-sufficient-state-mutual-tls.md)
- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Network security boundary](../learning/network-security-boundary.md)
