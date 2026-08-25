# ADR 0494: Bounded mutable grouped sufficient-state TCP server

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0481](0481-bounded-grouped-sufficient-state-tcp-server.md),
  [ADR 0492](0492-bounded-mutable-grouped-sufficient-state-mutual-tls.md)

## Context

The mutable grouped TLS server accepted an already connected descriptor. Production embeddings
still had to bound listener admission and accept work, keep accepted descriptors stable while TLS
borrowed them, progress deadlines without readiness, expose lifecycle metrics, and shut sessions
down before their listener.

## Decision

Add a move-only, single-thread-affine POSIX `poll` owner for one exact `CHDMREQ1` request and one
complete empty-or-contiguous `CHDVGRP2` stream per connection. Startup validates TLS credentials,
every outer and nested carrier limit, positive deadlines, connection capacity, and the per-poll
accept budget before binding. It reserves the finite connection table and fixed poll storage.

Each poll progresses every existing carrier once and attempts no more than the configured accept
budget. Full-table accepts are closed and counted immediately. Every admitted session lives behind
a stable `unique_ptr` and declares its descriptor before its TLS carrier, so vector compaction moves
only handles and reverse destruction removes TLS first. The exact accepted peer address enters the
authentication boundary. Shutdown clears sessions before closing the listener and is idempotent.

Accepted, rejected, accept-error, completed, failed, and active counters are exposed; lifetime
counters saturate. The authenticator and mutable grouped receiver are borrowed and outlive the
server. Worker construction, route retry, all-tablet scheduling, cancellation, and process
lifecycle remain separate.

## Consequences

Retained server memory is `O(maximum_connections)` plus each independently bounded session. Poll
work is `O(active_connections + maximum_accepts_per_poll)`. One thread serializes mutation, so no
synchronization or inter-thread memory-ordering algorithm is introduced.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): listener ownership does not reinterpret `CHDMREQ1`
  or `CHDVGRP2` bytes.
- [Invariant 6](../architecture/invariants.md): connection, accept, frame, byte, group, nested-state,
  and deadline limits remain finite.
- [Invariant 10](../architecture/invariants.md): every authenticated request still binds fresh
  mutable grouped authority before execution.
- [Invariant 14](../architecture/invariants.md): the exact accepted peer address enters the
  authenticated route boundary.
- [Invariant 15](../architecture/invariants.md): no application byte is read before mutual TLS and
  every admitted session advances its finite deadlines.
- [Invariant 18](../architecture/invariants.md): stable records, reverse-safe teardown, borrowed
  dependencies, and single-thread affinity are explicit.

## Validation

A real IPv4 loopback case drives the mutable grouped TCP client through mutual TLS into this server,
authenticates both certificate fingerprints, binds and executes once, publishes two canonical
groups, observes one completed session and no retained connection, and proves idempotent shutdown.
A one-slot case proves bounded admission, explicit rejection, invalid configuration and poll
rejection, and ordered shutdown. Deterministic allocation injection classifies TLS, listener,
fixed-table, and owner allocation failures before reaching success.

The complete cluster and cluster-allocation suites pass 259 and 36 tests respectively. Both server
cases and the allocation case pass under ASan/UBSan with leak detection disabled. The warning-as-
error build, changed-file LLVM 18 formatting, and whitespace validation pass. LLVM 18 static
analysis reports no project-local finding but cannot complete because the installed macOS 26
libc++ requires compiler builtins unavailable to LLVM 18. The repository-wide format check retains
only the pre-existing violation in the unchanged grouped TLS v2 header self-containment test.

## Migration and rollback

This is additive and changes no durable or wire bytes. Rollback removes the listener owner while
retaining the connected mutable grouped TLS carrier; any replacement must preserve the same finite
admission, stable lifetime, deadline, peer-address, metrics, and teardown guarantees.

## Unresolved questions

- Production ownership of the mutable grouped worker, receiver, and server.
- Multi-address retry, cancellation, and all-tablet mutable grouped scheduling.
- Partitioned shuffle/skew policy and computed pre-group programs.

## References

- [Bounded grouped sufficient-state TCP server](0481-bounded-grouped-sufficient-state-tcp-server.md)
- [Bounded mutable grouped sufficient-state mutual TLS](0492-bounded-mutable-grouped-sufficient-state-mutual-tls.md)
- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
