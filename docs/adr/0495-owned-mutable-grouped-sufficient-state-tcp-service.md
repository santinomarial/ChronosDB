# ADR 0495: Owned mutable grouped sufficient-state TCP service

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB service, query, cluster, and networking maintainers
- **Extends:** [ADR 0490](0490-proof-revalidated-mutable-grouped-sufficient-state-worker.md),
  [ADR 0494](0494-bounded-mutable-grouped-sufficient-state-tcp-server.md)

## Context

The mutable grouped TabletState worker, authenticated receiver, and bounded TCP/mTLS server existed
separately. Every lower layer borrows its dependency address, and binding plus execution must each
reacquire current coherent mutable authority. Caller-managed composition could invalidate borrowed
addresses by moving an object or destroy the layers in the wrong order.

## Decision

Add a move-only production owner whose heap-stable implementation declares the mutable grouped
worker, optional receiver, and optional server in dependency order. Startup creates the request-
local proof-revalidating worker, constructs the receiver with the worker's stable address, then
starts the bounded server with the receiver's stable address. Reverse destruction removes TCP/TLS
sessions before the receiver and the receiver before the worker. Moving the public owner transfers
only its implementation pointer.

Configuration accepts the borrowed mutable context provider, local node, finite grouped-worker
limits, listener/TLS credentials, connection authenticator, node authorizer, optional leader-hint
provider, complete carrier limits, and finite admission limits. Receiver frame and byte ceilings
are copied from the carrier. External worker or receiver pointers are not configurable. Polling,
metrics, endpoint access, and shutdown delegate to the bounded server.

## Consequences

One object now composes mutual-TLS authentication, claimed-source authorization, exact mutable
request decoding, independently fresh authority binding and execution, complete grouped state
publication, finite admission, metrics, and ordered shutdown. Work and memory retain the bounds of
the underlying worker, receiver, transport, and server.

The owner is single-thread-affine and borrows its context provider, authenticator, authorizer, and
optional hint provider. No synchronization or inter-thread memory-ordering algorithm is introduced.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): composition changes no durable or wire bytes.
- [Invariant 6](../architecture/invariants.md): worker, receiver, carrier, and admission limits stay
  finite and independently enforced.
- [Invariant 10](../architecture/invariants.md): grouped authority is derived from the exact current
  TabletState schema rather than response descriptors.
- [Invariant 11](../architecture/invariants.md): binding and execution independently reacquire and
  retain coherent mutable authority.
- [Invariant 14](../architecture/invariants.md): request and response identity remain attached to
  the exact mutable fragment.
- [Invariant 15](../architecture/invariants.md): no worker invocation precedes mutual authentication
  and claimed-source authorization.
- [Invariant 18](../architecture/invariants.md): dependency order, stable addresses, borrowed
  lifetimes, and single-thread affinity are explicit.

## Validation

A real committed TabletState publication is served through the moved production owner and real
nonblocking TCP/mutual TLS client. Both fingerprints authenticate, authority is reacquired once for
binding and once for execution, two canonical groups arrive, one connection completes, shutdown is
idempotent, and the moved-from owner exposes no stale server. Deterministic allocation injection
walks worker, receiver, TLS, listener, fixed-table, and owner construction until success.

The complete service and service-allocation suites pass 109 and 6 tests. The focused production
and allocation cases pass under ASan/UBSan with leak detection disabled. Changed-file LLVM 18
formatting, the warning-as-error build, and whitespace validation pass. Static analysis reports no
project-local finding but cannot complete because the installed macOS 26 libc++ requires compiler
builtins unavailable to LLVM 18. The repository-wide format check retains only the pre-existing
violation in the unchanged grouped TLS v2 header self-containment test.

## Migration and rollback

This is additive and changes no durable or wire bytes. Rollback removes the packaged owner while
retaining its worker, receiver, and bounded server components; any replacement must preserve stable
addresses, fresh authority reacquisition, finite admission, and reverse-safe destruction.

## Unresolved questions

- All-tablet mutable grouped scheduling and atomic Native publication.
- Multi-address retry and whole-query cancellation.
- Partitioned shuffle/skew policy and computed pre-group programs.

## References

- [Proof-revalidated mutable grouped sufficient-state worker](0490-proof-revalidated-mutable-grouped-sufficient-state-worker.md)
- [Bounded mutable grouped sufficient-state TCP server](0494-bounded-mutable-grouped-sufficient-state-tcp-server.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
