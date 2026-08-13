# ADR 0334: Owned real-CSEG grouped query receiver

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, cluster, and networking maintainers
- **Extends:** [ADR 0332](0332-authenticated-grouped-query-receiver.md),
  [ADR 0333](0333-request-local-real-cseg-grouped-worker-service.md)

## Context

The authenticated grouped receiver borrows its worker. Constructing those values independently can
leave the receiver pointing at a moved or destroyed service, especially when an embedding transfers
an owning server object. A production boundary must establish stable addresses and reverse-safe
destruction before adding TLS/TCP state.

## Decision

`ReplicatedDistributedGroupedQueryReceiver` is a move-only owner with a heap-stable implementation.
It creates the request-local real-CSEG grouped worker first, then constructs the authenticated
receiver with a pointer to that worker. The implementation declares worker before receiver, so
reverse destruction removes the receiver before its dependency. Moving the public value transfers
only the implementation pointer and does not invalidate either internal address.

Configuration accepts the worker's borrowed Manifest storage and authority provider, the node
authorizer, optional committed leader-hint provider, and response-frame limit. It does not accept an
external worker or receiver pointer. `receive` delegates the canonical request and carrier-supplied
peer authentication to the owned receiver and returns only its complete response-frame vector.

The owner is single-threaded and synchronous. It does not authenticate a socket itself and adds no
durable or network format.

## Consequences and validation

One production object now composes peer authorization, exact grouped request decoding, fresh owning
authority acquisition, real-CSEG grouping, complete stream validation, and response encoding with
stable lifetimes. Borrowed storage, providers, and authorizer still must outlive it.

The focused real-Manifest-v2/CSEG service case now constructs the owner, submits one authenticated
canonical grouped request, and exact-decodes the returned terminal group with key and sum `2.5`.
It proves a second fresh grouped context acquisition, invalid packaged configuration rejection,
move-only public ownership, and installed-consumer construction. The complete focused case passes
when its existing loopback fixture is permitted.

ADR 0335 subsequently supplies grouped multi-response TLS ownership. TCP acquisition/listener
ownership, sender/coordinator integration, packaged multi-tablet execution, multi-process failover,
movement-time remote CSEG reads, and broad fault/measurement evidence remain incomplete. No Phase
16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Authenticated grouped query receiver](0332-authenticated-grouped-query-receiver.md)
- [Request-local real-CSEG grouped worker service](0333-request-local-real-cseg-grouped-worker-service.md)
- [Owned real-CSEG distributed-query TCP service](0319-owned-real-cseg-distributed-query-tcp-service.md)
