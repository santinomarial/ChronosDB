# ADR 0338: Owned real-CSEG grouped-query TCP service

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, cluster, query, and networking maintainers
- **Extends:** [ADR 0333](0333-request-local-real-cseg-grouped-worker-service.md),
  [ADR 0337](0337-bounded-grouped-query-tcp-server.md)

## Context

The real-CSEG grouped worker, authenticated receiver, and bounded TCP server were individually
implemented, but constructing them independently could leave the receiver pointing at a moved
worker or the server pointing at a moved receiver. Production ownership must establish stable
addresses and reverse-safe destruction for the complete inbound stack.

## Decision

`ReplicatedDistributedGroupedQueryTcpServer` is a move-only, single-threaded owner. Startup first
creates the request-local real-CSEG grouped worker, places it in a heap-stable implementation,
constructs the authenticated receiver with that stable worker address, then starts the bounded TCP
server with the stable receiver address.

The implementation declares worker, optional receiver, and optional server in dependency order.
Reverse destruction therefore shuts down and destroys the TCP/TLS server before the receiver, and
the receiver before the worker. Moving the public owner transfers only the implementation pointer.

Configuration accepts worker storage/current-authority dependencies, listener/TLS settings,
authenticator and node authorizer, optional committed leader hints, carrier limits, and finite
server admission limits. It does not accept an external worker or receiver pointer. Polling,
metrics, endpoint access, and shutdown delegate to the owned bounded server. No durable or network
format changes.

## Consequences and validation

One production object now composes authentication-before-bytes, source/target authorization,
request-local owning authority acquisition, proof revalidation, real temporal CSEG grouping,
ordered multi-response TLS/TCP, finite admission, metrics, and ordered shutdown. Borrowed storage,
authority provider, authentication, authorization, and optional hint dependencies must outlive it.

The focused real-Manifest-v2/CSEG service test now starts this owner on a kernel-selected loopback
port, sends a canonical grouped dispatch through the production grouped TCP client, and exact-reads
the installed-CSEG group key and sum `2.5`. It proves a fresh grouped authority acquisition, both
certificate fingerprints, one completed connection, invalid packaged configuration rejection, and
deterministic shutdown. Header and installed-consumer checks cover the public API.

This is one process and one tablet. Sender/coordinator integration, packaged multi-tablet
execution, moved remote-CSEG reads, process loss/failover, and broad fault/measurement evidence
remain incomplete. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Request-local real-CSEG grouped worker service](0333-request-local-real-cseg-grouped-worker-service.md)
- [Bounded grouped-query TCP server](0337-bounded-grouped-query-tcp-server.md)
- [Owned real-CSEG grouped query receiver](0334-owned-real-cseg-grouped-query-receiver.md)
- [Owned real-CSEG distributed-query TCP service](0319-owned-real-cseg-distributed-query-tcp-service.md)
