# ADR 0319: Owned real-CSEG distributed-query TCP service

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, cluster, query, and networking maintainers
- **Extends:** [ADR 0176](0176-bounded-distributed-query-tcp-server.md),
  [ADR 0318](0318-request-local-real-cseg-query-worker-service.md)

## Context

The request-local worker service could execute a proof-bound dispatch from a real temporal CSEG,
and the cluster TCP server could authenticate and serve an embedding-owned worker. There was still
no production owner that kept the concrete worker, authenticated receiver, and TCP server at stable
addresses in their required lifetime order. The focused TCP tests therefore used deterministic
partials rather than the durable storage path.

## Decision

`ReplicatedDistributedQueryTcpServer` is a move-only, single-threaded owner of the complete inbound
production stack. Startup first validates and creates `ReplicatedDistributedQueryWorker`, allocates
one stable implementation, constructs `DistributedQueryReceiver` with a pointer to that worker,
then starts `DistributedQueryTcpServer` with a pointer to the stable receiver.

The implementation declares worker, optional receiver, and optional server in that order. Reverse
destruction therefore shuts down and destroys the TLS/TCP server before the receiver, and the
receiver before the worker and its borrowed storage/context-provider dependencies. Moving the
public owner transfers only the implementation pointer and cannot invalidate either borrowed
internal address.

The wrapper retains the existing server's listener, TLS, connection, accept-per-poll, framing,
deadline, and shutdown limits. It also retains the receiver's authentication and source-node
authorization ordering and the worker service's request-local Manifest/schema/placement/group/
barrier acquisition. Optional leader hints remain advisory. Configuration does not accept an
external receiver or worker pointer, preventing accidental substitution between these owned
layers. The wrapper adds no durable or network format.

## Consequences and validation

One production object now serves authenticated distributed aggregate requests from validated
Raft-sourced temporal CSEGs. Its retained memory and per-poll work remain those documented by the
underlying bounded TCP server plus one worker, receiver, and implementation allocation.

A focused approved-host service test installs a real Manifest-v2 CSEG, starts the packaged server
on a kernel-selected loopback port, completes mutual TLS with exact principal-to-node
authorization, sends one proof-bound canonical dispatch, and accepts the exact correlated terminal
aggregate response. It verifies the remote partial equals the direct real-CSEG result, both
certificate fingerprints were observed, one connection completed, and shutdown succeeds. Invalid
packaged configuration fails before binding. Header and installed-consumer checks cover the public
API.

This is a one-process loopback composition gate. It does not prove a moved CSEG on another process,
native SQL/data-plane routing, process loss/failover, multi-tablet remote CSEG execution, or the
Phase 16 testing and measurement exit gates.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Bounded distributed-query TCP server](0176-bounded-distributed-query-tcp-server.md)
- [Request-local real-CSEG query worker service](0318-request-local-real-cseg-query-worker-service.md)
- [Distributed Query Transport v1](../formats/distributed-query-transport-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
