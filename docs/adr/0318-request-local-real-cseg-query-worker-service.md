# ADR 0318: Request-local real-CSEG query worker service

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, query, manifest, and cluster maintainers
- **Extends:** [ADR 0167](0167-proof-revalidated-distributed-aggregate-worker.md),
  [ADR 0174](0174-bounded-inbound-distributed-query-tls-carrier.md)

## Context

The authenticated distributed-query receiver intentionally delegates execution to an
embedding-owned service. ChronosDB already had a proof-revalidating worker that reads exact
generation-pinned temporal CSEGs, but no production service adapter acquired its request-local
Manifest, schema, placement, and barrier context. Each embedding therefore had to reconstruct a
security- and snapshot-sensitive request by hand, and the real TCP gates used deterministic worker
partials instead of the durable CSEG path.

## Decision

`ReplicatedDistributedQueryWorker` implements `DistributedQueryWorkerService`. A borrowed
`ReplicatedDistributedQueryWorkerContextProvider` acquires one coherent owning context for each
exact dispatch: a copyable Manifest-v2 storage snapshot, shared immutable schema lineage, copied
committed tablet placement and Raft group identity, and optional local linearizable barrier. The
service supplies its fixed local node and borrowed Manifest storage, then invokes
`execute_distributed_aggregate_fragment` without rewriting any dispatch authority.

The provider must not assemble a context from publications observed at different logical instants.
The returned context remains alive through synchronous CSEG validation, temporal winner
resolution, filtering, and aggregation, so the Manifest pin and schema lineage outlive every part
view. The service and provider are single-threaded at this boundary; an embedding that admits work
concurrently must serialize or synchronize provider access.

Invalid fixed configuration fails at construction. A provider failure propagates without part I/O.
A missing lineage is rejected, and all mismatched node, group, placement, generation, schema,
barrier, source, part-hash, and temporal-row authority continue to fail at the existing worker
gate. The adapter adds no durable or network format.

## Consequences and validation

The authenticated receiver now has a production bridge to the real temporal-CSEG worker without
weakening its proof boundary or asking the carrier to own metadata policy. Fresh context acquisition
on every dispatch permits current placement or barrier state to reject a request that was valid at
an earlier call.

A focused service test installs a real Raft-sourced temporal Float64 CSEG and Manifest v2, acquires
one request-local context, executes the filtered aggregate, and verifies the exact terminal partial.
It rejects invalid fixed configuration, then changes the provider's group and placement epoch and
proves the same stale dispatch is rejected after fresh acquisitions. A missing lineage also fails
before part I/O. Header and installed-consumer checks cover the public API.

This does not yet prove the adapter behind a real remote query socket, a three-process SQL workflow,
process loss, movement-time remote CSEG reads, or the Phase 16 testing and measurement exit gates.

Invariants 4–6, 10, 11, 14, and 18 apply.

## References

- [Proof-revalidated distributed aggregate worker](0167-proof-revalidated-distributed-aggregate-worker.md)
- [Bounded inbound distributed-query TLS carrier](0174-bounded-inbound-distributed-query-tls-carrier.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
