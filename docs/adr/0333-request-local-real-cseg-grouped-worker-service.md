# ADR 0333: Request-local real-CSEG grouped worker service

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, manifest, and cluster maintainers
- **Extends:** [ADR 0318](0318-request-local-real-cseg-query-worker-service.md),
  [ADR 0328](0328-proof-revalidated-grouped-float64-worker.md),
  [ADR 0332](0332-authenticated-grouped-query-receiver.md)

## Context

The authenticated grouped receiver has an embedding-owned worker seam, but no production adapter
acquires the request-local Manifest, schema, placement, group, and barrier authority needed by the
real-CSEG grouped executor. Reusing the ungrouped provider interface would add a new pure virtual
method and break its accepted embedding contract.

## Decision

`ReplicatedDistributedGroupedQueryWorker` implements `DistributedGroupedQueryWorkerService`. Its
distinct `ReplicatedDistributedGroupedQueryWorkerContextProvider` acquires the existing owning
`ReplicatedDistributedQueryWorkerContext` for one exact grouped dispatch. The service borrows fixed
local-node and Manifest-storage configuration, retains the returned context through synchronous
execution, and invokes `execute_distributed_grouped_float64_fragment` without rewriting dispatch
authority.

The provider must acquire one coherent publication instant; its Manifest snapshot, shared schema
lineage, copied placement/group, and optional local barrier outlive every part view for the call.
Provider failure and missing lineage fail before worker storage access. All local authority and CSEG
validation remain in the shared proof-revalidating worker.

The ungrouped provider ABI and service are unchanged. Both adapters are single-owner at this seam;
concurrent embeddings must provide serialization or synchronized providers.

## Consequences and validation

The authenticated grouped receiver can now be configured with a production service that reaches
real generation-pinned temporal CSEG data through one request-local owning context. This adds no
durable or network format and no alternate authority path.

The existing focused real-Manifest-v2/real-CSEG service case now also constructs the grouped
adapter, freshly acquires grouped authority, and returns one exact terminal group with key and sum
`2.5` after the event filter. It covers invalid fixed configuration and public move/ownership
traits. The complete focused case passes when its existing loopback fixture is permitted, and the
installed-consumer gate references grouped service construction.

The adapter is not yet packaged into a grouped TLS/TCP server, and sender/coordinator integration,
multi-process execution, movement-time remote CSEG reads, and broad fault/measurement evidence
remain incomplete. No Phase 16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, and 18 apply.

## References

- [Request-local real-CSEG query worker service](0318-request-local-real-cseg-query-worker-service.md)
- [Proof-revalidated grouped FLOAT64 worker](0328-proof-revalidated-grouped-float64-worker.md)
- [Authenticated grouped query receiver](0332-authenticated-grouped-query-receiver.md)
