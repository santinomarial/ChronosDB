# ADR 0443: Database-owned mutable worker context

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, Raft, ingest, query, and distributed-systems maintainers
- **Extends:** [ADR 0433](0433-request-local-mutable-vector-query-service.md),
  [ADR 0299](0299-correlated-replicated-read-authority.md)

## Context

The production mutable worker accepted an embedding-owned context provider. The first Native
request integration used a controlled test provider that copied known tablet publications and
echoed the fragment barrier. That proved the carrier and ownership chain but was not a production
authority source: the packaged database already owned the durable Raft runtime, committed metadata
catalog, schema lineages, and current immutable tablet publications needed to revalidate a worker
request.

## Decision

`ReplicatedIngestDatabase` implements
`ReplicatedDistributedMutableVectorQueryWorkerContextProvider`. For one authenticated,
structurally valid leader-linearizable fragment it:

1. requires the serving node to be this database's local node;
2. submits an ordered `ObserveGroupOperation` with the fragment term as an atomic leader-term
   precondition;
3. requires the current observation to retain local leader identity, term, commit/read coverage,
   application coverage, and stable voters;
4. pins one database query snapshot containing a single committed metadata publication and the
   current resident tablet publications;
5. exact-matches database, table, tablet, schema, group, stable placement epoch and voters, applied
   Raft position, and read-index coverage; and
6. returns an owning tablet snapshot, copied immutable lineage and placement, group identity, and
   the admitted barrier to the existing proof-revalidating worker.

The observation does not replace or manufacture the coordinator's correlated barrier. The
authenticated fragment retains that proof; the worker-side observation only fails the request if
the named local leader term or committed authority is no longer current. A later leadership change
does not invalidate a completed read whose pinned publication already covers its read index.

The provider is intentionally correctness-first and acquires a complete database query snapshot
for each request. A narrower snapshot API may replace that cost only with evidence and the same
metadata/publication consistency contract.

## Consequences

The real mutable TCP server can now borrow the database directly instead of a deployment-specific
or test-only provider. Every returned context owns all schema and tablet lifetime needed by
synchronous vector execution. Observation plus full snapshot acquisition adds one durable-runtime
request and work linear in the retained query catalog/tablet set before fragment scanning. No new
durable or wire format is introduced.

The database remains move-only. The stateless provider base therefore permits move construction
and assignment while continuing to forbid copies. The database, server, and worker stay
single-owner/thread-affine; asynchronous durable-runtime submission and completion provide the
existing synchronization edge.

Local-fragment/coordinator composition, daemon TLS routes/listener ownership, reactor-aware
cancellation, fresh all-group rebinding, and multi-process split-leader qualification remain.

## Affected invariants

- [Invariant 4](../architecture/invariants.md): only an exact applied tablet publication is pinned.
- [Invariant 5](../architecture/invariants.md): the current leader term and committed placement are
  independently revalidated before scanning.
- [Invariant 6](../architecture/invariants.md): returned metadata, lineage, group, and tablet state
  come from one retained database snapshot.
- [Invariant 11](../architecture/invariants.md): the context owns its tablet snapshot and schema
  lineage through worker execution.
- [Invariant 18](../architecture/invariants.md): any stale term, placement, group, schema, position,
  or read coverage fails before result publication.

## Validation

The recovered two-tablet integration now passes the actual database as the production worker
provider behind the mutual-TLS TCP server. It directly acquires and checks one context, rejects a
wrong leader term and a wrong applied position, then executes the full Native projected, filtered,
ordered, and limited query and validates the concrete returned row. Header self-containment checks
the provider relationship. The milestone gate runs the full service and service-allocation suites,
ASan/UBSan builds and service suites, installed-consumer compilation, and formatting.

## Migration and rollback

Existing custom providers remain valid. Embeddings with a `ReplicatedIngestDatabase` may pass it
directly as the worker context provider. Rollback removes the interface inheritance and production
`acquire` method without changing fragment, carrier, result, metadata, or Raft bytes.

## References

- [Request-local mutable vector query service](0433-request-local-mutable-vector-query-service.md)
- [Correlated replicated read authority](0299-correlated-replicated-read-authority.md)
- [Distributed Mutable Vector Fragment v1](../formats/distributed-mutable-vector-fragment-v1.md)
