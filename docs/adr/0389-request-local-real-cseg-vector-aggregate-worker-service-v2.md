# ADR 0389: Request-local real-CSEG vector aggregate worker service v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and service maintainers
- **Extends:** [ADR 0375](0375-proof-revalidated-schema-bound-vector-row-worker-v2.md),
  [ADR 0384](0384-proof-revalidated-vector-aggregate-worker-v2.md),
  [ADR 0388](0388-authenticated-vector-aggregate-query-receiver-v2.md)

## Context

The authenticated aggregate receiver requires one definition binding before execution and an
independent proof-derived definition vector after execution. The core real-CSEG aggregate worker
already repeats structural, placement, snapshot, schema, group, and read-barrier gates, but its
definition binding was embedded in the full part-loading path. Calling full execution twice would
scan and aggregate every CSEG twice. Reconstructing definitions in the service would duplicate
authority logic and risk weaker validation.

## Decision

The core worker exposes `bind_distributed_vector_aggregate_worker_definitions_v2`. It runs the exact
structural Fragment-v2 encode validation, worker-limit checks, ungrouped-mode gate, local placement,
snapshot generation, tablet/schema identity, Raft group and durable position, read-barrier, projected
column shape, result-schema, and aggregate-width gates used by execution. It returns only the exact
definition vector and does not enumerate or load part bodies. Full execution calls the same internal
binding primitive before empty-tablet handling or real-CSEG part loading.

`ReplicatedDistributedVectorAggregateQueryWorkerV2` implements the receiver's production worker
service. Both `bind_definitions` and `execute` acquire a fresh coherent owning context from the
existing Fragment-v2 context provider. Each context retains the Manifest snapshot, schema lineage,
placement, group, and optional local barrier through its complete synchronous call. Binding enters
the binding-only core boundary; execution enters the full proof-revalidating real-CSEG worker.
Nothing is cached between calls, so a placement or authority change fails execution and cannot reuse
the earlier definition result.

The adapter validates all static resource limits at construction and contains allocation/container
exceptions as `RESOURCE_EXHAUSTED`. It borrows storage and the context provider, is single-threaded,
and does not own receiver, TLS, socket, retry, or process lifecycle.

## Alternatives considered

- **Execute and discard states during definition binding:** rejected because it doubles CSEG I/O
  and aggregation work.
- **Derive definitions directly in the service:** rejected because service code would need to
  duplicate core placement/snapshot/schema proof gates.
- **Cache the first authority context through execution:** rejected because the receiver requires
  an independent second proof and explicit definition comparison.
- **Cache definitions across requests:** rejected because definitions belong to exact dispatch and
  current local authority.

## Consequences

Binding performs metadata/schema work but no part I/O. Execution pays the same proof cost it did
before and then loads each real CSEG once. The two service calls intentionally acquire authority
twice. The shared core helper prevents their definition rules from drifting; placement changes
between calls fail closed.

## Affected invariants

- [Invariant 10](../architecture/invariants.md): projected input and result shapes derive from the
  exact committed schema rather than response descriptors.
- [Invariant 11](../architecture/invariants.md): both binding and execution revalidate the complete
  Manifest/Raft authority tuple.
- [Invariant 14](../architecture/invariants.md): definition identity remains attached to the exact
  Fragment-v2 dispatch.
- [Invariant 15](../architecture/invariants.md): binding authority is not treated as execution
  authority after a state change.
- [Invariant 18](../architecture/invariants.md): context, storage, schema, and borrowed provider
  lifetimes are explicit.

## Validation plan

Use a real installed temporal CSEG to bind COUNT/SUM definitions and independently execute their
states. Change committed placement between the calls and prove execution returns `UNAVAILABLE`,
then restore authority and verify exact rows, sufficient states, sequencing, and definitions.
Compile the public headers, run allocation-oriented core tests, formatting, static analysis,
ASan/UBSan, installed consumer, and the full serialized suite.

## Migration or rollback considerations

The helper and service are additive and change no durable or network bytes. Rollback removes the
adapter and binding entry point; aggregate receivers then have no production worker and must remain
disabled rather than reuse the row service.

## References

- [Proof-revalidated vector aggregate worker v2](0384-proof-revalidated-vector-aggregate-worker-v2.md)
- [Authenticated vector aggregate query receiver v2](0388-authenticated-vector-aggregate-query-receiver-v2.md)
- [Distributed Vector Aggregate Exchange v1](../formats/distributed-vector-aggregate-exchange-v1.md)
