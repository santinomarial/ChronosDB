# ADR 0482: Owned grouped sufficient-state TCP service

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB service, query, cluster, and networking maintainers
- **Extends:** [ADR 0394](0394-owned-definition-bound-vector-aggregate-query-v2-tcp-service.md),
  [ADR 0474](0474-proof-revalidated-grouped-sufficient-state-worker-v2.md), and
  [ADR 0481](0481-bounded-grouped-sufficient-state-tcp-server.md)

## Context

The proof-revalidated grouped real-CSEG worker, authenticated grouped receiver, and bounded TCP/mTLS
server existed separately. The receiver service interface requires a binding-only call followed by
independently fresh execution, while the core worker accepted an explicit coherent authority
context. Every lower layer also borrows the address of its dependency. Production startup needed an
adapter that reacquired authority per call and one owner that established stable addresses and
reverse-safe destruction for the complete inbound stack.

## Decision

`ReplicatedDistributedVectorGroupedAggregateQueryWorkerV2` implements the grouped receiver's worker
service. `bind_authority` and `execute` each acquire a fresh coherent owning context from the existing
Fragment-v2 context provider. Each context retains the Manifest snapshot, schema lineage, placement,
Raft group, and optional local barrier through its synchronous call. Binding enters the core
binding-only boundary and performs no part I/O; execution enters the full proof-revalidating real-
CSEG grouped worker. Nothing is cached between calls.

The adapter validates static query-memory, encoded-byte, retained-configuration, scan, and projection
limits at construction. Exact grouped table limits and authority widths remain validated against the
actual dispatch during binding. Allocation and container failures become `RESOURCE_EXHAUSTED`. The
adapter borrows storage and the context provider and is single-thread-affine.

`ReplicatedDistributedVectorGroupedAggregateQueryTcpServerV2` owns the production inbound grouped-
state stack. Startup creates the production worker inside one heap-stable implementation, constructs
`DistributedVectorGroupedAggregateQueryReceiverV2` with its stable address, then starts
`DistributedVectorGroupedAggregateQueryTcpServerV2` with the stable receiver address. The
implementation declares worker, optional receiver, and optional server in dependency order, so
reverse destruction removes TCP/TLS sessions before the receiver and the receiver before the worker.
Moving the public owner transfers only its implementation pointer.

Configuration accepts borrowed Manifest storage and coherent authority provider, listener/TLS
credentials, connection authenticator, node authorizer, optional leader-hint provider, complete
grouped carrier limits, and finite admission limits. Receiver frame and byte ceilings are copied
from the carrier. Polling, metrics, endpoint access, and shutdown delegate to the bounded server.
External worker and receiver pointers are deliberately not configurable.

## Alternatives considered

- **Require embeddings to pin three objects:** rejected because lifetime safety belongs to the
  production boundary rather than caller convention.
- **Reuse the ungrouped production worker:** rejected because it cannot bind group-key authority or
  return canonical data-dependent grouped frames.
- **Cache binding authority through execution:** rejected because placement or schema authority may
  change between calls and execution must independently fail closed.
- **Store dependencies inline in the public owner:** rejected because moving it could invalidate
  borrowed addresses.

## Consequences

One object composes mutual-TLS authentication, claimed-source/target authorization, exact Fragment-
v2 decoding, fresh complete grouped authority binding, independently fresh real-CSEG execution,
complete state-stream publication, finite admission, metrics, and ordered shutdown. Work and memory
remain bounded by the underlying worker, receiver, transport, and server limits. Borrowed storage,
provider, authentication, authorization, and hint objects outlive the service.

One thread serializes calls, so no synchronization or inter-thread memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): composition changes no application or durable bytes.
- [Invariant 6](../architecture/invariants.md): worker, grouped table, receiver, carrier, and admission
  limits remain finite and independently enforced.
- [Invariant 10](../architecture/invariants.md): group-key and aggregate authority derive from the
  exact committed schema rather than response descriptors.
- [Invariant 11](../architecture/invariants.md): binding and execution independently retain and
  revalidate complete Manifest/Raft authority.
- [Invariant 14](../architecture/invariants.md): authority and grouped response identity remain
  attached to the exact Fragment-v2 dispatch.
- [Invariant 15](../architecture/invariants.md): no binding or worker invocation precedes mutual
  authentication and claimed-source authorization.
- [Invariant 18](../architecture/invariants.md): dependency order, stable addresses, borrowed
  lifetimes, and single-thread affinity are explicit.

## Validation

A real installed temporal CSEG first binds one FLOAT64 key plus COUNT authority. Changing placement
before execution proves the second context is fresh and fails `UNAVAILABLE`; restoring authority
produces two canonical first-seen groups. A moved packaged service then completes nonblocking TCP and
mutual TLS, authenticates both fingerprints, reacquires authority twice, returns the two-group
terminal stream, reports one completed connection, and shuts down deterministically. The moved-from
owner exposes no running server or stale internal address.

The focused real-CSEG service case passes normally and under ASan/UBSan. The complete service suite
passes 107 of 107 and its allocation-failure suite passes 4 of 4; the complete cluster suite remains
green at 242 of 242. Header self-containment, formatting, and whitespace checks pass. LLVM 18 static
analysis remains blocked by the installed macOS 26 libc++ headers after two project-local test
findings were corrected; no project-local finding remains before those compiler errors.

## Migration and rollback

No durable or wire migration exists. Production embeddings can replace separately pinned grouped
worker/receiver/server objects with this owner. Rollback must disable this grouped sufficient-state
endpoint or preserve equivalent stable ownership and independent authority reacquisition; it must
not substitute the row or ungrouped aggregate service.

## Unresolved questions

- Multi-address retries, whole-query cancellation, and all-tablet TCP scheduling.
- Final grouped Native SQL projection/order/limit integration.
- Computed pre-group physical-plan splitting and partition/shuffle routing.

## References

- [Proof-revalidated grouped sufficient-state worker v2](0474-proof-revalidated-grouped-sufficient-state-worker-v2.md)
- [Bounded grouped sufficient-state TCP server](0481-bounded-grouped-sufficient-state-tcp-server.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
