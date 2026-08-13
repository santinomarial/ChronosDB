# ADR 0394: Owned definition-bound vector aggregate query v2 TCP service

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, query, cluster, and networking maintainers
- **Extends:** [ADR 0389](0389-request-local-real-cseg-vector-aggregate-worker-service-v2.md),
  [ADR 0393](0393-bounded-definition-bound-vector-aggregate-query-v2-tcp-server.md)

## Context

The production real-CSEG aggregate worker, authenticated definition-bound receiver, and bounded
TCP/mTLS server were implemented separately. Each lower layer borrows the address of its dependency.
Constructing or moving those objects independently could therefore leave a receiver pointing at a
moved worker or a server pointing at a moved receiver. Production startup needed one owner that
established the complete lifetime and destruction order.

## Decision

`ReplicatedDistributedVectorAggregateQueryTcpServerV2` is the move-only, single-threaded owner of
the production inbound aggregate-v2 stack. Startup validates and creates
`ReplicatedDistributedVectorAggregateQueryWorkerV2`, places it inside one heap-stable implementation,
constructs `DistributedVectorAggregateQueryReceiverV2` with the stable worker address, and then
starts `DistributedVectorAggregateQueryTcpServerV2` with the stable receiver address.

The implementation declares worker, optional receiver, and optional server in dependency order.
Reverse destruction removes accepted TCP/TLS sessions and the server before the receiver, then the
receiver before the worker. Moving the public owner transfers only its implementation pointer and
cannot invalidate internal addresses.

Configuration accepts borrowed Manifest storage and coherent authority provider, listener/TLS
credentials, connection authenticator, node authorizer, optional committed leader-hint provider,
all aggregate carrier limits, and finite admission limits. Receiver response count and byte limits
are copied from the carrier limits. External worker and receiver pointers are deliberately not
configurable.

Polling, metrics, endpoint access, and idempotent shutdown delegate to the bounded server. The owner
adds no durable or network bytes and does not own outbound retry, coordination, finalization, or
broader process lifecycle.

## Alternatives considered

- **Require embeddings to pin three objects:** rejected because lifetime safety is part of the
  production boundary rather than a caller convention.
- **Store dependencies inline in the public object:** rejected because moving it could invalidate
  borrowed addresses.
- **Accept an external aggregate worker:** rejected because the production service must guarantee
  proof-revalidating real-CSEG execution.
- **Cache definition authority between binding and execution:** rejected because the worker must
  acquire and validate fresh authority for each call.

## Consequences

One object now composes mutual-TLS authentication, claimed-source/target authorization, exact
Fragment-v2 decoding, fresh binding authority, independently fresh proof-revalidated real-CSEG
execution, complete state-vector publication, finite admission, metrics, and ordered shutdown.
Borrowed storage, context provider, authentication, authorization, and hint providers outlive it.
One thread serializes calls, so no synchronization or memory-ordering argument is required.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): composition changes no application or durable bytes.
- [Invariant 6](../architecture/invariants.md): worker, receiver, carrier, and admission limits stay
  finite and consistent.
- [Invariant 10](../architecture/invariants.md): definition binding and execution both enter the
  committed schema/placement proof boundary.
- [Invariant 11](../architecture/invariants.md): service lifetime covers every Manifest/CSEG read
  and active session.
- [Invariant 14](../architecture/invariants.md): definition and response identity remain attached to
  the exact Fragment-v2 dispatch.
- [Invariant 15](../architecture/invariants.md): no worker invocation precedes mutual authentication
  and claimed-source authorization.
- [Invariant 18](../architecture/invariants.md): dependency order, stable addresses, borrowed
  lifetimes, and single-thread affinity are explicit.

## Validation plan

Use a real installed temporal CSEG and Manifest-backed authority provider. Construct then move the
public service, complete TCP and mutual TLS through the aggregate client, prove both certificate
fingerprints, observe one binding plus one independently fresh execution, validate the complete
COUNT/SUM definition-bound state vector, and observe one completed connection plus deterministic
shutdown. Reject invalid packaged configuration. Run header self-containment, installed consumption,
formatting, static analysis, ASan/UBSan, and the full serialized suite.

## Migration or rollback considerations

No durable or wire migration exists. Production embeddings can replace separately pinned aggregate
worker/receiver/server objects with this owner. Rollback must disable the packaged service or restore
equivalent stable ownership; it must not substitute the row worker or cache definition authority.

## References

- [Request-local real-CSEG vector aggregate worker service v2](0389-request-local-real-cseg-vector-aggregate-worker-service-v2.md)
- [Bounded definition-bound vector aggregate query v2 TCP server](0393-bounded-definition-bound-vector-aggregate-query-v2-tcp-server.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
