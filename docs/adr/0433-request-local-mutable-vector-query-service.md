# ADR 0433: Request-local mutable vector query service

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, query, ingest, cluster, and security maintainers
- **Extends:** [ADR 0429](0429-distinct-proof-bound-mutable-vector-fragment.md),
  [ADR 0432](0432-bounded-mutable-vector-query-tcp-ownership.md)

## Context

The mutable fragment row worker, authenticated receiver, mutual-TLS carrier, and TCP owner existed
as separate boundaries. The receiver still depended on an embedding-supplied worker, and moving
independently constructed objects could invalidate the borrowed worker or receiver addresses.
Production inbound execution also needed to reacquire one current immutable `TabletSnapshot` and
its matching schema and Raft authority for every request rather than trust coordinator-time state.

## Decision

`ReplicatedDistributedMutableVectorQueryWorkerContextProvider` is the embedding boundary for one
coherent request-local value: an owning `TabletSnapshot`, shared schema lineage, committed
placement, Raft group, and optional local linearizable barrier. The provider receives the exact
fragment and supplies its own synchronization. It may not assemble the returned fields from
different logical instants.

`ReplicatedDistributedMutableVectorQueryWorker` acquires that context for every call and retains it
through synchronous execution. It invokes the existing mutable row worker, which independently
exact-matches publication position, schema, route, placement, group, and barrier before scanning
pinned heads. A shared bounded collector encodes each borrowed vector chunk into the admitted
Native result schema, caps messages and retained response bytes, and publishes only a complete
value-owned terminal stream. The durable Fragment-v2 worker uses the same collector without
changing its authority or storage path.

`ReplicatedDistributedMutableVectorQueryTcpServer` is a move-only heap-stable composition owner.
It constructs the worker first, the authenticated mutable receiver with that stable worker address
second, and the bounded TCP/mutual-TLS server with the stable receiver address last. Reverse
destruction therefore closes all connections before destroying the receiver, then destroys the
receiver before the worker. Receiver response limits are copied from carrier limits.

## Consequences

The production inbound mutable stack now composes certificate authentication, source-node
authorization, distinct request decoding, fresh immutable publication acquisition, proof
revalidation, vectorized head scanning, schema-bound result encoding, complete bounded
publication, listener admission, metrics, and ordered shutdown. It remains row-mode only;
tablet-global ordering and limit remain coordinator responsibilities.

The owner borrows the context provider, connection authenticator, node authorizer, and optional
leader-hint provider. Those dependencies must outlive it. Each worker and server is
single-thread-affine; providers needing concurrent access must synchronize externally. No new
memory-ordering algorithm or durable/network format is introduced.

## Affected invariants

- [Invariant 4](../architecture/invariants.md): the provider returns one exact applied tablet
  publication.
- [Invariant 5](../architecture/invariants.md): only committed/applied head generations are scanned.
- [Invariant 6](../architecture/invariants.md): snapshot, schema, placement, group, and barrier form
  one request-local authority value.
- [Invariant 11](../architecture/invariants.md): the owning snapshot outlives every borrowed head
  view and vector chunk.
- [Invariant 15](../architecture/invariants.md): worker memory, rows, columns, messages, encoded
  bytes, connections, accepts, and deadlines are bounded.
- [Invariant 18](../architecture/invariants.md): any authority or output mismatch fails before
  terminal result publication.

## Validation

A production-composition loopback appends two rows to a real `TabletState` under an exact Raft
position, moves the packaged server owner, completes mutual TLS, reacquires the current snapshot
once, executes the proof-bound fragment, and exact-decodes the one-column/two-row Native result.
It verifies both certificate fingerprints, one completed connection, invalid configuration, and
idempotent shutdown. Existing real-CSEG worker tests protect the shared collector refactor. An
allocation sweep requires every injected packaged-owner failure to return
`RESOURCE_EXHAUSTED`. Header self-containment and installed external consumption cover the public
API.

## Migration and rollback

This is additive and is not yet installed in `chronosd` query scheduling. Rollback removes the
mutable service owner and worker adapter without changing mutable request or vector response bytes.

## References

- [Distributed Mutable Vector Fragment v1](../formats/distributed-mutable-vector-fragment-v1.md)
- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Vectorized Tablet-State query source](0217-vectorized-tablet-state-query-source.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
