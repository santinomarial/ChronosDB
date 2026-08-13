# ADR 0396: Pinned vector aggregate query v2 TCP scheduling and finalization

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, networking, and protocol maintainers
- **Extends:** [ADR 0392](0392-deadline-bound-definition-bound-vector-aggregate-query-v2-tcp-client.md),
  [ADR 0395](0395-pinned-definition-bound-vector-aggregate-query-v2-execution-owner.md)

## Context

The portable aggregate execution owner retained the compatible Manifest pin, exact cross-tablet
definitions, shared query resources, finite senders, and global coordinator. The TCP client owned
one definition-bound authenticated connection attempt, and aggregate finalization could encode one
merged scalar row. An embedding still had to validate all routes and output limits before I/O,
schedule one client per tablet, honor finite backoff, destroy survivors on terminal state, and
publish the Native Protocol result exactly once.

## Decision

`DistributedVectorAggregateQueryTcpExecutionV2` is a move-only, single-threaded POSIX `poll` owner.
It owns `DistributedVectorAggregateQueryExecutionV2` and at most one aggregate TCP client per
tablet. Authentication policy and route TLS contexts are borrowed and must outlive it.

Creation validates positive connect, handshake, and exchange timeouts; nested response frame,
aggregate-state, extremum, and byte limits; Native Protocol finalization limits; unique nonzero
node routes; finite unique nonzero IPv4 endpoints; nonnull TLS contexts; exact definition-width
coverage; and one route for every immutable dispatch before opening a socket. Each tablet retains
one route index. Attempt number rotates only that target node's ordered prevalidated addresses;
leader hints never rewrite the pinned dispatch or route authority.

Each attempt gets a value copy of the exact pinned definition vector and a handle to the one shared
query resource context. Definition-copy allocation failure is classified before acquisition.
Connection, TLS, framing, and response validation failures enter the finite sender exactly once.
One preallocated descriptor table drives live clients, and every wait is bounded by the caller, the
whole-query deadline, and the earliest retry deadline.

Terminal failure, orchestration failure, deadline expiry, or explicit cancellation synchronously
destroys all live clients and publishes no result. Only after every sender succeeds does the owner
consume the portable coordinator result and call
`finalize_distributed_vector_aggregate_v2` once. The retained public result is one bounded canonical
Native Protocol v1 payload; later polls are idempotently complete.

## Alternatives considered

- **Let clients choose definitions or memory authorities:** rejected because retries could diverge
  from the pinned cross-tablet proof or escape the query-wide memory ceiling.
- **Finalize in the portable owner:** rejected because socket cancellation and deadline failure
  still needed one terminal publication boundary.
- **Retry inside the TCP client:** rejected because the finite sender must observe and bound every
  whole-attempt outcome.
- **Rebind from leader hints in place:** rejected because hints are advisory and cannot replace the
  immutable compatible snapshot authority.

## Consequences

Creation is `O(routes log routes + tablets log routes)` and retains `O(routes + tablets)` state.
Every poll is `O(tablets)` plus bounded readiness work per live attempt. At most one connection
exists per tablet, and aggregate definitions are copied only for a due attempt. One owner thread
serializes every transition, so no inter-thread memory-ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only committed, proof-bound fragment results enter
  global merge.
- [Invariant 6](../architecture/invariants.md): every attempt and the final merge retain one pinned
  compatible snapshot, exact definitions, and shared query resource authority.
- [Invariant 11](../architecture/invariants.md): the compatible Manifest pin survives all attempts
  and finalization.
- [Invariant 14](../architecture/invariants.md): endpoint, peer, request, state, and Native Protocol
  formats remain versioned and exact.
- [Invariant 18](../architecture/invariants.md): endpoint rotation and bounded polling never weaken
  authentication, snapshot, visibility, or integrity guarantees.

## Validation plan

Use two real loopback mutual-TLS aggregate services. Close the first endpoint for one tablet so its
finite retry rotates to the second prevalidated address. Prove three attempts, one retry, two
completed transports, one failed transport, zero survivors, exact definition binding, global
COUNT values, and one decoded Native Protocol row. Separately prove incomplete-route and invalid
finalization-limit rejection before I/O, an already-expired deadline starts no attempts, and
explicit cancellation releases all clients. Run header self-containment, installed consumption,
formatting, static analysis, ASan/UBSan, allocation-failure neighbors, and the full serialized
suite.

## Migration or rollback considerations

No durable or network bytes change. Embeddings can replace ad hoc aggregate scheduling and final
encoding with this owner. Rollback must preserve the Manifest pin, exact definition/resource
authority on every retry, route validation before I/O, finite terminal teardown, and exactly-once
global finalization.

## References

- [Deadline-bound definition-bound vector aggregate query v2 TCP client](0392-deadline-bound-definition-bound-vector-aggregate-query-v2-tcp-client.md)
- [Pinned definition-bound vector aggregate query v2 execution owner](0395-pinned-definition-bound-vector-aggregate-query-v2-execution-owner.md)
- [Native vector aggregate finalization](0386-native-vector-aggregate-result-finalization-v2.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
