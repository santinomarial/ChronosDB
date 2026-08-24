# ADR 0435: Bounded mutable vector query TCP scheduling

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster, networking, query, and security maintainers
- **Extends:** [ADR 0378](0378-pinned-multi-tablet-vector-query-v2-tcp-scheduling.md),
  [ADR 0434](0434-proof-bound-mutable-vector-query-execution.md)

## Context

The portable mutable execution owner correlated finite senders and withheld partial results, while
the mutable TCP client owned one nonblocking authenticated attempt. An embedding still had to
validate route coverage, create at most one client per tablet, honor backoff and deadlines, rotate
only prevalidated addresses, drive every descriptor, release all clients on failure, and transfer
the all-tablet result once.

## Decision

`DistributedMutableVectorQueryTcpExecution` is a move-only, single-threaded POSIX `poll` owner. It
owns the portable execution and one optional mutable TCP client per plan-ordered tablet. Creation
validates positive connect/handshake/exchange timeouts, bounded response frames and bytes, unique
nonzero node routes, finite unique nonzero IPv4 endpoints, nonnull TLS contexts, and exact initial
route coverage before opening a socket.

Every slot permanently retains the route for its immutable fragment serving node. The finite sender
owns retry/backoff state; attempt number rotates only that route's ordered prevalidated endpoints.
The scheduler verifies that every attempt still names the fixed target. Advisory hints are exposed
to a later fresh-authority owner and never change this execution's route or fragment.

A preallocated descriptor and slot-index table drives active clients. Each wait is bounded by the
caller maximum, whole-execution deadline, and earliest sender backoff. Completed response streams
enter the sender once before client teardown. Only `UNAVAILABLE` and `IO_ERROR` transport failures
enter sender retry; local resource, authentication, codec, or contract failures terminate the whole
execution. Failure, cancellation, or deadline expiry synchronously destroys every live client and
publishes no result. All-tablet success transfers one retained schema-bound result.

The internal implementation constructor is intentionally not `noexcept`: initializing diagnostic
state may allocate. Construction catches allocation and length failures and returns
`RESOURCE_EXHAUSTED` instead of terminating.

## Consequences

At most one connection exists per tablet. Creation costs
`O(routes log routes + tablets log routes)` and each poll costs `O(tablets)` plus readiness work.
Authentication and TLS contexts are borrowed and must outlive the owner. One thread serializes all
calls, so no inter-thread memory-ordering argument applies. No network or durable format changes.

Fresh-authority rebinding, SQL-to-mutable-plan lowering, Native request lifecycle integration, and
daemon wiring remain later boundaries.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): fixed fragment authority is never rewritten during
  retry.
- [Invariant 6](../architecture/invariants.md): only the complete all-tablet result is published.
- [Invariant 11](../architecture/invariants.md): each TLS carrier is destroyed before its socket,
  and all clients are released before execution teardown.
- [Invariant 15](../architecture/invariants.md): routes, endpoints, clients, frames, bytes, waits,
  retries, and deadlines are bounded.
- [Invariant 18](../architecture/invariants.md): advisory hints cannot weaken exact authority.

## Validation

A real loopback test schedules two tablet leaders concurrently over mutual TLS, starts one tablet at
a refused endpoint, rotates its finite retry to the serving address, and proves three attempts, one
retry, two completed transports, one failed transport, zero live clients, and one plan-ordered
all-tablet result. Negative cases reject incomplete routes before I/O, cancel an expired execution
without attempts, and release active clients on explicit cancellation. An allocation sweep covers
scheduler construction, including diagnostic state. Header self-containment and installed external
consumption protect the public API.

## Migration and rollback

This is additive and is not yet installed in the native daemon. Rollback removes the scheduler and
target introspection without changing mutable fragment, carrier, response, or Native result bytes.

## References

- [Proof-bound mutable vector query execution](0434-proof-bound-mutable-vector-query-execution.md)
- [Bounded mutable vector query TCP ownership](0432-bounded-mutable-vector-query-tcp-ownership.md)
- [Bounded global vector row finalization v2](0379-bounded-global-vector-row-finalization-v2.md)
