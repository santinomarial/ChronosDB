# ADR 0342: Pinned grouped-query TCP scheduling

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0178](0178-pinned-multi-tablet-tcp-query-scheduling.md),
  [ADR 0336](0336-deadline-bound-grouped-query-tcp-client.md),
  [ADR 0341](0341-fail-closed-grouped-query-execution-owner.md)

## Context

The grouped execution owner retained a proof-bound multi-tablet snapshot and finite senders, while
the grouped TCP client owned one authenticated attempt and its complete response stream. An
embedding still had to validate all routes before I/O, start and retire one client per tablet,
respect retry deadlines, rotate finite addresses without changing authority, close peers on
failure or cancellation, and publish only the all-tablet grouped result.

## Decision

`DistributedGroupedQueryTcpExecution` is a move-only, single-threaded poll owner. It owns the
`DistributedGroupedQueryExecution`, and therefore the pinned Manifest epoch, plus one optional TCP
client for each bound tablet. Authentication policy and route TLS contexts are borrowed and must
outlive it.

Creation validates all timeouts, response-frame bounds, unique nonzero routes, bounded unique IPv4
endpoints, nonnull TLS contexts, and complete coverage of every immutable dispatch target before
opening a socket. Each tablet permanently retains its route index. Attempts start in dispatch order;
the finite sender remains the retry/backoff authority. Attempt number deterministically rotates the
route's ordered address set without changing target node, TLS identity, proof, or retry budget.

One preallocated poll table drives every active client and caps waits by the caller bound, query
deadline, and earliest sender retry deadline. A completed client's full response vector is handed
to its sender once before client teardown. Transport failure is reported once. Terminal query
failure, monotonic deadline expiry, and explicit cancellation synchronously release every client.
Only all-tablet coordinator completion publishes the grouped result. Metrics separate attempts,
retries, completed transports, failed transports, and live clients.

The scheduler does not rebind from advisory hints. Fresh authority requires a new grouped execution.
No durable or network format changes.

## Consequences and validation

Creation uses `O(routes log routes + tablets log routes)` work and retains `O(routes + tablets)`
state. Each poll is `O(tablets)` plus one bounded readiness operation per live attempt. At most one
connection exists per tablet. The owner is explicitly single-threaded, so no inter-thread
memory-ordering argument applies.

A focused real loopback test drives two authenticated mutual-TLS servers, forces the first tablet's
first address to refuse connection, rotates its second finite attempt to the valid address, and
merges the same key across both tablets to count two and sum six. It proves three attempts, one
retry, two complete transports, one failed transport, and zero live clients. A second test proves
incomplete routes reject before I/O, an already-expired deadline starts no attempt, and explicit
cancellation releases two active clients. Header self-containment and installed-consumer
compilation cover the public API.

The shared allocation-failure executable now sweeps grouped request/socket/client ownership until
the first successful construction. Every injected failure returns `RESOURCE_EXHAUSTED`; the client
owner's allocating diagnostic state is no longer hidden behind `noexcept`. Scheduler start sends
only `UNAVAILABLE` and `IO_ERROR` into finite transport retry, while local resource or contract
failure closes the whole query immediately. No protocol bytes change.

The in-flight carrier follows the same rule: only `UNAVAILABLE` and `IO_ERROR` enter transport
retry. Every other local failure, including resource exhaustion during TLS authentication, carrier
construction, response decoding, or bounded response-vector retention, is whole-query terminal,
clears every active client, and leaves retry and transport-failure metrics unchanged.
Allocator-linked construction coverage includes the TLS owner's sticky diagnostic state instead of
hiding that allocation behind `noexcept`. No wire or durable format changes.

The packaged bounded-stale grouped lifecycle additionally sweeps scheduler construction after real
mutual-TLS authority acquisition. The scheduler owner's diagnostic status may allocate, so its
implementation constructor is not `noexcept`; an injected failure returns
`RESOURCE_EXHAUSTED` with no active attempt, no published result, and exact Manifest-pin rollback.
Pre-acquired owners are also swept through real mutual-TLS response decode, sender/coordinator
acceptance, and final result installation. Every selected allocation failure closes the active
client and fails the whole query atomically; the first unselected boundary publishes the exact
grouped result.

Packaged metadata-backed grouped construction, explicit whole-query authority rebinding,
multi-key/non-FLOAT64 grouping, general vector fragments, and broad fault/measurement evidence
remain incomplete. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
- [Deadline-bound grouped-query TCP client](0336-deadline-bound-grouped-query-tcp-client.md)
- [Fail-closed grouped query execution owner](0341-fail-closed-grouped-query-execution-owner.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
