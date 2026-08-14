# ADR 0378: Pinned multi-tablet vector query v2 TCP scheduling

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0371](0371-deadline-bound-schema-bound-vector-query-v2-tcp-client.md),
  [ADR 0377](0377-pinned-schema-bound-vector-query-v2-execution-owner.md)

## Context

The pinned portable vector-v2 execution owner retained the compatible snapshot, finite senders, and
bounded coordinator. The TCP client owned one authenticated connection attempt. An embedding still
had to validate every route before I/O, start and retire one client per tablet, honor sender
backoff, rotate finite addresses without changing target authority, cap waits by a whole-query
deadline, release survivors on failure or cancellation, and publish only the all-tablet result.

## Decision

`DistributedVectorQueryTcpExecutionV2` is a move-only, single-threaded POSIX `poll` owner. It owns
`DistributedVectorQueryExecutionV2`, and therefore the compatible Manifest pin, plus one optional
TCP client per bound tablet. Authentication policy and route TLS contexts are borrowed and must
outlive it.

Creation validates positive connect, handshake, and exchange timeouts; response frame and exact
byte bounds; unique nonzero node routes; finite unique nonzero IPv4 endpoints; nonnull TLS contexts;
and route coverage for every immutable dispatch before opening a socket. Each tablet permanently
retains its route index. The finite sender remains the retry/backoff authority, and attempt number
rotates only that target node's ordered prevalidated endpoints. Advisory leader hints never rewrite
the route or Fragment-v2 authority.

One preallocated descriptor table drives all active clients. A poll wait is bounded by the caller,
the whole-query deadline, and the earliest sender retry deadline. Completed response vectors enter
their sender exactly once before client teardown. Connection, TLS, framing, or response validation
failure is reported once. Terminal query failure, deadline expiry, explicit cancellation, and any
orchestration error synchronously destroy every live client and publish no result.

Only when all senders close successfully does the scheduler transfer the portable owner's global
plan plus plan-ordered schema-bound result into one retained completed value. `result()` borrows that
value until the scheduler is moved or destroyed. The scheduler performs no authority rebinding and
adds no durable or network bytes.

## Consequences and validation

Creation uses `O(routes log routes + tablets log routes)` work and retains `O(routes + tablets)`
state. Each poll is `O(tablets)` plus one bounded readiness operation per live attempt. At most one
connection exists per tablet. Attempt and metric counts cannot overflow because fragment and retry
hard limits bound them below 67,108,864. One owner thread serializes calls, so no inter-thread
memory-ordering argument applies.

A focused two-tablet real-loopback/mutual-TLS test closes the first address of one target, rotates
its second finite attempt to the serving endpoint, and returns two terminal schema-bound streams.
It proves three attempts, one retry, two completed transports, one failed transport, exact
all-tablet publication, and zero live clients. A second case proves incomplete route rejection
before I/O, an expired deadline starts no attempts, and explicit cancellation releases active
clients. Header self-containment, installed-consumer coverage, sanitizers, formatting, and focused
static analysis protect the public boundary.

A deterministic allocation sweep now covers schema-bound request/socket/client ownership and
requires returned `RESOURCE_EXHAUSTED` at every selected failure. The outbound owner's diagnostic
status may allocate and is therefore no longer constructed behind `noexcept`. Scheduler start
classifies only `UNAVAILABLE` and `IO_ERROR` as retryable connection outcomes; local resource or
contract failure is whole-query terminal and is not reported as failed transport. No frame changes.

That classification is preserved after the TLS carrier starts: only `UNAVAILABLE` and `IO_ERROR`
enter transport retry. Every other client-local failure, including resource exhaustion during
authentication, carrier construction, schema-bound response decoding, or response-stream retention,
closes the whole execution and all active clients without entering sender backoff or incrementing
transport-failure metrics. A TLS-owner allocation sweep and real scheduler/carrier failure test
freeze the boundary. No frame or durable format changes.

ADR 0379 subsequently supplies bounded global row ordering/limit. All-type aggregate merge state,
whole-query authority rebinding, and process integration remain separate tasks. No Phase 16 exit
gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Deadline-bound schema-bound vector query v2 TCP client](0371-deadline-bound-schema-bound-vector-query-v2-tcp-client.md)
- [Pinned schema-bound vector query v2 execution owner](0377-pinned-schema-bound-vector-query-v2-execution-owner.md)
- [Pinned grouped-query TCP scheduling](0342-pinned-grouped-query-tcp-scheduling.md)
- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
