# ADR 0178: Pinned multi-tablet TCP query scheduling

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0171](0171-fail-closed-distributed-query-execution-owner.md), [ADR 0177](0177-deadline-bound-distributed-query-tcp-client.md)

## Context

The execution owner retained a compatible multi-tablet snapshot and deterministic sender state, and
the TCP client owned one authenticated attempt, but an embedding still had to join them correctly.
It could release the Manifest pin while clients were active, schedule a retry before its deadline,
route a tablet to a node other than its immutable dispatch target, lose a terminal transport
outcome, leak another tablet's connection after failure, or return a partial aggregate.

## Decision

`DistributedQueryTcpExecution` is a move-only, single-threaded `poll` owner for one
`DistributedQueryExecution`. It therefore retains the compatible Manifest snapshot for the entire
network execution. Creation validates positive connect/TLS deadlines, unique nonzero node routes,
nonzero IPv4 endpoints, nonnull TLS contexts and policy owners, and complete coverage of every
plan-ordered dispatch target before any socket is opened. Each slot binds its tablet to that target's
route index permanently.

The owner starts every ready attempt and every due retry in exact dispatch order. The existing
sender remains the sole retry-budget and backoff authority; the execution now exposes its sender's
optional monotonic retry deadline for efficient polling. Each active slot owns one
`DistributedQueryTcpClient`. Poll storage and slot indexes are allocated once at creation, while
each client retains the independently bounded request, TCP/TLS state, and response. The poll wait is
capped by both the caller's bound and the earliest retry deadline, and every active client is driven
after each wait even without readiness so connect, handshake, and exchange deadlines still expire.

A completed carrier's exact response is delivered once to its tablet sender before the client is
released. Connect, TLS, framing, or correlation failures are reported once through
`record_transport_failure`; the sender either enters finite backoff or becomes terminal. One
terminal sender failure closes all other clients and exposes the coordinator's first failure. Only
when every sender succeeded does `finish` publish the aggregate. Calls after success are idempotent;
failure is sticky. Attempt, retry, transport-completion, transport-failure, and active counts are
exposed separately.

The scheduler never follows an advisory leader hint or mutates a route. A leader or placement
change requires fresh admission, placement, barrier, and compatible snapshot evidence and a newly
bound execution. Authentication and node-authorization policies plus TLS contexts are borrowed and
must outlive the scheduler; all other execution, route, slot, client, and result state is owned.

ADR 0179 subsequently adds a whole-query monotonic deadline and explicit cancellation while
preserving this owner's all-tablet success boundary and deterministic client teardown.
ADR 0180 adds finite explicit whole-query replacement after retryable terminal failure; it validates
logical identity and generation monotonicity and never retains partials across authorities.

## Consequences and validation

Creation uses `O(routes log routes + fragments log routes)` work and retains `O(routes + fragments)`
state. Each poll is `O(fragments)` plus one bounded readiness operation per active attempt. At most
one connection exists per tablet, and attempt counts remain capped by the sender configuration. The
owner is explicitly single-threaded, so no inter-thread memory-ordering argument applies.

A real loopback integration test binds two mutual-TLS query servers for two plan-ordered tablets.
One worker returns `UNAVAILABLE` on its first request. The test proves both requests retain the same
pinned Manifest generation and immutable target, the retry starts only through sender backoff, each
response reaches its sender once, the final Welford states merge exactly, three transport-complete
attempts leave zero active clients, and no transport failure is fabricated. A negative test proves
an incomplete route table rejects before attempts begin. Header self-containment, sanitizer, and
installed-consumer checks cover the public boundary and ownership.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## Alternatives considered

- **Let the embedding retain the execution while the scheduler borrows it:** rejected because the
  Manifest pin and active clients must have one unambiguous lifetime.
- **Rewrite a slot from a leader hint:** rejected because that silently changes the proof-bound
  snapshot and placement contract.
- **Start a timer/thread per tablet:** rejected because one owner can apply the existing monotonic
  deadlines and finite poll storage without extra synchronization or stacks.
- **Return the coordinator result after the first success:** rejected because missing tablets must
  remain an explicit failure, never a partial result.
- **Pool or multiplex attempts:** deferred until a bounded connection protocol defines independent
  request cancellation, response demultiplexing, and failure ownership.

## Migration and rollback

This adds no durable or wire format. Embeddings may replace manual per-tablet TCP driving with this
owner. Equivalent implementations must retain the compatible snapshot, validate all routes before
opening sockets, honor sender retry deadlines and immutable targets, deliver every terminal attempt
once, close peer attempts on terminal query failure, and publish only the complete coordinator
result. Removing it returns that orchestration to the embedding without changing individual clients
or servers.

## References

- [Fail-closed distributed query execution owner](0171-fail-closed-distributed-query-execution-owner.md)
- [Deadline-bound distributed-query TCP client](0177-deadline-bound-distributed-query-tcp-client.md)
- [Bounded distributed-query TCP server](0176-bounded-distributed-query-tcp-server.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
- [Whole-query TCP cancellation and deadline](0179-whole-query-tcp-cancellation-deadline.md)
- [Explicit whole-query authority rebinding](0180-explicit-whole-query-authority-rebinding.md)
