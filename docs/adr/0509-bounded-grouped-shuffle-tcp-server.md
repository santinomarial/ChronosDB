# ADR 0509: Bounded grouped shuffle TCP server

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, distributed-query, and networking maintainers
- **Extends:** [ADR 0506](0506-bounded-grouped-shuffle-mutual-tls-session.md),
  [ADR 0508](0508-deadline-bound-grouped-shuffle-tcp-client.md)

## Context

The grouped-shuffle TLS server accepted one already connected descriptor and withheld its decoded
stream until the success receipt was fully written. A process listener still needed bounded
admission, stable descriptor/carrier ownership, readiness polling, lifecycle metrics, and a way to
transfer acknowledged streams to a reducer. A conventional completion queue allocated after TLS
success would be unsafe: the source has already received or may receive the acknowledgment, so a
queue allocation failure could silently discard accepted data.

## Decision

Add a move-only, single-thread-affine POSIX poll server. Startup validates the TLS/stream limits,
nonzero local node, borrowed authentication/authorization/authority dependencies, retained-stream
capacity, and accept-work limit before creating the TLS context and listener. It preallocates one
poll record, completion slot, completion-order entry, and free-slot identity per configured retained
stream. The hard retained-stream limit is 65,536 and the per-poll accept limit is 1,024.

Admission requires a free completion slot before TLS construction. The server owns each accepted
`TcpSocket` beside its `DistributedVectorGroupedAggregateShuffleTlsServer`, with declaration order
that destroys TLS before the borrowed descriptor. The actual peer IPv4 address becomes the
authentication context. Each poll progresses every existing session at most once and accepts only
the configured finite number of new descriptors.

When a TLS session completes, its receipt has already been written and its exact stream is moved
into the reserved optional slot without allocation. A preallocated ring records completion FIFO.
`take_next_complete_stream` moves one result to the caller and returns the slot to the free stack;
no result is exposed twice. Active plus retained streams can never exceed configured capacity.
Failed sessions release their reserved slot and every private prefix. Saturating metrics expose
accepted, rejected, accept-error, completed, failed, active, and retained counts.

Shutdown destroys active TLS sessions before closing the listener and preserves already completed
streams for explicit extraction. Borrowed security dependencies and authority outlive the server.
The shared query resource handle bounds decoded retained data across sessions. The owner performs no
duplicate arbitration, reducer merge, retry scheduling, address resolution, or query packaging.
One thread owns all calls, so no shared concurrent algorithm or memory-ordering argument is needed.

## Detailed rationale

Reserving capacity at admission moves the only fallible queue allocation before the peer can be
acknowledged. The fixed slot plus index ring keeps completion publication deterministic and makes
backpressure visible as connection rejection rather than post-success data loss. Keeping the
listener separate from reducer admission lets transport be tested without exposing partial group
state.

## Alternatives considered

- **Push into a growing queue after receipt write.** Rejected because allocation failure could
  discard an acknowledged stream.
- **Stop reading when the completion queue fills.** Rejected because already admitted sessions
  still need guaranteed result capacity and could otherwise deadlock after consuming resources.
- **Merge directly into a reducer before acknowledgment.** Deferred because idempotent duplicate
  arbitration and deterministic all-source closure require their own authority owner.
- **Spawn one thread per connection.** Rejected because it weakens finite ownership and conflicts
  with the existing readiness-driven network model.

## Consequences

Remote grouped-shuffle streams now have complete outbound connect and inbound listener ownership.
Kernel descriptors, active sessions, decoded results, polling storage, and accept work are all
finite. Capacity pressure rejects connections before application acceptance, allowing the source's
existing retry policy to react without ambiguous success.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): each retained result preserves the exact immutable
  source-partition edge accepted by TLS.
- [Invariant 10](../architecture/invariants.md): listener ownership cannot bypass TLS, stream,
  frame, nested payload, or receipt validation.
- [Invariant 11](../architecture/invariants.md): descriptors, TLS carriers, reserved completion
  slots, retained streams, and borrowed dependencies have explicit lifetimes and teardown order.
- [Invariant 15](../architecture/invariants.md): admission, poll work, active/retained streams,
  decoded memory, and every session deadline are bounded.
- [Invariant 18](../architecture/invariants.md): transport FIFO does not change canonical source
  order, key equality, hash routing, or reducer semantics.

## Validation plan

A real loopback test composes the deadline-bound TCP client with this listener, proves both
certificate fingerprints and node authorizations, receipt-gated client success, complete retained
stream extraction, exact-once FIFO removal, and lifecycle metrics. A capacity-one test proves one
active session reserves the only completion slot and a second connection is rejected; invalid
configuration, poll bounds, deterministic shutdown, and active cleanup are covered. Allocation
injection sweeps TLS-context, listener, poll, connection, completion-slot, free-stack, ring, and
owner construction. The warning-as-error ASan/UBSan build, all 284 cluster tests, and all 47 cluster
allocation-failure tests pass. Changed-source clang-tidy reaches only the known LLVM 18/macOS 26
libc++ builtin incompatibility without a ChronosDB-source finding.

## Migration or rollback considerations

No durable or wire bytes change. Rollback removes listener ownership while retaining the
single-attempt TCP client and already-connected TLS server; embeddings must provide an equivalent
pre-reserved completion boundary or leave remote shuffle unavailable.

## Unresolved questions

- Make destination reducer admission idempotent across lost receipts and retries.
- Compose deterministic all-source partition closure and merge.
- Add all-edge scheduling, cancellation, address rotation, and packaged SQL selection.

## References

- [Bounded distributed-query TCP server](0176-bounded-distributed-query-tcp-server.md)
- [Bounded grouped sufficient-state TCP server](0481-bounded-grouped-sufficient-state-tcp-server.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
