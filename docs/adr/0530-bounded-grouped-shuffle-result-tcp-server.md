# ADR 0530: Bounded grouped shuffle result TCP server

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0527](0527-bounded-grouped-shuffle-result-mutual-tls.md) and
  [ADR 0529](0529-deadline-bound-grouped-shuffle-result-tcp-client.md)

## Context

The result TLS server accepts one connected descriptor and withholds a partition until its receipt
is fully written. A coordinator process still needs bounded listener admission, stable socket/TLS
ownership, deadline-aware readiness polling, and retained-result transfer. Allocating a completion
queue after receipt write could lose a result the reducer already considers accepted.

## Decision

Add a move-only, single-thread-affine poll server. Startup validates the listener, TLS and stream
limits, coordinator node, exact raw result schema, borrowed security and authority dependencies,
retained-stream capacity, and per-poll accept bound before publication. It preallocates one poll
record, optional completion slot, FIFO order entry, and free-slot identity for every retained
stream. Capacity is limited to 65,536 and accepts per poll to 1,024.

Every admitted connection reserves a free completion slot before TLS construction. Its owned TCP
socket outlives the nested result TLS carrier, and the accepted peer IPv4 address becomes the
authentication context. A completed carrier has already written its receipt; its exact partition
is moved into the reserved slot without allocation and exposed once in FIFO order. Failed sessions
release their slots and private prefixes. When capacity is full, new connections are closed before
application acceptance so reducer retry remains unambiguous.

Polling advances each active session at most once, shortens caller wait to the earliest carrier
deadline, and bounds accepts. Saturating metrics report accepted, rejected, accept-error,
completed, failed, active, and retained counts. Shutdown destroys active TLS carriers before their
descriptors and closes the listener while preserving already retained streams. Duplicate
arbitration, result merge, retry scheduling, and query lifecycle composition remain separate.

## Consequences

Independent reducers can now return acknowledged result partitions to a real coordinator listener
without unbounded descriptors, work, or post-acknowledgment queue allocation. Retained exact
retries may still be duplicates until the coordinator collector arbitrates them.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): retained results preserve the authority, raw schema,
  reducer, coordinator, and partition accepted by TLS.
- [Invariant 11](../architecture/invariants.md): listeners, sockets, carriers, completion slots, and
  borrowed dependencies have explicit lifetimes.
- [Invariant 15](../architecture/invariants.md): admission, polling, active sessions, retained
  results, frame bytes, and deadlines are finite.
- [Invariant 18](../architecture/invariants.md): a reserved slot precedes acknowledgment and FIFO
  transport does not change global result semantics.

## Validation plan

Compose the real TCP result client with the server and prove mutual authentication, receipt-gated
client success, exact retained result extraction, one-shot transfer, metrics, and deterministic
shutdown. At capacity one, prove a second connection is rejected while the first owns the only
slot. Exercise invalid startup and poll bounds, header isolation, and allocation-failure startup
sweeps. Run cluster, allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire format changes. Rollback removes coordinator listener ownership while retaining
the already-connected TLS server and one-attempt TCP client.

## Unresolved questions

- Deduplicate exact completed partition attempts at the coordinator.
- Schedule all required remote partition results under one query deadline.
- Integrate independent-process return with global grouped finalization.

## References

- [Result TLS decision](0527-bounded-grouped-shuffle-result-mutual-tls.md)
- [Result TCP client decision](0529-deadline-bound-grouped-shuffle-result-tcp-client.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
