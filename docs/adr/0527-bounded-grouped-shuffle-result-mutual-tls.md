# ADR 0527: Bounded grouped shuffle result mutual TLS

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query, networking, and security maintainers
- **Extends:** [ADR 0525](0525-authenticated-complete-grouped-shuffle-result-stream.md) and
  [ADR 0526](0526-correlated-grouped-shuffle-result-success-acknowledgment.md)

## Context

The result stream and receipt defined application ownership but did not drive a connected socket.
Independent reducer processes need a bounded carrier that authenticates both certificate principals,
handles nonblocking TLS partial I/O, and makes the receipt—not a successful socket write—the
result-return success boundary.

## Decision

Add single-attempt, move-only client and server owners for one already-connected nonblocking mutual-
TLS socket. The reducer-side client authenticates the peer certificate, authorizes its principal for
the exact coordinator node, writes one preconstructed complete result stream, then waits for the
exact receipt. Receipt partition, frame count, and byte extent must equal the immutable sender.

The coordinator-side server authenticates the client certificate before application reads, passes
that peer identity into the complete-stream receiver, and therefore authorizes the claimed reducer
source on the first frame. It privately retains the complete partition, constructs the correlated
receipt, and makes the partition available for one-shot transfer only after the receipt has been
fully written. Any handshake, authentication, authorization, TLS, framing, extent, timeout, or
allocation failure is sticky and clears retained result state.

Handshake and exchange deadlines are distinct, positive, saturating steady-clock durations.
Interest state explicitly reports read/write readiness. Authority, raw result schema,
authenticators, and authorizers are borrowed and outlive each owner. One caller thread serializes
all operations; no shared-memory ordering is introduced.

## Consequences

One reduced partition can now cross an authenticated process boundary with all-or-none coordinator
retention and explicit application acceptance. TCP connect/listen/admission, finite retry and
duplicate collection, all-partition scheduling, and lifecycle integration remain separate.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): result stream, receipt, peer nodes, authority, and
  raw schema remain one exact attempt.
- [Invariant 10](../architecture/invariants.md): certificate authentication and node authorization
  precede application publication.
- [Invariant 11](../architecture/invariants.md): socket, sender, receiver, receipt, and complete
  partition have explicit move-only lifetimes.
- [Invariant 15](../architecture/invariants.md): handshake, exchange, frame, and total-byte influence
  are finite.
- [Invariant 18](../architecture/invariants.md): TLS write completion cannot weaken terminal closure
  or exact receipt requirements.

## Validation plan

Drive two canonical result batches through a real nonblocking socket pair and mutual-TLS fixtures;
prove both fingerprints were authenticated, the coordinator retains no early result, the client
requires the exact receipt, and transfer is one-shot. Reject a coordinator principal before any
stream write and expire exactly at the handshake deadline. Sweep client/server owner allocations.
Run cluster, allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire format changes. Rollback removes the connected owners while preserving the
standalone result stream and receipt codecs; result return must then remain unavailable.

## Unresolved questions

- Add deadline-bound TCP connect and bounded server admission owners.
- Add finite byte-identical retry and idempotent duplicate collection.
- Schedule all remote partitions and compose them into global finalization.

## References

- [Result stream decision](0525-authenticated-complete-grouped-shuffle-result-stream.md)
- [Result receipt decision](0526-correlated-grouped-shuffle-result-success-acknowledgment.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
