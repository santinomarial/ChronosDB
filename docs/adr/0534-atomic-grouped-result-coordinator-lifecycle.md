# ADR 0534: Atomic grouped result coordinator lifecycle

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0533](0533-deadline-bound-grouped-result-retry-scheduling.md)

## Context

Reducer processes can return immutable partition results through finite mutual-TLS retries, and the
coordinator has separate server, collector, materializer, and global finalizer owners. Leaving
their handoffs to an embedding creates a dangerous gap: the server may already have acknowledged a
stream when a later local allocation fails, so dropping the retained value would make a successful
reducer unable to repair the query.

## Decision

Add one move-only, single-thread-affine coordinator result execution. It borrows the exact shuffle
and fragment-derived finalization authorities, optional final projection, authenticator, and node
authorizer. It owns the bounded result listener, idempotent collector, one lossless pending-stream
slot, complete partition vector, accounted materializer, deadline/cancellation state, and final
Native result.

Creation requires the finalization authority to reference the exact supplied shuffle-authority
object. It prevalidates carrier, collection, materialization, and finalization limits, allocates the
collector before opening the listener, and starts the result server with the finalization
authority's exact raw-schema object. All construction allocation failures are classified as
resource exhaustion.

Polling advances the server under the smaller caller or query-deadline wait. A server-retained
stream moves first into the pending slot and leaves that slot only after collector admission.
Collector admission now validates by reference and moves only on first-partition success; exact
duplicates and failures preserve the caller. Complete collection publication is retryable, and the
collected-result materializer performs every fallible construction step before a no-throw vector
move. Consequently, local resource exhaustion after a network receipt returns to the event loop
without losing the acknowledged bytes.

Once all authority partitions are present, the owner shuts the listener, materializes under its
separate query-memory bound, and runs the existing proof-revalidated projection, global `ORDER BY`,
`LIMIT`, and atomic Native encoder. No result is observable until the whole finalizer succeeds.
The completed value may be taken once. Permanent failure, explicit cancellation, or deadline
expiry closes transport and clears unpublished state. Saturating metrics snapshot server and
collector progress plus poll, finalization, row, and encoded-byte counts.

This is an in-memory query lifecycle. A coordinator process crash requires the reducer query to be
retried; the decision does not claim durable result receipts or cross-process recovery.

## Consequences

Independent reducer outputs now have a complete coordinator-side path from authenticated receipt
through one atomic SQL result. The acknowledged-result handoffs are lossless under local allocation
failure, while exact retransmissions remain idempotent. Listener shutdown before final publication
prevents late streams from racing a finalized answer.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): the exact shuffle authority, finalization proof,
  raw schema, reducer identity, partition, and coordinator are revalidated across every boundary.
- [Invariant 11](../architecture/invariants.md): transport, pending bytes, collected streams,
  materialization resources, and final output have one explicit owner; proof and security objects
  are documented borrows.
- [Invariant 15](../architecture/invariants.md): descriptors, sessions, accepts, frames, collected
  bytes, batch memory, total memory, output, per-poll work, and time are bounded independently.
- [Invariant 18](../architecture/invariants.md): only a complete authority-ordered partition set can
  enter global finalization, and output remains atomic.

## Validation plan

Drive two authority-derived reducer results through the real retry scheduler, TCP, mutual TLS,
receipt validation, server, collector, materializer, and global finalizer. Require global aggregate
ordering and limiting, exact proof identity, one-shot publication, cancellation, and deadline
expiry. Inject allocation failure through coordinator construction and directly prove that
collector and materializer handoff failures preserve their caller-owned stream vectors. Run
cluster, allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire format changes. Rollback may retain the component owners, but an embedding must
provide the same acknowledged-stream preservation and atomic finalization guarantees; it must not
publish directly from the result server or collector.

## Unresolved questions

- Qualify distinct reducer and coordinator operating-system processes against one real packaged
  endpoint, including abrupt process loss and retry behavior.
- Add a hybrid result collector for a coordinator process that also owns a reducer partition.

## References

- [Result scheduler decision](0533-deadline-bound-grouped-result-retry-scheduling.md)
- [Collector decision](0531-idempotent-grouped-shuffle-result-collector.md)
- [Accounted result finalization](0532-accounted-independent-grouped-result-finalization.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
