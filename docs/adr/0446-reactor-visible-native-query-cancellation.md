# ADR 0446: Reactor-visible Native query cancellation

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, Native protocol, query, and daemon maintainers
- **Extends:** [ADR 0442](0442-bounded-native-distributed-row-request-wiring.md),
  [ADR 0445](0445-committed-daemon-mutable-query-plane.md)

## Context

The replicated queue adapter called the Native query dispatcher synchronously. While that call
polled a remote distributed execution, the same adapter could not consume a later Native `CANCEL`
task. The transport scheduler already supported cooperative cancellation, but the reactor boundary
could not reach it. Shutdown likewise had to wait for the query deadline rather than publishing a
stop request.

## Decision

`ReplicatedIngestService` owns at most one heap-stable, joined query thread. The query dispatcher
remains synchronous and thread-affine: only that thread calls it, and a second query is rejected
with bounded overload while the slot is occupied. The queue-owner thread remains free to admit and
poll replicated ingest and consume later tasks.

`NativeQueryCancellation` is a sticky release/acquire publication. A `CANCEL` applies only when its
connection and request identifiers exactly match the active query. It publishes cancellation and
suppresses the query's complete response sequence; a mismatched cancellation continues to the
replicated-ingest coordinator. Cancellation therefore emits no response and cannot suppress an
unrelated request. Once result publication begins, the active owner has been harvested and later
cancellation does not revoke already linearized output.

The Native dispatcher checks cancellation before decoding, at local-fragment boundaries, before
each remote scheduler poll, and before each local physical-pipeline pull. A remote cancellation
calls the scheduler's existing client-destroying `cancel`. A local physical query also publishes
into its query resource context. Synchronous local fragment execution remains cooperatively bounded
at fragment-call boundaries and is not interrupted inside a worker call.

Shutdown stops admission, cancels and suppresses the active query, and does not report drained until
the query thread has joined. Thread creation, completion transfer, and exception boundaries fail
closed. The completion flag uses a release store and acquire load; the subsequent join makes the
owned result visible before transfer. No dispatcher or database owner is released before the joined
query.

## Consequences

The packaged replicated daemon can now consume an exact Native cancellation while distributed
socket work is active, rather than waiting for the whole-query deadline. One thread and one bounded
query result owner are admitted per service shard. Ingest progress can continue on the queue-owner
thread, although CPU/storage contention with the query remains possible. Thread creation per query
is a correctness-first boundary; a measured fixed executor may replace it without changing the
exact cancellation contract.

Fresh all-group authority reacquisition and compatible scheduler rebinding remain separate. No
durable or network format changes.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): cancellation cannot publish a partial query result.
- [Invariant 11](../architecture/invariants.md): the query thread joins before borrowed dispatcher,
  database, queue, or response ownership is released.
- [Invariant 15](../architecture/invariants.md): one active query slot and finite existing query
  deadlines bound asynchronous ownership.
- [Invariant 18](../architecture/invariants.md): exact connection/request correlation prevents
  cross-request cancellation.

## Validation

A deterministic blocking dispatcher test proves that a mismatched cancellation has no effect, a
second query is rejected as overloaded, an exact cancellation reaches the query thread, the query
owner joins, and its otherwise valid response is suppressed. A Native-service test proves a
pre-cancelled request returns the protocol `CANCELLED` error before execution. Existing scheduler
tests retain live-client teardown and deadline coverage. The focused cross-thread cases pass under
ThreadSanitizer; the complete service and allocation suites pass normally and under ASan/UBSan,
along with installed-consumer, formatting, and static-analysis builds.

## Migration and rollback

Embeddings that configure replicated Native queries now supply the `NativeQueryDispatcher`
interface implemented by `NativeProtocolService`; existing service pointers convert directly.
Rollback restores synchronous queue dispatch and removes the cancellation token without changing
any protocol bytes.

## References

- [Bounded Native distributed row request wiring](0442-bounded-native-distributed-row-request-wiring.md)
- [Proof-revalidated local and remote Native row merge](0444-proof-revalidated-local-and-remote-native-row-merge.md)
- [Packaged native daemon lifecycle](../learning/packaged-native-daemon.md)
