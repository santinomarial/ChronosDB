# ADR 0377: Pinned schema-bound vector query v2 execution owner

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and distributed-systems maintainers
- **Extends:** [ADR 0373](0373-finite-schema-bound-vector-query-v2-sender.md),
  [ADR 0374](0374-bounded-schema-bound-vector-result-coordinator-v2.md)

## Context

The finite vector-v2 sender and schema-owning result coordinator were independently usable. An
embedding still had to keep the compatible Manifest snapshot alive, create exactly one immutable
sender per dispatch, deliver each complete terminal stream once, report terminal failure once, and
keep the global plan attached to the eventual schema-bound result. Accepting a caller-assembled
dispatch vector at that boundary would lose the proof that every fragment came from one compatible
snapshot.

## Decision

`DistributedVectorQueryExecutionV2` is a move-only, single-threaded portable orchestration owner.
Creation accepts only `CompatibleDistributedVectorSnapshotV2`, which already owns one
acquire-pinned Manifest epoch, one plan-ordered dispatch vector, and one result schema proved
against every dispatch. It rejects invalid source identity, mixed query/database/generation/plan
authority, and duplicate tablets before creating one finite sender per dispatch and one coordinator
over the exact tablet order.

The owner retains the compatible snapshot for its complete lifetime. Each sender receives one
canonical Fragment-v2 value made from its bound dispatch and the shared admitted schema. Tablet
lookup uses an owned ordered index. Attempt creation, complete response acceptance, transport
failure, backoff deadlines, state, and advisory hints delegate to exactly one sender.

A sender publishes nothing while ready, waiting, or backing off. On success, its complete retained
stream is delivered to the coordinator exactly once. Any coordinator admission failure poisons the
query and becomes the completion result; a sender's terminal failure is reported exactly once with
its final status. No result prefix is exposed.

`finish` remains unavailable until every sender has succeeded and every stream has been delivered.
It first copies the global plan while all state is still retryable, then transfers the coordinator's
plan-ordered schema and messages once. The returned value therefore keeps final semantics attached
to the schema-bound remote results. Allocation failure before or during final transfer does not
publish a partial result.

The owner has no socket, thread, callback, or internal clock. Callers serialize methods and provide
monotonic time. It introduces no durable or network bytes.

## Consequences and validation

Creation is `O(tablets log tablets)` and retains one Manifest pin, one bounded sender per tablet,
the bounded coordinator, and an ordered index. Event lookup is `O(log tablets)`. Result retention
remains bounded by the independent sender and coordinator count/byte limits. The owner is
unsynchronized, so no inter-thread memory-ordering argument applies.

Focused tests prove two-tablet plan/schema/result closure in original plan order, unavailable
partial completion, retry backoff followed by exact terminal failure, foreign-tablet rejection,
and coordinator-retention poisoning without accidental `UNAVAILABLE`. Header self-containment and
installed-consumer coverage protect the public API.

ADRs 0378 and 0379 subsequently supply pinned TCP scheduling, cross-tablet deadline cancellation,
and bounded global row ordering/limit. All-type aggregate merge state, authority rebinding, and
process integration remain separate tasks. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Finite schema-bound vector query v2 sender](0373-finite-schema-bound-vector-query-v2-sender.md)
- [Bounded schema-bound vector result coordinator v2](0374-bounded-schema-bound-vector-result-coordinator-v2.md)
- [Bounded distributed vector Fragment-v2 ownership](0367-bounded-distributed-vector-fragment-v2-ownership.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
