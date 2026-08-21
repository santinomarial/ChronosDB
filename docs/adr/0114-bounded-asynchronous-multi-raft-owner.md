# ADR 0114: Bounded asynchronous Multi-Raft owner

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems maintainers
- **Extended by:** [ADR 0135](0135-bounded-asynchronous-prepared-reconfiguration-admission.md)
  and [ADR 0138](0138-fifo-ordered-raft-group-observation.md)

## Context

`DurableMultiRaftRuntime` and its multiplexed physical log are deliberately single-thread-affine,
but callers had to execute storage synchronization on their own thread. A production reactor or
timer loop cannot block on that work, and concurrent callers cannot safely invoke the owner
directly. An asynchronous boundary must preserve FIFO operation order, persist-before-send release,
bounded admission, completion lifetime, and deterministic shutdown.

## Decision

`AsyncDurableMultiRaftRuntime` owns one background thread, one `DurableMultiRaftRuntime`, and a
bounded mutex-protected FIFO of complete caller batches. Producers use nonblocking `try_submit`.
Admission counts both queued and actively executing batches/operations; full capacity returns
`RESOURCE_EXHAUSTED` without a side queue. A successful submission transfers the complete operation
vector and returns an owning single-consumer completion handle that may outlive the runtime. Its
wait operation moves the potentially large transition result out exactly once.
The producer also applies the synchronous durable owner's operation-aware outbound reservation
before publishing a task. An undersized batch is ordinary admission backpressure and cannot enter
the worker only to be misclassified as a terminal durable failure.

Mutex unlock/lock publishes each complete task to the sole worker. Completion-state mutex
unlock/lock publishes the complete durable result to waiters. These ordinary synchronization edges,
not relaxed atomics, establish ownership and visibility. The worker executes admitted batches in
FIFO order through the existing durable batch API, so persistence and synchronization still finish
before outbound messages become observable.

Shutdown closes admission, drains accepted FIFO work, shuts down the worker extension, closes the
physical log, and joins the worker. Extension cleanup precedes physical close because it may still
borrow the synchronous owner. The first failure in that order is retained, but a failed extension
shutdown never skips any physical close. Shutdown is idempotent. An unexpected worker exception or
top-level durable batch failure fails the owner closed, completes the current and all queued requests
with one terminal error, and rejects new admission. Destruction performs the same shutdown and never
detaches the owner thread.

## Detailed rationale

One worker preserves the existing thread-affinity proof and total physical persistence order while
removing disk waits from producers. Batch-level FIFO gives a deterministic initial fairness rule;
the existing per-batch operation limit bounds how long one admission can monopolize the worker.
Metrics expose pending/high-water work and accepted, rejected, completed, and failed batches.

## Alternatives considered

- **Call the durable runtime from reactors:** blocks event progress and violates exclusive owner
  affinity under multiple producers.
- **One worker per Raft group:** destroys shared physical-log batching and ordering.
- **A general multi-worker pool:** cannot concurrently call the single physical owner and adds
  scheduling complexity without parallel durable progress.
- **Unbounded MPSC work:** hides overload as memory and latency growth.
- **Detached callbacks:** obscure completion lifetime and shutdown; owning waitable handles make the
  release edge explicit.

## Consequences

The physical log has one clear thread owner and producers receive explicit backpressure. One slow or
large bounded batch can still delay later groups; per-group deficit scheduling and timer/transport
integration remain future work requiring measurements. Metrics distinguish physical completion
notifications from full-pipe coalescing. Waiting on a completion is allowed for control-plane
callers but reactor threads must poll or hand it to a non-reactor continuation.

## Affected invariants

Invariants 1, 4, 5, 8, 11, and 18 apply. FIFO ownership preserves logical apply order and
persist-before-send durability. Owning tasks/completions and joined shutdown preserve lifetimes.
The mutex publication argument is part of the concurrency contract.

## Validation plan

Focused tests submit election, proposal, and apply batches without waiting between them, request
shutdown immediately, verify FIFO drain and completion, and reopen the durable log at the applied
state. Boundary tests reject empty, oversized, aggregate-outbound-exhausting, closed-admission, and
invalid completion use; outbound rejection preserves admission and accepts a smaller batch. A
controlled full-bound race admits exactly 64 observations from eight producers, releases two
concurrent shutdown callers against final admission attempts, and proves every accepted completion
drains once with convergent shutdown status and exact terminal metrics. A controlled record-limit
failure blocks the worker with eight accepted observations queued behind it, begins shutdown, and
proves the current and every queued completion receive one retained terminal status with exact
failure, rejection, notification, and zero-pending metrics. An exhaustive physical-close matrix
drains one accepted election under every nonempty active-file/lock/directory close-failure
combination, both with and without an extension shutdown failure. It proves exact first-failure
arbitration, complete physical cleanup, idempotence, terminal metrics, successful completion
preservation, and exact reopen. Broader queue-interleaving stress, asynchronous-owner allocation
injection, worker-start injection, other syscall-level I/O failure injection,
thousands-of-groups fairness, and latency/throughput measurements remain in Phase 18.

## Migration or rollback considerations

This adds no durable or wire format. Callers may retain the synchronous owner where thread affinity
is already externally guaranteed. Rollback removes the wrapper without changing recovered bytes.

## Unresolved questions

Measured group-aware fairness policy, completion integration with reactor continuations, timer
coalescing, thread placement, and production transport ownership remain unresolved.

## References

- [ADR 0004](0004-thread-ownership-and-ingress-concurrency.md)
- [ADR 0069](0069-deterministic-raft-and-multiplexed-state-record.md)
- [ADR 0071](0071-segmented-multi-raft-persistence.md)
- [Segmented Multi-Raft Persistent Log](../learning/raft-persistent-log.md)
