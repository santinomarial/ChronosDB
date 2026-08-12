# Asynchronous Multi-Raft Owner

## Purpose and public interface

`AsyncDurableMultiRaftRuntime` moves durable Raft batches off producer threads while keeping the
existing physical log single-owned. `try_submit` either transfers a complete batch and returns an
`AsyncDurableRaftCompletion`, or immediately reports invalid input, overload, or closed admission.
The completion can be polled with `is_ready` or consumed with `wait`. Because transitions may own
large message batches, `wait` moves the result out exactly once instead of copying it.
Every admitted batch also receives a nonzero runtime-lifetime submission sequence under the FIFO
mutex. That sequence orders in-process completions across component queues; it is not durable state
and has no meaning after restart.
The runtime also exposes one borrowed nonblocking completion descriptor. Its worker publishes the
owning completion first and then signals that descriptor; one event loop drains the coalesced signal
and inspects every completion owner it coordinates.

Tablet reconfiguration uses `try_submit_local_prepared_tablet_reconfiguration`. It accepts only the
sealed capability produced after durable action-ledger preparation, copies its exact request into a
one-operation batch, and leaves the capability valid when admission rejects overload or shutdown.
The returned completion has the same publication and single-consumer rules as generic batches.

`try_observe_group` submits one `ObserveGroupOperation` through that same FIFO. Its completion owns
a bounded `RaftGroupObservation` with coherent role, term, leader, log frontiers, and stable/joint
membership facts. It contains no retained log payload or borrowed worker memory. Observation does
not create a transition or physical-log synchronization, but it is ordered after earlier admitted
work and is not released ahead of any synchronization required by earlier operations in its batch.

## Data structures and invariants

The runtime retains one FIFO of owning task objects. Admission counts every accepted batch and
operation until execution finishes, including the active task, so popping from the queue does not
silently create capacity. Limits exist at both levels because one vector can contain many bounded
operations. Metrics report the same retained quantities and high-water marks.

Only the worker calls `DurableMultiRaftRuntime`. Therefore its deterministic group transitions,
node-global physical sequence, append/sync batch, and persist-before-outbound contract remain
serialized exactly as in the synchronous owner. FIFO batch order is observable and deterministic.
An observation is a point-in-order fact rather than a lease: leadership can change after completion,
and callers must still validate term, placement epoch, commit, and application requirements.

## Ownership, lifetime, and synchronization

The producer fully initializes a task before publishing it while holding the queue mutex. Worker
acquisition of that mutex is the release/acquire edge for the task and operations. The worker
similarly installs a complete result under the completion mutex before notifying waiters. No atomic
counter participates in publication, and no thread is detached.

The completion pipe is nonblocking and close-on-exec. A successful write follows result publication;
pipe saturation coalesces wakeups because unread data already keeps the descriptor readable. Runtime
destruction joins the worker before closing either pipe descriptor.

Completion state is shared ownership, so it can outlive the runtime. Requests are unique ownership
and are released after exactly one completion. Runtime destruction stops admission, drains or
terminally rejects every accepted task, closes storage, and joins before destroying state.

## Failure behavior

Capacity rejection changes no Raft or disk state. Per-operation statuses remain ordinary successful
batch results. A top-level durable failure or unexpected exception is ambiguous with respect to
partial in-memory progress, so the worker fails closed, gives all remaining accepted work the same
terminal failure, and stops. Shutdown returns the retained storage/worker failure.

## Complexity, tradeoffs, and interview questions

Admission is `O(batch operations)` for transferred ownership and `O(1)` queue work. Execution keeps
the synchronous batch complexity. One worker enables physical batching but cannot parallelize a
single log; fairness is FIFO and only bounded by maximum batch size.

- Why does active work continue consuming admission capacity?
- Which mutex edges publish tasks and results?
- Why is a top-level execution exception terminal rather than retryable?
- Why does shutdown drain normal work but reject queued work after a terminal failure?
- When must a reactor avoid calling `wait`?
