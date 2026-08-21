# Asynchronous Multi-Raft Owner

## Purpose and public interface

`AsyncDurableMultiRaftRuntime` moves durable Raft batches off producer threads while keeping the
existing physical log single-owned. `try_submit` either transfers a complete batch and returns an
`AsyncDurableRaftCompletion`, or immediately reports invalid input, overload, or closed admission.
The same operation-aware outbound reservation used by the synchronous durable owner runs before
queue publication, so an undersized batch is rejected as ordinary backpressure without stopping the
worker.
The completion can be polled with `is_ready` or consumed with `wait`. Because transitions may own
large message batches, `wait` moves the result out exactly once instead of copying it.
Every admitted batch also receives a nonzero runtime-lifetime submission sequence under the FIFO
mutex. That sequence orders in-process completions across component queues; it is not durable state
and has no meaning after restart.
The runtime also exposes one borrowed nonblocking completion descriptor. Its worker publishes the
owning completion first and then signals that descriptor; one event loop drains the coalesced signal
and inspects every completion owner it coordinates.

An optional `AsyncDurableRaftWorkerExtension` composes application state with that same owner.
`initialize` runs before construction returns and admission opens. For each accepted batch,
`prepare_batch` sees the still-owned requests before payload moves and creates one opaque context;
`complete_batch` receives that context and the post-sync results before completion publication.
`shutdown` runs before the log closes. These hooks let a higher-level ingest/metadata library own
state machines that borrow the synchronous runtime without creating a reverse Raft dependency.
The runtime measures each top-level extension hook against a positive configurable monotonic
watchdog threshold. Metrics identify a currently active hook and its elapsed duration, and retain
per-hook invocation/completion counts, threshold violations, and maximum completed duration. This
is diagnosis rather than cancellation: a hook retains sole ownership until it returns, and crossing
the threshold cannot change persistence or result publication.
When more than one application owner is required, `AsyncDurableRaftWorkerExtensionSet` composes a
flat bounded list. It initializes, prepares, and completes children in declaration order, retains
one opaque child context per batch, and shuts down every attempted child in reverse order. The
runtime's identity check recognizes both the set and its direct children, so a higher-level owner
still proves it is hosted by the exact worker before admitting dependent work.
`AsyncRaftTabletApplication` is the first concrete consumer: it recovers bounded tablet machines on
the worker, applies only request-touched groups before completion publication, and exposes only
pinned immutable snapshots, copied observations, and bounded exact term/index receipt completions
to other threads.

Tablet reconfiguration uses `try_submit_local_prepared_tablet_reconfiguration`. It accepts only the
sealed capability produced after durable action-ledger preparation, copies its exact request into a
one-operation batch, and leaves the capability valid when admission rejects overload or shutdown.
The returned completion has the same publication and single-consumer rules as generic batches.

`try_observe_group` submits one `ObserveGroupOperation` through that same FIFO. Its completion owns
a bounded `RaftGroupObservation` with coherent role, term, leader, log frontiers, and stable/joint
membership facts. It contains no retained log payload or borrowed worker memory. Observation does
not create a transition or physical-log synchronization, but it is ordered after earlier admitted
work and is not released ahead of any synchronization required by earlier operations in its batch.

`try_checkpoint_and_reclaim` submits a distinct node-wide maintenance task through the same bounded
FIFO. It returns a typed single-consumer completion carrying the installed recovery anchor and
reclaimed-segment counts. The worker calls the synchronous owner's complete all-group checkpoint;
application hooks do not run because the task contains no logical transition. Dedicated metrics
separate reclamation admission/results from ordinary Raft batches. A caller must not wait from a
worker extension, for the same reason it must not wait on a submitted batch there.

## Data structures and invariants

The runtime retains one FIFO of owning task objects. Admission counts every accepted batch and
operation until execution finishes, including the active task, so popping from the queue does not
silently create capacity. Limits exist at both levels because one vector can contain many bounded
operations. Metrics report the same retained quantities and high-water marks.
Extension measurements live under the same metrics mutex. The worker records an active hook and its
monotonic start before invocation; `metrics()` copies that state and computes a fresh live elapsed
duration. On every normal or exceptional return, the worker clears the active identity and updates
the matching completed count, maximum duration, and saturating threshold-violation count. The
configured threshold is also copied into each snapshot so monitoring interprets the counters under
the same runtime contract.

Only the worker calls `DurableMultiRaftRuntime`. Therefore its deterministic group transitions,
node-global physical sequence, append/sync batch, and persist-before-outbound contract remain
serialized exactly as in the synchronous owner. FIFO batch order is observable and deterministic.
An observation is a point-in-order fact rather than a lease: leadership can change after completion,
and callers must still validate term, placement epoch, commit, and application requirements.
An extension hook error is a top-level owner failure, not a per-operation rejection: the durable
transition may already exist, so no result can be advertised until restart recovery reconciles
application state.

## Ownership, lifetime, and synchronization

The producer fully initializes a task before publishing it while holding the queue mutex. Worker
acquisition of that mutex is the release/acquire edge for the task and operations. The worker
similarly installs a complete result under the completion mutex before notifying waiters. No atomic
counter participates in publication, and no thread is detached.

The completion pipe is nonblocking and close-on-exec. A successful write follows result publication;
pipe saturation coalesces wakeups because unread data already keeps the descriptor readable. Runtime
destruction joins the worker before closing either pipe descriptor. Saturating metrics count
physical signal bytes separately from full-pipe coalescing; they provide diagnostics but do not
replace the completion mutex or descriptor readiness as publication edges. A bounded test leaves the
real pipe unread until coalescing occurs, consumes every completed observation, drains the pipe, and
proves a later completion creates a fresh wakeup.

Completion state is shared ownership, so it can outlive the runtime. Requests are unique ownership
and are released after exactly one completion. Runtime destruction stops admission, drains or
terminally rejects every accepted task, closes storage, and joins before destroying state.
A controlled test blocks the worker at the exact 64-task admission bound, races eight producers
with two shutdown callers, and verifies every accepted sequence completes once, both shutdown calls
converge, terminal metrics are exact, and the joined runtime's descriptor remains drainable.
An extension object may be shared with an embedding for separately synchronized admission/result
coordination, but its application owners and batch contexts remain worker-affine. A hook must never
wait on a completion submitted to this same runtime; the sole worker would be waiting on itself.

## Failure behavior

Capacity rejection changes no Raft or disk state. Per-operation statuses remain ordinary successful
batch results. A top-level durable batch or checkpoint/reclamation failure is ambiguous with respect
to partial in-memory or filesystem progress, so the worker fails closed, gives all remaining
accepted work the same terminal failure, and stops. Shutdown returns the retained storage/worker
failure.

Focused coverage blocks a deterministic durable record-limit failure with eight observations queued
behind it, starts shutdown, and verifies orderly admission closure returns `UNAVAILABLE` before the
failure while the current and all accepted queued completions later receive the identical retained
terminal status. Failure and rejection metrics, notification counts, and pending ownership all
converge exactly after the join.

Initialization failure prevents admission and still invokes extension shutdown for partial cleanup.
For a composed extension, that cleanup includes the child whose initialization returned failure;
shutdown continues through earlier children even when a later child returns an error or throws.
A shutdown-hook failure happens only after accepted work drains: it marks the owner terminal and is
returned by repeated shutdown, but does not retroactively replace an already published successful
completion. Focused coverage persists an election before a child throws during reverse shutdown and
then reopens the exact term and vote.
When multiple children return shutdown failures, all children still run and the first failure in
reverse invocation order wins. The runtime retains that exact status for later shutdown callers
without invoking child cleanup again.
The composed extension classifies allocation failure separately from an arbitrary child exception.
Exhaustive sweeps cover its two creation allocations and its batch context-vector and composite-
context allocations. A failed creation releases transferred child ownership; a failed preparation
destroys every partial child context on the worker and can be retried. A child allocation failure
during any lifecycle hook is resource exhaustion, while initialization and shutdown preserve their
complete reverse-cleanup rules.
Extension shutdown precedes physical-log close because extension cleanup may still use the durable
owner. If both layers fail, the extension status therefore remains the root cause, while the active
file, advisory lock, and directory are still all closed exactly once. If the extension succeeds,
the first physical close error becomes terminal. An exhaustive real-filesystem matrix proves both
branches for every nonempty combination of those three physical failures after one accepted durable
election; the completion stays successful, metrics converge, repeated shutdown is inert, and the
exact term and vote reopen.
Watchdog threshold crossings are not failures. Forced interruption could abandon a partially
applied application callback while the worker still owns the synchronous Raft runtime, so the
runtime only reports the live or completed overrun. The callback's own status and the existing
fail-closed rules remain authoritative.

## Complexity, tradeoffs, and interview questions

Admission is `O(batch operations)` for transferred ownership and `O(1)` queue work. Execution keeps
the synchronous batch complexity. One worker enables physical batching but cannot parallelize a
single log; fairness is FIFO and only bounded by maximum batch size.

- Why does active work continue consuming admission capacity?
- Why does extension completion precede external result publication?
- Why is extension composition flat, bounded, and reverse-ordered during shutdown?
- Why may an extension call the synchronous runtime but not submit and wait on the async owner?
- Which mutex edges publish tasks and results?
- Why is a top-level execution exception terminal rather than retryable?
- Why does shutdown drain normal work but reject queued work after a terminal failure?
- Why does physical-log reclamation require an all-group checkpoint on the owning worker?
- When must a reactor avoid calling `wait`?
