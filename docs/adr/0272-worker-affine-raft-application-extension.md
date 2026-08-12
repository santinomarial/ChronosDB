# ADR 0272: Worker-affine durable Raft application extension

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft, ingestion, metadata, and runtime maintainers

## Context

`AsyncDurableMultiRaftRuntime` exclusively owns `DurableMultiRaftRuntime` and its shared physical
log on one background thread. Tablet and metadata state machines intentionally borrow the
synchronous runtime because application must persist `applied_index` and compose an exact
`QuorumSyncReceipt`. Constructing those owners outside the durable worker creates an invalid
cross-thread borrow; moving the synchronous runtime back out would break the transport/timer
serialization and persistence boundary.

The transport runtime publishes a completion only after a durable batch returns. That point is the
one deterministic seam where committed application can run on the same owner before outbound work
or a client acknowledgement is externally consumed.

## Decision

The asynchronous runtime accepts one optional shared `AsyncDurableRaftWorkerExtension`. Its hooks
run serially on the durable worker:

1. `initialize(runtime)` runs before `create_new` or `open_existing` returns and before admission
   opens. It may recover worker-affine tablet and metadata application owners against the supplied
   runtime.
2. `prepare_batch(runtime, requests)` runs immediately before request payloads are moved into the
   durable runtime. It returns one non-null opaque context whose destructor remains worker-affine.
3. `complete_batch(runtime, context, results)` runs after the durable persistence boundary and
   before the asynchronous completion is published. It may apply newly committed entries, persist
   applied indexes synchronously, resolve proposal correlation captured by the context, and produce
   application/quorum receipts in extension-owned state.
4. `shutdown(runtime)` runs on the worker before the durable log closes. It is also invoked after a
   partially failed initialization and therefore must tolerate partial extension state.

Any hook error or exception fails the asynchronous owner closed. A missing batch context is an
internal error. The extension is not a second scheduler: it receives no thread, queue, or hidden
durability path. Hooks must not wait for work submitted to the same asynchronous runtime, which
would deadlock; they may call the supplied synchronous runtime directly under its sole owner.

The extension interface remains in `chronos::raft` and carries only Raft requests/results. Ingest
and metadata implementations live in their higher-level libraries, preventing a Raft-to-ingest
dependency cycle. A shared extension may expose separately synchronized admission/completion state
to another thread, but its worker-owned application objects never escape.

`AsyncRaftTabletApplication` is the first concrete extension. It takes a bounded, unique set of
group/tablet recovery configurations, sorts them once, and recovers every `RaftTabletStateMachine`
on the worker before admission. Each batch context retains the sorted unique group identities from
the still-owned requests. Completion applies only configured groups touched by that batch, advances
their durable applied indexes, and records a leader receipt after visibility. External code may
copy the latest receipt, acquire a pinned immutable `TabletSnapshot`, or own a bounded exact
group/term/index receipt completion; it cannot borrow a mutable machine or the synchronous runtime.

## Consequences

- Durable Raft, application publication, `applied_index`, and receipt construction can share one
  explicit thread-affine lifetime.
- Original transport results remain unchanged and are published only after extension completion.
- Application failure after a durable Raft transition is terminal: the log may contain the entry,
  but the process returns no success and must recover the authoritative committed state on restart.
- Deployments without an extension retain the prior queue, metrics, completion, and shutdown
  behavior.
- Long application work delays Raft completions. Application batching/fairness must be measured and
  bounded by the concrete extension rather than hidden here.
- The concrete tablet extension costs one bounded group-identity copy/sort per batch and avoids a
  scan of every resident tablet. Snapshot readers serialize with worker application while they pin
  the immutable tablet view.

## Affected invariants and validation

Invariants 1, 4, 5, 8, 11, 14, 15, 16, and 18 apply. Focused tests prove initialization completes
before construction returns, all hooks use one worker thread, committed application can persist
`applied_index` and construct a quorum receipt before proposal completion, shutdown remains on that
thread, reopen retains the applied frontier, and initialization failure prevents admission. The
concrete tablet tests prove touched-group application before completion, untouched-group isolation,
pre-admission restart reconstruction, duplicate-group rejection, and terminal corruption handling.

The metadata extension, proposal-result index extraction, transport/client integration, crash cut
points, TSan, and scheduling measurements remain subsequent work.

## References

- [ADR 0073](0073-committed-raft-tablet-application.md)
- [ADR 0074](0074-quorum-sync-proof-boundary.md)
- [ADR 0250](0250-async-durable-raft-timer-driver.md)
- [ADR 0271](0271-native-protocol-v2-quorum-sync-negotiation.md)
