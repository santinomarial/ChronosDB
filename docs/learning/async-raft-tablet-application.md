# Worker-Affine Asynchronous Raft Tablet Application

## Purpose and public interfaces

`AsyncRaftTabletApplication` is the ingestion-layer consumer of
`AsyncDurableRaftWorkerExtension`. It closes the ownership gap between the one worker that owns
`DurableMultiRaftRuntime` and `RaftTabletStateMachine`, which intentionally borrows that synchronous
runtime to persist `applied_index` and prove applied quorum durability.

`create` accepts a bounded nonempty vector of unique group configurations. Each configuration owns
the fresh tablet, retry directory, retained schemas, decode limits, and optional durable application
snapshot storage needed for recovery. `AsyncDurableMultiRaftRuntime::create_new` or `open_existing`
then initializes the extension on its worker before opening admission.

External readers use `snapshot(group_id)` to acquire a pinned immutable `TabletSnapshot` and
`latest_quorum_sync_receipt(group_id)` to copy the latest receipt constructed while that group was
leader. The latter is an observation, not proposal correlation: a client response must eventually
bind its exact proposal/index rather than infer success from whichever receipt is latest.

## Data structures and invariants

Configurations are sorted by group identity at creation, which also makes adjacent duplicate
detection deterministic. Initialization converts them into sorted `OwnedTablet` records containing
one worker-owned state machine and an optional copied receipt.

Before the durable runtime moves request payloads, the per-batch context copies, sorts, and
deduplicates the request group identities. After durable execution, completion binary-searches only
those groups in the configured tablets. An unconfigured metadata or other Raft group is ignored by
this ingestion extension.

For every touched configured group, `apply_committed` preserves the state-machine ordering:

1. decode and validate complete committed command bytes;
2. publish rows and retry outcomes into the tablet;
3. synchronously persist the exact resulting `applied_index` through the worker-owned runtime; and
4. on a current leader with a nonzero applied frontier, construct and copy a quorum-sync receipt.

Only after all touched groups finish does the async runtime publish the original durable batch
completion. Therefore a successful completion cannot race ahead of query-visible tablet state.
Untouched tablets do no application work for that batch.

## Ownership, lifetime, and synchronization

Pending recovery configurations, live machines, and batch contexts are never exposed outside the
durable worker. The extension object may be shared with a service, but one mutex protects lifecycle,
failure, snapshot pinning, and copied receipt access. A reader may wait while application performs
storage I/O; it never observes a partially advanced tablet or borrows mutable state.

Shutdown runs on the worker before the physical log closes, destroys every machine while its
borrowed runtime is still alive, clears pending configurations after partial initialization, and
makes new snapshot/receipt observations unavailable.

## Failure behavior

Nil or duplicate groups and invalid tablet-count bounds fail before a worker starts. Recovery damage
prevents runtime construction from opening admission. Allocation and container-bound failures are
reported explicitly. If committed command decoding, tablet publication, applied-index persistence,
or receipt construction fails after a Raft transition, the extension records the first failure and
the asynchronous owner stops admission. No stale snapshot or receipt is returned after that
terminal failure. Restart recovery must reconcile the authoritative committed log before service
can resume.

## Complexity, tradeoffs, and benchmark methodology

For `T` configured tablets and a batch of `B` requests, configuration costs `O(T log T)`, context
preparation costs `O(B log B)`, and each unique touched group lookup costs `O(log T)` plus the actual
number and bytes of newly committed entries it applies. Memory is `O(T + B)` beyond tablet and
retained-log state. One worker preserves ordering but means slow tablet application delays Raft
transport completion.

No performance claim is made yet. The relevant benchmark should vary resident tablet count, unique
groups per batch, committed command bytes, and snapshot-reader contention; report completion
latency, applied commands per second, mutex wait time, and physical sync count. Compare touched-group
lookup against a measured alternative only before changing this deliberately simple sorted-vector
design.

## Likely interview questions

- Why must tablet machines be constructed on the durable worker rather than a service thread?
- Why is application performed after Raft persistence but before completion publication?
- Why does the batch context copy group identities before request payloads move?
- What does the mutex publish to snapshot readers, and why are machine references never returned?
- Why is an application failure terminal even if the Raft entry is already durable?
- Why is the latest copied receipt insufficient for exact client proposal correlation?
