# ADR 0273: Bounded term-bound applied-quorum completions

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft, ingestion, service, and runtime maintainers

## Context

Protocol 2.0 can carry a complete `QuorumSyncReceipt`, and the worker-affine tablet application can
construct one only after the committed entry is query-visible and its `applied_index` is durable.
Its latest copied receipt is useful observation state but cannot correlate a client response: a
later proposal may advance the group before the service reads it, and a node may lose and later
regain leadership.

A replicated service therefore needs a bounded asynchronous owner for one exact accepted proposal
identity: Raft group, admitting leader term, and log index. It must not block the sole durable worker
or retain abandoned client requests forever.

## Decision

`AsyncRaftTabletApplication::request_quorum_sync` accepts an exact `(group_id,
required_leader_term, log_index)` after proposal admission has supplied those values. Under the
application mutex it validates the active configured group, enforces one node-wide pending bound,
and installs a weak reference to a new single-consumer completion. Before registration it also
checks immutable extension identity so the observation cannot be sent to a different async worker.
It then submits an ordered group observation to the same bounded asynchronous runtime:

- admission rejection returns the error and leaves no externally owned waiter;
- if the exact index is already applied, the observation drives resolution;
- otherwise a later request for that group, normally a replication response that advances commit,
  drives application and resolution; and
- a nonleader or different current term resolves the waiter with `UNAVAILABLE` rather than allowing
  a future leadership term to acknowledge the stale request.

The worker resolves a success only after `RaftTabletStateMachine::apply_committed` has published the
tablet state, synchronously persisted `applied_index`, and proved the exact index on the exact
current leader term. Resolution results for all touched groups are staged until the entire worker
extension hook succeeds. Only then are waiter states completed; a later failure in the same hook
cannot race an externally visible success.

The move-only `AsyncRaftTabletQuorumCompletion` owns the request. Application storage retains only a
weak pointer, so dropping the completion cancels it without a cross-thread callback. Registration
and metrics prune expired waiters. Terminal application failure and orderly shutdown complete every
surviving waiter with an error before destroying tablet machines.

`wait()` is not a worker API and must never run on the durable Raft thread. This increment adds no
second completion descriptor: a production service must poll `is_ready` from its existing result
coordination path, enforce its request deadline, and drop the owner on cancellation/disconnect.

## Consequences

- A replicated QUORUM_SYNC response can depend on its exact applied proposal rather than a latest
  group receipt.
- Waiters are finite and abandoned owners do not consume retained application memory.
- Each registration consumes one ordinary bounded async observation admission. Overload is
  explicit and changes no tablet/Raft state.
- A waiter may remain pending while its exact leader remains active but the entry lacks majority
  commit; the service deadline/cancellation owner remains required.
- Resolution scans pending waiters only for request-touched groups. High pending counts still add
  worker latency and require measurement in the final hardening pass.
- No durable, Raft, or network bytes change.

## Affected invariants and validation

Invariants 1, 4, 5, 8, 11, 14, 15, 16, and 18 apply. Focused tests cover immediate single-node
resolution, delayed three-voter majority commit, exact leader-term loss, pending-capacity rejection,
dropped-owner reclamation, shutdown completion, terminal corrupt-command failure, and suppression of
a staged success when a later touched group fails the same hook.

Proposal-result index extraction, reactor wakeup/service deadlines, protocol-v2 response encoding,
disconnect races, crash cut points, TSan, and scheduling/latency measurements remain subsequent
work.

## References

- [ADR 0074](0074-quorum-sync-proof-boundary.md)
- [ADR 0271](0271-native-protocol-v2-quorum-sync-negotiation.md)
- [ADR 0272](0272-worker-affine-raft-application-extension.md)
