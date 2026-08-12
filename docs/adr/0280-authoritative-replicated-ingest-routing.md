# ADR 0280: Authoritative replicated-ingest routing

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, metadata, Raft, and ingest maintainers

## Context

The bounded replicated-ingest coordinator previously required its caller to supply both a Raft
group and required leader term. Metadata now publishes committed tablet placement plus an immutable
tablet-to-group binding, and the asynchronous durable owner can return an ordered group
observation. Retaining caller-supplied routing authority would permit a request to target an
unrelated group or stale term despite those authoritative sources.

## Decision

`ReplicatedIngestCoordinator::admit` exact-decodes the canonical COLUMNAR_APPEND, pins the current
immutable metadata catalog, and joins its table/tablet identity to one committed placement and one
committed group binding. Missing placement or binding fails closed. The metadata group itself
cannot be used as a tablet group.

Admission enqueues an ordered observation of that derived group and retains the canonical command
under the existing coordinator capacity and deadline. `poll` consumes the observation before
proposal. It reacquires metadata, exact-revalidates the table, placement, and immutable group
binding, then requires all of the following:

- this node is the observed leader and names itself as leader in a nonzero term;
- the local node belongs to the committed placement;
- current and committed stable voters exactly equal the placement replicas; and
- no joint, finalizing, or pending membership transition is active.

Only then does the coordinator submit the command under the exact observed term. The durable
runtime checks that role/term precondition immediately before mutation, so leadership change after
observation rejects the proposal rather than admitting it under stale authority. Metadata leader
hints are advisory and are not consulted.

Cancellation or timeout can erase either the pending observation or ingest operation owner but
cannot undo work already admitted to the durable FIFO. The same finite pending-request bound covers
both phases.

## Consequences and validation

Callers no longer provide a group or term. Stable local leadership consistent with committed
metadata is the only route that reaches proposal. Placement/group absence, nonlocal leadership,
membership movement, or authority divergence yields a correlated error without appending the
command.

No durable or network bytes change. Focused tests cover a successful metadata-derived route,
bounded cancellation and timeout across the two-phase owner, missing-binding rejection, and
placement/voter divergence. Multi-node leader redirection, packaged reactor wakeups, concurrent
metadata movement races, crash cuts, TSan, and load measurement remain hardening work.

## Affected invariants

Invariants 1, 4–6, 9, 11, 14, 15, and 18 apply.

## References

- [ADR 0274](0274-nonblocking-replicated-ingest-operation.md)
- [ADR 0276](0276-bounded-replicated-ingest-coordinator.md)
- [ADR 0278](0278-worker-affine-metadata-application.md)
- [ADR 0279](0279-authoritative-tablet-group-binding.md)
