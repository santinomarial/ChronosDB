# ADR 0345: Packaged remote bounded-stale grouped-query lifecycle

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB service, cluster, query, and networking maintainers
- **Extends:** [ADR 0317](0317-packaged-remote-bounded-stale-query-lifecycle.md),
  [ADR 0344](0344-packaged-bounded-stale-grouped-query-construction.md)

## Context

The grouped bounded-stale constructor accepted a complete correlated authority vector, while the
placement-backed authenticated batch owner acquired that vector remotely. An embedding still had
to retain the plan and Manifest pin across acquisition, transfer the exact result into grouped
construction, and unify cancellation, failure, metrics, and result publication across both phases.

## Decision

`ReplicatedFollowerDistributedGroupedFloat64Query` is a move-only, single-threaded lifecycle owner
with acquiring-authority, executing, complete, failed, and cancelled states. Creation requires a
bounded-stale plan and exact source-node/authenticator/authorizer agreement between the observation
and grouped-query configurations. It constructs the placement-backed canonical batch while owning
the plan and pinned Manifest snapshot; construction itself opens no socket.

While acquiring, `poll_once` drives the batch owner and exposes no partial authority. Once all
group-sorted pairs are complete, it transfers the owning plan, snapshot, and result vector directly
into `create_replicated_follower_distributed_grouped_float64_query`. That call reacquires the
metadata-group barrier and enters the follower binder, active-schema FLOAT64 specialization,
committed route resolution, grouped execution, and TCP scheduler. Query sockets begin only on a
later poll.

Cancellation targets the active phase and becomes one sticky service status. Failures cancel any
live child owner. Authority and execution metrics remain distinct; results are available only after
grouped execution completes. Borrowed catalog, barrier, projection, authentication, and TLS policy
must outlive the lifecycle owner. No durable or network format changes.

## Consequences and validation

One owner now preserves the Manifest pin and authority chain from committed placement through
authenticated remote observations into grouped execution. Memory and work retain the finite bounds
of the observation batch and grouped scheduler. The owner is unsynchronized and requires serial
polling, so no memory-ordering argument applies.

The focused service test drives two real mutual-TLS observation servers, acquires one leader/
follower pair exactly once for the grouped lifecycle, transitions to a running grouped TCP owner,
exposes both phase metrics, and proves execution-phase cancellation is sticky. Header
self-containment and the installed-consumer gate cover the public lifecycle.

Deterministic allocation sweeps now cover lifecycle construction and the real mutual-TLS
authority-to-execution transition. Every selected allocation failure returns sticky
`RESOURCE_EXHAUSTED`, cancels authority, installs no execution or result, and restores the exact
Manifest-pin count. The service and grouped TCP scheduler implementations therefore do not claim
`noexcept` around constructors that retain allocating diagnostic statuses.

The test does not complete a real remote CSEG response. Explicit grouped authority rebinding,
general vector fragments, multi-key/non-FLOAT64 grouping, real multi-process failover, and broad
fault/measurement evidence remain incomplete. No Phase 16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Packaged remote bounded-stale query lifecycle](0317-packaged-remote-bounded-stale-query-lifecycle.md)
- [Packaged bounded-stale grouped-query construction](0344-packaged-bounded-stale-grouped-query-construction.md)
- [Placement-backed Raft observation batch construction](0316-placement-backed-raft-observation-batch-construction.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
