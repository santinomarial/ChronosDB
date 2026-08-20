# ADR 0410: Raft subscription snapshot and prefix reclamation

- **Status:** accepted
- **Date:** 2026-08-15
- **Owners:** ChronosDB live-query, ingest, and Raft maintainers
- **Extends:** [ADR 0073](0073-committed-raft-tablet-application.md),
  [ADR 0088](0088-owned-raft-tablet-snapshot-compaction.md),
  [ADR 0102](0102-exact-multi-tablet-subscription-snapshot.md),
  [ADR 0106](0106-topology-bound-subscription-retention.md), and
  [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)

## Context

The source-neutral subscription coordinator can record Raft group/log-index positions, but the
historical executor previously read only aggregate Manifest/WAL publications. The retention
authority could authorize a Raft logical frontier, but it had no safe composition from an
application snapshot through the node-shared physical Raft log. Reinterpreting a logical log index
as a shared segment coordinate would be incorrect, and deleting a shared segment for one group
could remove the only current state record for another group.

The asynchronous runtime exclusively owns `DurableMultiRaftRuntime`. Physical-log maintenance must
therefore execute on that worker rather than borrow the synchronous owner from a live-query thread.

## Accepted decision

Raft-backed historical subscription execution registers first, then acquires one pinned immutable
`TabletSnapshot` for every canonical member from the hosted `AsyncRaftTabletApplication`. Every
snapshot must exact-match table, tablet, Raft group, and registered applied log index. The complete
snapshot vector instantiates one global tablet-state physical pipeline; it is not finalized once per
tablet. Any mismatch or pre-READY failure abandons the registration. Historical source sets are
homogeneous: mixed WAL/Raft snapshots remain unsupported rather than combining incomparable
publication epochs.

`RaftSubscriptionSourceReclaimer` binds a fixed tablet/group/placement-epoch set to one asynchronous
runtime and the exact tablet application hosted by its worker. For the complete canonical request
batch, it verifies identity and topology, checks an immutable applied tablet publication at or beyond
each requested index, and obtains FIFO group observations proving both `applied_index` and the
durable Raft `snapshot_index` cover each frontier. Only after every read-only proof succeeds does it
enqueue one node-wide `checkpoint_and_reclaim` maintenance task.

The asynchronous runtime serializes this maintenance task in its existing bounded FIFO. The worker
creates and synchronizes a complete current checkpoint for every resident group before the
persistent-log owner removes older shared segments. Its typed single-consumer completion may be
waited only outside the worker. Dedicated reclamation counters distinguish maintenance admission,
completion, rejection, and failure from ordinary Raft batches. A maintenance failure is terminal
because durable-log mutation may already have begun and recovery must determine the authoritative
state.

## Consequences and alternatives

An application snapshot must already be durable before subscription retention can reclaim its Raft
prefix. This can retain more log history than the logical subscriber frontier requires, but it
prevents Raft recovery from outrunning application recovery. The node-wide checkpoint can also
retain segments because of unrelated groups; shared-log reclamation is intentionally conservative.

Reading mutable tablet machines from the subscription thread was rejected because it would violate
worker ownership. Running one physical plan per tablet was rejected because global aggregates,
ordering, and limits would be wrong. Mapping group indexes directly to physical segments and
checkpointing only requested groups were rejected because the physical stream is multiplexed.
Executing log maintenance directly on the caller was rejected because the synchronous runtime and
its file descriptors are worker-affine.

**Retrospective update (ADR 0411):** mixed historical execution now composes this exact immutable
Raft adapter with the existing aggregate WAL adapter. The registered source vector is the product
boundary; it does not claim that the independent publications form one scalar epoch.

## Affected invariants and validation

Invariants 1, 3, 8, 10, 11, 12, 15, and 17 apply. Focused tests apply a real Raft columnar append,
execute its exact historical aggregate, reject a stale registered boundary, route a new Raft query
through the subscription service, require matching application and durable Raft snapshot coverage,
perform worker-owned node-wide reclamation, and reopen the reclaimed log at the same committed
state. Header self-containment, Debug/Release suites, sanitizers, formatting, and static analysis
remain required by the task verification record.
