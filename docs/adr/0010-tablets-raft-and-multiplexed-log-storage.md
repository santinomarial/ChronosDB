# ADR 0010: Tablets, Raft, and Multiplexed Log Storage

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB distributed-systems maintainers

## Context

ChronosDB needs a future distribution unit that preserves per-partition ordering, storage snapshots, rebalancing, and explicit read consistency. Giving every partition an independent physical log and synchronization stream would scale poorly, while one global consensus group would couple unrelated workloads and limit parallelism. These decisions must constrain early identities and interfaces without authorizing premature distributed implementation.

## Accepted decision

Tables are divided into logical tablets. A tablet is a replicated deterministic state machine and, in the distributed phase, has its own logical Raft group. A small separate metadata Raft group owns schemas, tablet placement, membership, and cluster metadata.

Many logical Raft groups resident on one node share a multiplexed physical segmented log, shared I/O resources, and an explicitly fair runtime. Physical records retain group identity, logical term/index, integrity coverage, and enough metadata for independent recovery and reclamation. A physical offset never substitutes for a tablet's logical Raft position.

Committed entries are applied deterministically in logical log-index order per tablet. An appended or locally durable but uncommitted entry is never query-visible, emitted live, or reflected in a materialized view.

Raft snapshot installation uses immutable CSEG part sets plus an atomic manifest boundary and the state-machine position they cover. Installation must retain old readable state until the new snapshot is complete and selected.

Rebalancing first adds an eligible replica, catches it up and validates it under the membership protocol, and only then removes an old replica. Read consistency is explicit—such as linearizable or bounded-stale under later definitions—and never silently downgraded.

The single-node log, apply, CSEG, manifest, and snapshot interfaces must be reusable by the replicated state machine. Distribution adds replicated ordering and coordination rather than a separate persistence engine.

Implementation remains deferred until single-node gates pass.

## Detailed rationale

Per-tablet groups isolate failure, placement, and write order while allowing independent leadership and scaling. Multiplexing amortizes synchronization and file-management cost across many groups without conflating their logical histories. Immutable part-set snapshots align consensus transfer with the same crash-safe storage boundary used locally.

A metadata group provides a small authoritative source for placement and schema epochs; it must not become a high-volume data path. Add-before-remove rebalancing protects replication level while data moves.

## Alternatives considered

- **One global Raft group:** simplifies total ordering but serializes unrelated tablets, couples failures, and limits horizontal write scaling.
- **One physical WAL and fsync stream per tablet:** preserves isolation but creates excessive files, sync streams, timers, and recovery overhead at high tablet counts.
- **Use an existing KV/Raft engine:** would introduce a second persistence/state-machine model and outsource a core subsystem this project is required to own.
- **Expose leader-appended entries:** lowers apparent latency but violates committed visibility and can return entries later rolled back.
- **Remove old replica before catch-up:** reduces temporary resource use but lowers redundancy and risks unavailability/data loss during movement.
- **Implement distribution before local storage is proven:** combines consensus and crash-consistency failures without a stable oracle.

## Consequences

- Tablet/group identity is present in future log, snapshot, routing, and resume contracts.
- The physical log must provide fairness, per-group recovery, independent truncation safety, and isolation from a corrupt record.
- Metadata availability and data-tablet availability have distinct operational behavior.
- Distributed snapshots and queries need explicit compatible per-tablet positions rather than a fictional global wall-clock order.
- Rebalancing temporarily consumes extra storage and network capacity.

## Affected invariants

This decision directly governs invariants [1, 4, 5, 6, 8, 10, 11, 12, 14, and 17](../architecture/invariants.md): quorum durability, ordered deterministic apply, committed visibility, snapshot stability, idempotent recovery, integrity/lifetime, unambiguous positions, versioned formats, and live handoff across topology.

## Validation plan

- Build Raft first in deterministic simulation with virtual time, controlled networks, and faultable disks.
- Compare replicas after arbitrary partitions, duplication, reordering, crash/restart, snapshots, and leadership/membership changes.
- Stress thousands of logical groups sharing a physical log; assert no cross-group apply or premature reclamation and measure starvation/fairness.
- Inject failure at every snapshot and rebalancing step and require old or new complete membership/storage state.
- Test every consistency mode against a formal history/checker and label returned commit/applied positions.

## Deferred decisions

Tablet partitioning, metadata schema, Raft wire/persistent formats, election/read protocol details, membership algorithm, physical-log record/layout and sync policy, scheduler, fairness bounds, snapshot transport, consistency-mode definitions, placement policy, and distributed query snapshot coordination remain deferred.

## Migration or reversal implications

No distributed state exists. Early local identities should allow versioned extension rather than pretending Raft fields already have values. Once groups and log formats are deployed, changing tablet identity, consensus history, or multiplexed layout requires coordinated mixed-version protocols and possibly offline conversion. A different consensus algorithm requires a superseding ADR and state migration plan.

## References

- [Single-node-first ADR](0003-single-node-first-development-order.md)
- [Architecture future distribution](../architecture/overview.md)
- [Roadmap phases 14–16](../roadmap.md)
