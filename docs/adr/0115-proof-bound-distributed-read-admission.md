# ADR 0115: Proof-bound distributed read admission

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB query and distributed-systems maintainers

## Context

Distributed plans carried three named consistency modes, but the coordinator accepted every plan
without evidence. “Leader linearizable” did not require a Raft leadership proof or application
frontier, and “follower bounded stale” did not name a numeric bound or leader commit observation.
Those labels could silently serve weaker state while reporting stronger semantics.

## Decision

Every planned fragment requires one exact `DistributedReadAdmission` before coordinator creation.
Admissions bind tablet identity, serving node, applied position, observed leader commit position,
and an optional actual Raft `ReadBarrier`. Duplicate, missing, foreign, or structurally incompatible
admissions reject the complete query before fragment results are accepted.

Leader-linearizable admission requires the planned leader node, a nonzero Raft barrier, and an
applied position covering its read index. Bounded-stale policy requires an explicit maximum log-
position lag. Admission must carry a leader-commit observation at least as new as planning metadata,
and the nonnegative gap from that observation to applied state must fit the bound. Local-eventual
requires an explicit serving node but no Raft proof and rejects a supplied barrier so it cannot be
misreported as stronger evidence.

The legacy enum overload remains for leader-linearizable and local-eventual callers. Selecting
bounded-stale through that overload fails because it cannot express the required numeric bound.
No mode silently falls back to another.

## Detailed rationale

Admission is the last safe boundary before distributed worker results enter the coordinator. Using
Raft log positions provides an exact deterministic staleness unit without inventing wall-clock
lease assumptions. Requiring applied coverage separately preserves the committed-and-applied
visibility rule.

## Alternatives considered

- **Trust the enum label:** provides no executable consistency guarantee.
- **Use follower wall-clock age:** needs clock and replication-delay assumptions not present in the
  deterministic system.
- **Treat bounded stale as local eventual:** is an undocumented downgrade.
- **Validate only at final merge:** wastes distributed work and may mix incompatible fragment
  snapshots before failure.

## Consequences

Coordinator construction now fails closed until every fragment has evidence appropriate to its
policy. Callers must carry the group-scoped runtime barrier to the matching tablet admission and
wait for apply. Multi-tablet atomic snapshot coordination, routing epochs, proof freshness during
long scans, and transport encoding remain follow-up work.

## Affected invariants

Invariants 4–6, 14, and 18 apply. The admission boundary prevents uncommitted/unapplied reads and
silent consistency weakening. The evidence structs are in-memory values and add no released wire
format.

## Validation plan

Focused tests cover applied and unapplied linearizable barriers, missing bounded-stale bounds,
within/outside position lag, stale leader observations, local eventual admission, and coordinator
rejection. Multi-node acquisition, leader changes during scans, compatible cross-tablet snapshots,
network versioning, partitions, and deterministic fault simulation remain deferred.

## Migration or rollback considerations

This is a source-level pre-alpha API tightening with no durable or wire bytes. All coordinator
callers must supply admissions. Rollback restores the weaker interface and is not semantics-safe for
clients promised explicit consistency.

## Unresolved questions

Tablet-to-group identity binding, multi-tablet snapshot vectors, routing-epoch freshness, follower
selection, and protocol representation remain unresolved integration work.

## References

- [ADR 0069](0069-deterministic-raft-and-multiplexed-state-record.md)
- [ADR 0113](0113-linearizable-raft-read-barrier.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
