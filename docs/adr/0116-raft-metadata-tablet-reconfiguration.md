# ADR 0116: Raft and metadata tablet reconfiguration

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and metadata maintainers

## Context

`TabletMovement` enforced learner-first snapshot transfer and local phase ordering, while Raft
implemented joint consensus and the metadata group implemented placement epochs. Nothing required
those three state machines to agree before local promotion or source removal. Advancing placement
first could route work to a nonvoter; advancing movement first could claim promotion that had not
committed; removing the source before the new configuration was durable could lose availability.

## Decision

`TabletReconfigurationCoordinator` is a deterministic desired-state reconciler over one ready
movement, its tablet Raft group, and committed metadata state. It emits at most one exact
`DurableRaftRequest` per reconciliation. The caller executes that action, waits for the relevant
commit/application observation, and reconciles again.

Target promotion follows this exact order:

1. begin joint consensus from the current voters to current voters plus the caught-up target;
2. wait for the joint entry to commit, then emit finalization;
3. wait for the final configuration to commit;
4. propose the next metadata placement epoch with the promoted replica set;
5. only after that metadata command is applied, record local `kTargetPromoted`.

Source removal repeats the same joint/final sequence from the promoted set to the set without the
source, publishes the following placement epoch, and only then records `kComplete`. A leader hint
is cleared if its node leaves the new replica set. Stable and joint voter sets are exact-compared
with intent; a divergent configuration or placement is corruption, never an implicit rewrite.

The Raft node exposes read-only stable/joint membership views and whether finalization is currently
eligible or already pending. These spans remain node-owned and are valid only until the next node
mutation. No durable or wire bytes change.

## Detailed rationale

Raft membership is the data-availability authority; metadata placement is the routing authority.
Neither can substitute for the other. Observing both before advancing local orchestration creates
an auditable handoff and makes retries deterministic. The coordinator emits existing durable
operations, so persist-before-send and metadata application ordering remain owned by their existing
runtimes.

## Alternatives considered

- **Mutate `TabletMovement` after submitting a command:** submission is not commit.
- **Publish placement before Raft finalization:** can route to a learner or removed source.
- **Infer joint intent from the active voter union:** different old/new configurations can share a
  union; exact old and new sets are required.
- **Directly replace the voter list:** violates joint-consensus safety.
- **Automatically repair divergent metadata:** hides competing control-plane history.

## Consequences

Promotion/removal cannot complete without both committed authorities. Reconciliation may return no
action while joint or final entries are in flight and may return `UNAVAILABLE` when the tablet group
is not locally led. The caller must serialize one returned action at a time and route metadata-group
requests to its leader. Durable movement-intent storage, restart reconstruction, resumable snapshot
files, and transport remain follow-up work.

## Affected invariants

Invariants 1, 4–6, 8, 11, 14, and 18 apply. The ordering prevents premature routing/removal,
preserves committed tablet identity, and uses only existing versioned commands.

## Validation plan

A focused test drives old and new quorums through promotion joint/final, metadata epoch publication,
removal joint/final, leader removal, final metadata publication, and completion. It proves no action
is emitted before joint commit and that removed leader hints clear. Negative coverage rejects a
divergent placement without changing movement state. Restart, duplicate action, leadership churn,
snapshot-file faults, metadata-group failover, and multi-node transport remain deferred.

## Migration or rollback considerations

This is a pre-alpha orchestration API over existing bytes. Rollback removes the reconciler but must
not restore direct unproven calls as a production path.

## Unresolved questions

Durable movement intent/checkpoint format, action identifiers for cross-process idempotency,
snapshot transport ownership, metadata/tablet leader routing, bandwidth scheduling, and cleanup of
the old replica's physical data remain unresolved.

## References

- [ADR 0070](0070-feature-pass-logical-boundaries.md)
- [ADR 0075](0075-durable-metadata-raft-commands.md)
- [ADR 0076](0076-joint-consensus-raft-membership.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
