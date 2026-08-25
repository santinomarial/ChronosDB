# ADR 0488: Coherent replicated grouped SQL preparation

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB SQL, replicated-service, Manifest, and distributed-query maintainers
- **Extends:** [ADR 0486](0486-replicated-grouped-sufficient-state-preparation.md) and
  [ADR 0487](0487-direct-grouped-sufficient-state-sql-lowering.md)

## Context

Direct grouped SQL could produce the exact sufficient-state intent and the replicated service could
execute a caller-supplied grouped plan, but the caller still had to construct every
`DistributedTablet`. That join spans committed placements, immutable tablet-to-group bindings,
one Manifest generation, and one correlated read-authority vector. Omitting a committed tablet or
mixing a plan position from another publication would produce an incomplete or unavailable query.

## Decision

Add a leader-linearizable replicated SQL constructor that consumes one
`DistributedVectorGroupedAggregateSqlPlan` and one owning Manifest snapshot. Before transport it:

1. validates the query identity and requires the configuration's table, projection, event-time
   predicate, grouped mode, and fragment limit to exactly match the lowered SQL product;
2. acquires the complete configured authority vector exactly once and proves metadata barrier
   coverage;
3. selects every committed placement for the table, bounded before plan allocation;
4. requires a matching immutable tablet-group binding, Manifest tablet, Raft source, recovery
   schema, and group authority for each placement;
5. requires the table's Manifest tablet set and committed placement set to have equal cardinality;
6. derives the leader node and applied/commit positions from that same authority and Manifest
   publication; and
7. transfers the derived plan, moved SQL result schema, snapshot pin, routes, and finite policies
   into the existing grouped sufficient-state TCP/Native lifecycle.

The caller-visible configuration remains shared with the generic grouped constructor. Its
redundant SQL binding fields are checked rather than silently overwritten, making publication
mixups explicit.

## Consequences

An embedding no longer constructs grouped SQL tablet fragments or chooses a subset of committed
tablets. A successful owner represents one complete catalog/Manifest/authority join and retains
the Manifest pin through atomic Native publication. Missing group authority, schema drift, and
catalog/Manifest tablet-set drift fail before any worker attempt begins.

This path still targets immutable Manifest/CSEG state. Mutable Native SQL needs a distinct
TabletState sufficient-state worker and cannot substitute this owner. Computed expression splitting
and partitioned shuffle routing are also separate work.

The constructor is single-thread-affine and introduces no shared publication, so no new
memory-ordering argument applies. It adds no durable or network format.

## Validation

The real-Manifest replicated fixture proves derived query/tablet/group identity, exact durable
position, unique projection remapping, grouped intent, result schema, generation pin, running
lifecycle, missing tablet-group authority rejection, and configuration-mismatch rejection.
Allocation injection covers every service-owned preparation and construction allocation and proves
the Manifest pin returns to its baseline after every failure and success.

The complete 107-test service suite and five service allocation-failure tests pass. The focused
fixture and allocation sweep pass under ASan/UBSan with leak detection disabled. Formatting,
static-analysis limitations, and diff evidence are recorded in the implementing change.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): each derived tablet uses the exact acquired
  leader/barrier authority.
- [Invariant 6](../architecture/invariants.md): SQL schema, committed placements, group bindings,
  Manifest tablets, projection, and result schema form one compatible snapshot.
- [Invariant 11](../architecture/invariants.md): the lifecycle owns the Manifest pin until terminal
  destruction.
- [Invariant 13](../architecture/invariants.md): every committed table tablet participates exactly
  once before global grouped finalization.
- [Invariant 15](../architecture/invariants.md): tablet count is bounded before plan allocation and
  all downstream limits remain explicit.
- [Invariant 18](../architecture/invariants.md): catalog/Manifest drift fails closed instead of
  silently serving a partial table.

## Migration and rollback

This is an additive pre-alpha in-memory constructor. Rollback removes it without changing the
generic grouped constructor, direct SQL lowerer, row-backed fallback, or any durable/wire bytes.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Manifest v2](../formats/manifest-v2.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
