# ADR 0486: Replicated grouped sufficient-state preparation

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query, replicated-service, cluster, and Native protocol maintainers
- **Extends:** [ADR 0475](0475-owned-cross-tablet-grouped-vector-authority.md) and
  [ADR 0485](0485-atomic-grouped-native-tcp-publication.md)

## Context

The grouped sufficient-state scheduler could produce an atomic Native result, but its caller still
had to join read authority, committed metadata, a Manifest snapshot, projection/schema proof, and
authenticated routes. That gap made it possible for an embedding to assemble authority from
different publications or omit one of the lifecycle gates.

The packaged Native query plane currently reads mutable `TabletState`, while the sufficient-state
worker revalidates immutable Manifest/CSEG authority. Treating those snapshots as interchangeable
would violate the stable-snapshot contract.

## Decision

The replicated query service now exposes leader-linearizable and bounded-stale grouped-v2
constructors. Each constructor accepts one grouped `DistributedVectorQueryPlan`, an owning
`TemporalDatabaseStorageSnapshot`, one owned result schema, and one bounded configuration. It:

1. validates the requested consistency and grouped plan mode;
2. acquires metadata barrier coverage without permitting metadata/tablet group aliasing;
3. binds every tablet against one committed catalog, the exact group authority, destination
   projection, event-time predicate, Manifest generation, and result schema;
4. resolves only the resulting serving nodes through the same catalog and borrowed TLS contexts;
5. creates the portable all-tablet grouped execution and the finite TCP scheduler; and
6. transfers sender, carrier, deadline, finalization, authentication, and route policy into the
   scheduler that publishes only a complete Native result.

The follower constructor first proves metadata coverage and then uses only a canonical correlated
leader/follower authority vector for tablet admission. Observation transport remains caller-owned,
matching the existing replicated aggregate API.

This decision does not route mutable Native SQL into the Manifest worker. That requires a distinct
proof-revalidated sufficient-state worker over `TabletState` or a deliberate query-plane storage
unification; until then, mutable grouped SQL retains its bounded row-backed coordinator path.

## Consequences

Manifest-backed embeddings no longer reconstruct grouped authority or route joins, and cannot
start network acquisition before every plan/schema/snapshot/finalization bound is accepted. The
returned move-only owner retains the Manifest pin and complete diagnostic authority through
success or terminal failure.

The constructors add no new wire or durable bytes and no shared-memory publication. They are
single-thread-affine compositions, so no new memory-ordering argument applies. Computed pre-group
expressions, final projection splitting, mutable Native worker integration, and shuffle routing
remain separate work.

## Validation

The replicated service fixture constructs both authority modes from a real selected Manifest. It
proves the exact tablet group, serving node, Manifest generation, nullable FLOAT64 key definition,
COUNT(*) definition, and running grouped TCP lifecycle before I/O. The existing grouped scheduler
suite remains the end-to-end mutual-TLS and atomic Native publication evidence.

The complete build and all 107 service tests pass; the four service allocation-failure tests pass;
and the focused replicated fixture passes under ASan/UBSan with leak detection disabled. The three
changed C++ files pass LLVM 18 formatting and the diff passes whitespace validation. LLVM 18 static
analysis reaches both changed translation units with no project-local finding, then fails because
the installed macOS 26 libc++ requires compiler builtins unavailable to LLVM 18. The repository-wide
format script also reports a pre-existing formatting violation in the unchanged grouped-TLS header
self-containment test; the changed files themselves pass the exact formatter check.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only the proved leader barrier or correlated
  bounded-stale follower position may serve a tablet.
- [Invariant 6](../architecture/invariants.md): catalog, plan, result schema, and every tablet share
  one compatible Manifest snapshot.
- [Invariant 11](../architecture/invariants.md): the returned owner retains the Manifest pin through
  execution and finalization.
- [Invariant 14](../architecture/invariants.md): routing reuses the versioned Fragment-v2 and grouped
  response-v2 formats unchanged.
- [Invariant 15](../architecture/invariants.md): binding, routes, workers, transport, merge, and
  Native output all retain explicit finite limits.
- [Invariant 18](../architecture/invariants.md): mutable and immutable authority models are not
  silently conflated to obtain a faster path.

## Migration and rollback

These are additive pre-alpha in-memory APIs. Rollback removes the two constructors and their
configuration without changing formats or the row-backed Native grouped path.

## Retrospective note (2026-08-25)

[ADR 0490](0490-proof-revalidated-mutable-grouped-sufficient-state-worker.md) implements the
separate `TabletState` grouped worker and request-local service adapter anticipated here. A distinct
mutable grouped carrier and scheduler composition are still required before the packaged Native
query plane can select that worker.

## References

- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
