# ADR 0291: Stable local-applied replicated query snapshot

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, query, metadata, Raft, and ingest maintainers

## Context

The replicated database already publishes immutable metadata projections and immutable tablet
snapshots from worker-affine Raft application state. Querying the mutable state machines directly
would violate worker ownership. Binding SQL against one live catalog and acquiring data later could
also mix an unrelated schema projection with tablet publications. Finally, executing only the
resident subset of a globally partitioned table would return a plausible but incorrect result.

## Decision

`ReplicatedIngestDatabase::acquire_query_snapshot` acquires one owning committed metadata catalog,
reconstructs each active retained schema lineage, and pins every resident tablet publication named
by that catalog. `ReplicatedQuerySnapshot` owns the binder catalog, lineages, and tablet pins, so the
view and any physical pipeline instantiated from it remain valid after later application and after
the database owner shuts down.

The read contract is **local applied**. Every tablet boundary contains only entries already
committed and applied by its Raft group. Acquisition does not run a quorum read barrier and does not
claim one linearizable instant across independent groups; it records a stable vector of per-tablet
applied publications. A tablet whose publication has advanced to a schema absent from the pinned
metadata lineage is rejected instead of mixed into the view. An older retained tablet schema may be
projected to the pinned active schema by the existing checked vector source.

The catalog remains available for SQL binding when a table has remote placements, but physical
instantiation requires every committed placement to have a resident group and a pinned matching
tablet. Partial residency and tables without a placement return `UNAVAILABLE`. The existing
`instantiate_tablet_states_pipeline` then places all sealed and active generations below one global
physical pipeline, preserving aggregate, sort, latest, and limit semantics.

## Consequences and validation

Replicated current-state SELECT now has an ownership-safe database boundary without exposing
uncommitted entries or silently truncating distributed tables. Native protocol dispatch, quorum
read barriers, remote fragment execution, historical reads, and subscription handoff remain
separate integration work. No durable or network format changes.

Focused recovery tests apply a QUORUM_SYNC append, reopen the database, pin a whole-table view,
destroy the database owner, and execute an exact vector count from the retained pins. A second test
provisions one resident and one remote tablet and verifies execution fails with `UNAVAILABLE` and
leaks no query credit. A two-resident-group recovery test preserves each independent retry identity
and executes one four-row global count after owner shutdown. Broader schema-change races,
high-cardinality group scheduling, allocation fault injection, sanitizers, and linearizable-read
work remain in the hardening ledger.

## Affected invariants

Invariants 4–6, 8, 11, 14, and 18 apply.

## References

- [ADR 0217](0217-vectorized-tablet-state-query-source.md)
- [ADR 0278](0278-worker-affine-metadata-application.md)
- [ADR 0284](0284-committed-metadata-replicated-database-recovery.md)
- [Architecture invariants](../architecture/invariants.md)
