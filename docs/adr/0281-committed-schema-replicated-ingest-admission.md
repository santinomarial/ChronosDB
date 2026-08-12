# ADR 0281: Committed-schema replicated-ingest admission

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, schema, metadata, and ingest maintainers

## Context

ADR 0280 derives a replicated write's group and leader term from committed authority, but a
canonical COLUMNAR_APPEND also names a table, schema identity, schema version, and complete batch
shape. Routing one whose schema is absent, inactive, or incompatible would defer a known catalog
failure until committed application and could append a command that the tablet state machine must
reject.

Complete schema definitions and active-schema identity are already published in the immutable
metadata catalog. Replicated admission must use them with the same authority as placement.

## Decision

Before enqueuing a group observation, `ReplicatedIngestCoordinator` exact-matches the command table
to one committed active-schema record, finds its complete immutable definition, and exact-matches
the definition's table, schema identity, and version. It then runs the canonical columnar-append
schema validator against the command's decoded batch.

The pending route retains table, tablet, schema identity, and schema version. After the ordered
group observation completes and before proposal, the coordinator reacquires the latest metadata
snapshot and repeats the active-definition identity checks alongside placement and group binding.
Schema definitions are immutable, so exact identity/version revalidation is sufficient after the
initial full shape validation. If schema authority advanced while the observation was in flight,
the old-schema request fails without appending to the tablet log.

Missing active schema is temporarily unavailable. A command that names a committed but inactive
schema is invalid. An internally inconsistent published definition is corruption. No fallback to a
retained tablet schema can grant admission because the metadata group is the catalog authority.

## Consequences and validation

Replicated ingest now rejects catalog-incomplete, stale-schema, and shape-incompatible commands
before durable data mutation. A concurrent schema activation cannot race between routing and
proposal.

No durable or network bytes change. Focused tests cover missing active authority, an inactive
predecessor, and activation after observation admission; the last case proves the tablet log remains
empty. Multi-node schema rollout timing, allocation faults, TSan, and load measurement remain
hardening work.

## Affected invariants

Invariants 4–6, 8–10, 14, and 18 apply.

## References

- [ADR 0214](0214-durable-complete-schema-definitions.md)
- [ADR 0278](0278-worker-affine-metadata-application.md)
- [ADR 0280](0280-authoritative-replicated-ingest-routing.md)
