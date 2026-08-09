# ADR 0081: CSEG v2 temporal snapshot resolution

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB temporal-query and storage maintainers

## Context

CSEG v2 projected reads expose physical temporal histories, but SQL current and `FOR SYSTEM_TIME AS
OF` require one visible winner per logical identity. The v2 format permits WAL and Raft source
identities; comparing unrelated sources without an application snapshot would invent an order.

## Accepted decision

Resolution requires an exact schema, complete schema-order user projection, one explicit
authoritative `(commit_source, source_id)` lineage, an optional inclusive system-time boundary, and
hard version/output/identity limits. Every row must match that lineage. Rows later than the boundary
are ignored. For each logical identity, greater system commit time wins and greater commit position
breaks a timestamp tie. Duplicate physical positions for one identity are corruption. A winning
tombstone emits no row.

The result is an owned `ScalarTableSnapshot`. Its committed position is the greatest authoritative
position visible at the requested time, including tombstones, and zero when no version is visible.
Physical source identity is retained in the existing scalar row-version fields. The resolver does
not merge independent tablet lineages, discover Manifest state, or infer Raft authority.

## Consequences and alternatives

The bounded vector baseline has linear winner lookup and can therefore be quadratic in distinct
identities. This is accepted as a correctness reference and differential oracle; a hash or sorted
vector implementation requires benchmark evidence and identical resource/failure behavior.

Using scan arrival order was rejected because part order and compaction would change results.
Ordering all WAL and Raft UUIDs lexicographically was rejected because UUID order has no commit
meaning. Applying CSEG versions into the mutable scalar provider was rejected for reads because it
would duplicate state, impose original-before-correction ingestion ordering, and hide immutable
snapshot provenance.

## Affected invariants and validation

Invariants 4–8, 11, 13, 14, and 18 apply. Focused tests compare current and as-of outcomes with the
existing scalar model, cover corrections and tombstones, retain the visible commit boundary, reject
foreign lineages, and enforce version limits. Multi-part generated histories, schema evolution,
active Manifest snapshot integration, allocation failure, fuzzing, compaction equivalence, and
performance evidence remain required follow-up validation.
