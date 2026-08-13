# ADR 0167: Proof-revalidated distributed aggregate worker

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0166](0166-authority-bound-distributed-fragment-construction.md)

## Context

Coordinator-side binding prevents authority mixing at construction time, but a request can arrive
after local placement, leadership, schema, or storage publication changes. Executing solely from
the encoded fields would let stale routing touch unrelated local data. Manifest v2 parts also carry
temporal versions, so summing physical rows directly would count superseded or tombstoned values.

## Decision

`execute_distributed_aggregate_fragment` validates the complete nested frame value before I/O and
then exact-matches local node, Raft group, committed placement epoch/membership, database/generation,
tablet source, durable applied boundary, and current destination schema. Leader-linearizable work
also requires the caller's locally acquired Raft read barrier to equal the dispatched proof. A
conflicting placement leader hint rejects the request but is never treated as consensus proof.

Only after those gates does the worker load exact generation-pinned parts through
`ManifestStorage::load_temporal_part_images`, which revalidates lengths, hashes, formats, source, and
schema lineage. The existing Manifest v2 temporal resolver selects current logical winners and
removes tombstones before the worker applies the event-time predicate and accumulates the selected
Float64 column into one terminal sequence-1 Welford exchange state. Null aggregate values are
skipped. An empty durable tablet produces canonical empty state.

## Consequences and validation

The first executable path is intentionally specialized to an ungrouped Float64 partial aggregate;
it does not claim general physical-plan serialization. It decodes complete temporal rows through
the existing scalar winner resolver, bounded by the supplied part/reader/resolution limits. A later
vector-native resolver may optimize this only with profiling evidence and semantic equivalence
tests.

Focused tests install a real Raft-sourced temporal CSEG in Manifest v2, pin its publication, filter
one of two visible rows, and verify the exact aggregate state. They also reject a different local
node, placement epoch, and local barrier. Full query, sanitizer, and installed-consumer checks cover
integration.

The barrier argument is proof acquired from the local Raft runtime; the value type alone does not
create leadership. Carrier authentication, cancellation/deadlines, retry response bytes, and
coordinator transport integration remain separate boundaries.

The first grouped executor reuses these complete local gates and temporal winner resolution under
[ADR 0328](0328-proof-revalidated-grouped-float64-worker.md); it does not bypass or replace the
ungrouped worker.

Invariants 4–6, 10, 11, 14, and 18 apply.

## Migration and rollback

Executable dispatch receivers must call this worker boundary rather than directly opening parts.
Rollback may reject remote work, but must not bypass local revalidation or aggregate raw versions.
