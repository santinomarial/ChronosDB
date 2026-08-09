# ADR 0089: Exact logical materialized-view checkpoints

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query maintainers
- **Extends:** [ADR 0068](0068-live-handoff-and-resume-token-v1.md)
- **Extended by:** [ADR 0090](0090-materialized-view-checkpoint-v1.md)

## Context

The windowed materialized-view owner retained enough state for corrections and incremental updates,
but exposed no exact restart boundary. Recomputing only from currently visible rows can change
floating aggregate state and loses empty historical windows, revisions, emitted/finalized status,
watermark progress, and the precise committed source position.

## Accepted decision

`IncrementalAggregateCheckpoint` owns canonical row-identity-ordered contributions plus the exact
count, sum, weighted sum, weight sum, Welford mean, and M2 state. Restore rebuilds ordered extrema
and OHLC indexes from the rows while preserving those running numeric fields exactly. New aggregate
admission requires nonzero row/source order, finite value/weight/product, and rejects a numeric
transition that would overflow to non-finite state.

`WindowedMaterializedViewCheckpoint` binds the window definition, exact tablet/WAL source position,
watermark, current logical rows, and every ordered window's revision, emitted/finalized state, and
aggregate checkpoint. Restore requires declared bounds, canonical unique rows/windows, aligned
window coordinates, watermark-consistent finalization, and exact agreement between each window's
contributions and the rows selected by the window definition. A populated window cannot be omitted.

Checkpoint and restore remain single-owner operations. Restored views accept only the next
consecutive committed source position and produce the same subsequent logical changes as the live
owner.

## Consequences and alternatives

The checkpoint is an owned logical contract. ADR 0090 encodes that contract without exposing private
maps; a filesystem installation owner remains separate. The logical layer also makes process-local
handoff and focused continuation tests possible independently of I/O.

Rebuilding numeric fields from rows in an arbitrary canonical order was rejected because floating
addition and Welford updates are order-sensitive. Storing only aggregate output was rejected because
future corrections require exact row contributions and ordered OHLC endpoints. Storing only current
rows was rejected because finalized empty windows and output revisions remain observable state.

## Affected invariants and validation

Invariants 4, 8, 12, 13, 15, and 17 apply. Focused tests checkpoint correction/finalization state,
restore it, apply the same next tombstone to both owners, and require identical changes. Aggregate
tests require exact continuation and reject non-finite inputs and noncanonical row ordering. Durable
codec/storage corruption, crash, allocation-failure, floating edge, and recomputation campaigns
remain in the Phase 18 ledger.
