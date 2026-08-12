# ADR 0236: Committed Append Subscription Result Changes

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB ingest, query, live-query, and protocol maintainers

## Context

The live coordinator advances each source by exact committed WAL record sequence, while one
Columnar Append record can contain many rows. Publishing one change per selected row would reuse a
source position and violate resume continuity. Publishing the unfiltered append would make clients
reimplement the bound SQL plan. The already-applied immutable batch can now enter the vector engine,
but no owner converted its complete output into one coordinator-compatible logical change.

Running aggregate, sort, LATEST, or LIMIT independently over each append is also not equivalent to
running the historical plan over the committed table. Those stages require incremental retained
state or a precisely accepted live semantic.

## Decision

`evaluate_committed_batch` accepts one prepared subscription plan, complete source position,
already-applied immutable batch, and query resource context. It admits only stateless,
cardinality-nonincreasing stages: Boolean and timestamp filters, column subsets, source-column
output, and computed/constant column output. Stateful stages fail with `NOT_SUPPORTED` before any
subscription state changes.

The evaluator executes every bounded vector chunk and emits exactly one UPSERT `CommittedChange`
for the WAL record. Its payload is one self-describing native `QUERY_RESULT` batch containing all
selected rows in source order. Zero selected rows still produce an empty schema-bearing payload so
the source position can advance without inventing a data row.

The 80-byte result key is canonical:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 8 | `CHLBRK1\0` |
| 8 | 32 | prepared plan fingerprint |
| 40 | 16 | tablet UUID |
| 56 | 16 | WAL ID |
| 72 | 8 | committed record sequence, little-endian |

Output chunks remain query-accounted until encoding completes. Chunk owners, cell views,
descriptors, result payload, and key receive checked workspace reservations. The encoded nested
result is bounded by both `QUERY_RESULT` limits and the remaining Protocol 1.1
`SUBSCRIPTION_CHANGE` envelope capacity.

## Consequences

A successful result can enter the existing single- or multi-tablet manager without changing its
gap-free sequence contract, and the existing delivery encoder can carry its payload unchanged.
Evaluation itself is side-effect free: callers publish only after successful committed application
and evaluation.

The result key identifies the plan-bound append result, not an application primary key. This slice
therefore models append-derived row-preserving changes only. Corrections, DELETE generation,
incremental aggregate/window state, evaluator fan-out to multiple plans, and ingest/daemon wiring
remain separate work.

## Validation

Focused tests force a two-chunk input, verify filtered projection and zero-row payloads, prove
repeatable and position-distinct keys, publish through the gap-free manager, encode the complete
Protocol 1.1 delivery, and reject aggregate and resource-failure paths without publication.

## References

- [ADR 0068](0068-live-handoff-and-resume-token-v1.md)
- [ADR 0094](0094-native-protocol-1-1-subscriptions.md)
- [ADR 0097](0097-schema-bound-subscription-plan-identity.md)
- [ADR 0235](0235-query-accounted-columnar-batch-source.md)
- [Committed append subscription evaluation](../learning/committed-append-subscription-evaluation.md)
