# ADR 0379: Bounded global vector row finalization v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and native-protocol maintainers
- **Extends:** [ADR 0352](0352-canonical-distributed-vector-plan-intent.md),
  [ADR 0377](0377-pinned-schema-bound-vector-query-v2-execution-owner.md)

## Context

Row workers deliberately emitted every matching row in tablet-local source order, and the pinned
execution owner withheld results until all tablet streams closed. The attached plan's `ORDER BY`
and `LIMIT` still had no coordinator-side executor. Applying them at workers would lose global
rows, while concatenating native batches would return the wrong order or limit.

## Decision

`finalize_distributed_vector_rows_v2` consumes one completed schema-bound execution result and
accepts row mode only. Before decoded row-state allocation it revalidates plan/schema width,
nonempty single-query tablet segments, contiguous sequences, terminal closure, unique tablet
segments, native batch header shapes, full exchange-byte accounting, and configured row/message/
working-memory bounds. Exact native decoding then independently reproves every descriptor and cell.

Rows are referenced by batch and row ordinal; cells remain borrowed from the consumed result during
the synchronous call. An allocation-free canonical-byte comparator implements every current scalar
type, SQL NULL placement, numeric order, NaN-after-infinity order, and unsigned byte order for text,
symbol, binary, and UUID values. A stable merge sort applies final order keys. Equal keys retain the
coordinator's deterministic plan-tablet/message/row order. Direction reverses only non-NULL values,
so explicit NULL placement remains independent. `LIMIT`, including present zero, applies only after
the complete global order.

The finalizer plans output batches exactly before encoding. It respects Protocol v1's row and
16-MiB payload ceilings, configured batch/count/total-byte limits, and repeats the admitted schema
descriptors in every native payload. A zero-row result owns exactly one schema-bearing payload. It
adds no wire or durable format.

## Consequences and validation

Input exchange bytes, additional working memory, input rows/messages, output payload bytes, and
output batch count have independent configured and hard ceilings. Decoded-cell and row-reference
state is conservatively charged before allocation. The blocking work is `O(cells + rows log rows *
order keys)`; no-order execution remains linear. The function has one caller thread and publishes
only a value-owned terminal result, so no inter-thread memory-ordering argument applies.

Focused cases merge two tablet streams, order descending INT64 plus ascending nullable STRING,
apply a global limit, and split the exact result across bounded native batches. They also prove
stable plan-order ties, a schema-bearing zero-limit result, malformed sequence/closure/duplicate
tablet/schema rejection, row/working-memory exhaustion, aggregate-mode refusal, all-type canonical
comparison behavior, and classification of every injected owned-allocation failure. Header
self-containment, installed-consumer coverage, sanitizers, formatting, focused static analysis, and
the full serialized suite are required before completion.

All-type aggregate merge-state transport/finalization, authority rebinding, and process integration
remain separate tasks. No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Canonical distributed vector plan intent](0352-canonical-distributed-vector-plan-intent.md)
- [Pinned schema-bound vector query v2 execution owner](0377-pinned-schema-bound-vector-query-v2-execution-owner.md)
- [Proof-revalidated schema-bound vector row worker v2](0375-proof-revalidated-schema-bound-vector-row-worker-v2.md)
- [Native Protocol v1](../protocol/native-v1.md)
