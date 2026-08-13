# ADR 0385: Bounded vector aggregate coordinator v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0374](0374-bounded-schema-bound-vector-result-coordinator-v2.md),
  [ADR 0384](0384-proof-revalidated-vector-aggregate-worker-v2.md)

## Context

Fragment-v2 workers could produce one complete schema-bound sufficient-state vector per tablet, but
no all-type owner arbitrated retries across those vectors or withheld global finalization until
every planned tablet closed. Reusing the row coordinator would treat partial state as native result
rows and prematurely finalize AVG, variance, and exact numeric SUM.

The older Float64 coordinator admits a different fragment and state contract. It neither owns the
all-type definition vector nor validates Distributed Vector Aggregate Exchange v1 bytes.

## Decision

`DistributedVectorAggregateCoordinatorV2` is a move-only, single-threaded state machine over one
query UUID, one unique nonempty plan-ordered tablet vector, one exact aggregate-definition vector,
and its result schema. Creation proves every definition's output shape against the matching result
descriptor and requires enough configured messages and minimum frame bytes for every tablet's
complete vector.

Every direct in-memory admission is canonically encoded before publication. Per-tablet sequences
must be contiguous from one; the exchange contract makes sequence `ordinal + 1` and only the last
aggregate terminal. A retained position is idempotent only when its complete canonical frame bytes
match. Conflicts are `ALREADY_EXISTS`, gaps are `UNAVAILABLE`, and output after terminal is invalid.
Retention is bounded by per-tablet and total message counts plus exact encoded frame bytes. An
allocation failure leaves the prior history unchanged. The first failure from an incomplete worker
remains authoritative, while a failure after that tablet's complete terminal is harmless.

Rvalue `finish` first requires one complete definition-width vector from every tablet. It creates a
fresh bounded query resource context, decodes the retained integrity-protected frames, and merges
each ordinal in plan-tablet order into one shared all-type state. Only after all merges succeed does
it call `take_result` once per state and transfer the schema and finalized values together. A failed
decode, merge, allocation, or result publication leaves the retained authority and bytes available
for retry. Fixed merged-state capacity and variable extrema have independent limits.

The coordinator does not own sockets, worker scheduling, grouped keys, SQL row encoding, or process
lifecycle. Ungrouped ORDER BY cannot change a one-row result; LIMIT and Native Protocol output are
applied by a subsequent global result owner.

## Consequences and validation

AVG, variance, and exact sums now cross tablets without losing sufficient state. Merge work is
O(tablet count × aggregate count); retained memory is bounded by exact frames, fixed configuration,
and query-accounted variable extrema. Floating-point results are deterministic for the pinned
plan-tablet order, though floating arithmetic is not mathematically associative. One owner thread
serializes state transitions, so no synchronization or memory-ordering argument applies.

Focused coverage proves out-of-order rejection, exact retry, conflicting retry, deterministic
cross-tablet AVG finalization, schema/identity/limit rejection, first-failure stability, and harmless
post-terminal worker loss. Allocation injection covers construction, rollback-safe retention, and
retryable decode/merge/final publication. Header self-containment and installed consumption cover
the public API.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Vector Aggregate Exchange v1](../formats/distributed-vector-aggregate-exchange-v1.md)
- [Mergeable Vector Aggregate State v1](../formats/mergeable-vector-aggregate-state-v1.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
