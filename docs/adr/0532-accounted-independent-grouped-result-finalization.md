# ADR 0532: Accounted independent grouped result finalization

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query maintainers
- **Extends:** [ADR 0531](0531-idempotent-grouped-shuffle-result-collector.md)

## Context

The all-remote result collector publishes complete partition-ordered Native batches, while the
existing grouped finalizer consumes query-accounted physical chunks. Decoding the collected bytes
outside either owner would lose a single memory authority and could let a merely similar plan or
schema finalize data returned by another query attempt.

## Decision

Add a move-only, single-thread-affine collected-result execution. Creation borrows the exact
shuffle authority and raw grouped result schema, owns all canonical collected streams in partition
order, and creates a separately bounded query resource context. Before publication it reconstructs
every result stream and checks query, reducer, coordinator, partition, frame count, encoded byte
extent, schema shape, and complete partition coverage.

Each pull decodes one Native batch, requires exact descriptors and a nonempty row set, computes a
checked retained-memory bound, reserves that bound, and canonical-copies values, offsets,
validity, Boolean bits, and selection into one `AccountedVectorChunk`. Corruption or exhaustion is
sticky; successful completion is sticky and reports partition, batch, row, and byte metrics.

Overload the existing grouped-shuffle finalizer for this execution. In addition to requiring the
same shuffle-authority object, independent-process finalization requires the execution to borrow
the exact raw-schema object owned by the fragment-derived finalization authority. The established
checked physical projection, global `ORDER BY`, `LIMIT`, output bounds, and atomic Native encoding
pipeline then runs unchanged. Similar-by-value schemas are deliberately insufficient authority.

The owner does not schedule TCP result clients or servers, retry timers, collector admission, or
query cancellation. Those lifecycle policies remain a later composition boundary.

## Consequences

Complete reducer results from independent processes can enter global SQL finalization without an
unaccounted decode boundary or a second finalization implementation. No output escapes before all
partition batches pass decoding and the final pipeline succeeds. The exact-schema identity rule
requires callers to construct result streams and collected execution from the finalization
authority's schema reference.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): raw result decoding and final SQL processing remain
  bound to one query, partition map, plan, and exact schema authority.
- [Invariant 11](../architecture/invariants.md): the execution owns streams and accounted chunks
  while borrowing authority and schema with documented lifetime requirements.
- [Invariant 15](../architecture/invariants.md): per-stream, per-batch, and total materialization
  memory are independently bounded before publication.
- [Invariant 18](../architecture/invariants.md): canonical partition order is retained while global
  ordering and limiting occur only after complete collection.

## Validation plan

Decode multiple partition streams into accounted chunks, preserve canonical partition order, and
exercise sticky schema, coverage, corruption, and memory failures. Derive two remote reducer
partitions from mutable fragment proof, reject a copied schema object, then globally sort by
aggregate value and limit to one row. Inject allocation failure through construction and batch
materialization. Run cluster, allocation-failure, sanitizer, formatting, static-analysis, and diff
gates.

## Migration or rollback considerations

No durable or wire format changes. Rollback removes independent-process global finalization; the
collector and result transport may remain but their output must not be exposed as a completed SQL
query.

## Unresolved questions

- Compose result retries, TCP client/server progress, collector admission, and finalization under
  one query deadline and cancellation owner.
- Add a hybrid collector for a coordinator that also owns a reducer partition.
- Qualify the composed lifecycle across independently scheduled database processes.

## References

- [Collector decision](0531-idempotent-grouped-shuffle-result-collector.md)
- [Proof-bound global finalization](0516-proof-bound-global-grouped-shuffle-finalization.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
