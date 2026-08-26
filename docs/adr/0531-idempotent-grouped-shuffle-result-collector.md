# ADR 0531: Idempotent grouped shuffle result collector

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query maintainers
- **Extends:** [ADR 0530](0530-bounded-grouped-shuffle-result-tcp-server.md)

## Context

A result receipt may be lost after the coordinator server retains a partition. The reducer then
resends the exact immutable stream, so the server can expose multiple individually valid copies.
The coordinator needs one authority-bound owner that suppresses exact retries, rejects conflicts,
and withholds global input until every partition is present.

## Decision

Add a move-only, single-thread-affine collector for an all-remote result topology. Creation pins the
exact shuffle authority, raw result schema, coordinator node, per-stream limits, and total retained
byte limit. It requires the coordinator not to be an authority reducer, making complete remote
coverage possible, preallocates one optional slot per canonical partition, and owns no network or
clock state.

Every candidate must match the authority query, coordinator target, partition, and authority-
derived reducer source. Before state mutation, the collector reconstructs the canonical result
sender from the retained batches and requires exact frame-count and encoded-byte extent. The first
valid stream for a partition consumes total-byte capacity and moves into its preallocated slot. A
subsequent byte-for-byte identical stream succeeds and increments a saturating duplicate metric; a
different valid stream for that partition returns `ALREADY_EXISTS` without replacing the original.

The collector becomes complete only when every partition slot is populated. One-shot publication
allocates and reserves the complete output vector before moving any partition, then returns streams
in partition-ID order and enters a taken state. Failed publication leaves every slot intact for
retry. Metrics expose total partitions, unique accepted partitions, duplicate streams, and current
retained encoded bytes. Hybrid local/remote collection, TCP polling, decoding into global physical
operators, and query lifecycle scheduling remain separate.

## Consequences

Lost result receipts no longer duplicate global grouped output, and conflicting retries cannot
silently overwrite accepted data. Global finalization receives either complete canonical partition
coverage or nothing.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): query, raw schema, partition, reducer, coordinator,
  frames, bytes, and batch payloads are one immutable identity.
- [Invariant 11](../architecture/invariants.md): the collector exclusively owns preallocated slots
  and retained result streams while borrowing authority and schema.
- [Invariant 15](../architecture/invariants.md): partition count, per-stream frames and bytes, total
  retained bytes, and publication storage are bounded.
- [Invariant 18](../architecture/invariants.md): exact retries are idempotent and canonical
  partition order is preserved without changing grouped semantics.

## Validation plan

Admit partitions in reverse order, accept an exact retry, reject a conflicting retry, and publish
once in canonical partition order. Reject wrong coordinator, noncanonical extent, coordinator-as-
reducer topology, and total-byte exhaustion without partial admission. Inject allocation failure
through construction, canonical validation, and atomic publication. Run cluster,
allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No durable or wire format changes. Rollback requires treating every retained TCP result as unique;
that is unsafe when a receipt can be lost, so independent-process result return must then remain
disabled.

## Unresolved questions

- Decode canonical partition results into the global grouped physical pipeline.
- Schedule result retries, TCP attempts, server drains, and collection under one deadline.
- Add a hybrid collector for a coordinator that also owns a reducer partition.

## References

- [Result server decision](0530-bounded-grouped-shuffle-result-tcp-server.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
