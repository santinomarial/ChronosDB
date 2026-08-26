# ADR 0524: Proof-bound grouped shuffle result frame

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0515](0515-exclusive-authority-ordered-grouped-shuffle-result-gathering.md),
  [ADR 0516](0516-proof-bound-global-grouped-shuffle-finalization.md)

## Context

Grouped destination reducers could be gathered only as in-process owners. Returning their disjoint
partition outputs from another process first requires a stable product that cannot confuse a query,
partition, destination node, coordinator, hash authority, or raw grouped schema. Reusing a tablet
result frame would invent a tablet identity after all tablet states had already merged.

## Decision

Add the versioned `CHDVGRR1` result frame. Each frame binds a query, authority destination source,
explicit coordinator target, partition ID/count, hash version, contiguous nonzero sequence, and
terminal flag to either one canonical nonempty Native `QUERY_RESULT` batch or an empty terminal.
The Native descriptors must exactly equal the proof-bound raw grouped result schema. A canonical
schema encoding checksum in the fixed header detects drift before payload decode, while exact
descriptor comparison remains authoritative.

The codec validates header and frame CRC32C, payload CRC32C, version, reserved bytes, authority,
schema identity, lengths, and Native cells. One header-first reader owns a bounded exact frame,
preserves coalesced successor bytes, and publishes only complete values. One move-only cursor owns
partial-write progress. Unknown versions are unsupported; damaged wire input is corruption; typed
caller mistakes are invalid arguments; limit and allocation failures are resource exhaustion.

This decision does not claim peer authentication or a complete partition stream. The next layer
must bind certificate principals to the exact nodes, require contiguous sequence and one terminal
per partition, withhold partial responses, and define retry and cancellation.

## Consequences

Remote reducer output now has a checksummed, schema- and authority-bound wire product without fake
tablet identity. The existing reducer-to-global-finalizer boundary can later consume exact decoded
Native batches. Mutual-TLS sessions, complete stream owners, and independent-process lifecycle
composition remain open.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): query, partition, destination, coordinator, hash,
  and raw schema belong to one immutable authority.
- [Invariant 10](../architecture/invariants.md): fixed integrity and allocation-driving bounds pass
  before frame allocation; payload integrity passes before Native decode.
- [Invariant 14](../architecture/invariants.md): remote partition results use an explicit versioned
  format rather than inferred in-memory layout.
- [Invariant 15](../architecture/invariants.md): frame, payload, row, column, and name sizes are
  bounded before retention.
- [Invariant 18](../architecture/invariants.md): exact raw grouped descriptors and canonical cells
  preserve the in-process finalization input shape.

## Validation plan

Round-trip a nonempty two-column grouped batch and the canonical empty terminal. Reject source,
coordinator, schema, checksum, version, and exact-length drift. Exercise every split point,
coalesced successors, short writes, over-advance, and deterministic allocation failure through
encode, exact decode, and header-first retention. Run cluster, allocation-failure, sanitizer,
formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No existing durable or wire bytes change. Rollback removes only the new unused result-return
product. A future session must negotiate or fix version 1 explicitly before sending it.

## Unresolved questions

- Define authenticated request and complete response-stream owners.
- Bind result retry/deduplication and destination sealing to coordinator receipt proof.
- Rehydrate remote Native batches into one bounded global query resource context.

## References

- [Result format](../formats/distributed-vector-grouped-aggregate-shuffle-result-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
