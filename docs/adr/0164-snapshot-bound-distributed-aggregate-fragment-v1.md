# ADR 0164: Snapshot-bound distributed aggregate fragment v1

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, networking, and distributed-systems maintainers
- **Extends:** [ADR 0163](0163-bounded-distributed-fragment-sequencing.md)

## Context

The response exchange has canonical bytes and retry semantics, but workers still receive no
canonical request. Serializing the entire physical pipeline now would require stable bytes for all
expressions, types, grouping, sorting, latest, and output stages. The implemented distributed path
currently needs a narrower executable unit: one tablet scan, projection, event-time predicate, and
one mergeable Float64 aggregate input.

## Decision

Distributed Aggregate Fragment v1 is a variable-length checksummed request with a 216-byte header,
one `1..4096` vector of unique destination-schema ordinals, and a four-byte complete-frame trailer.
It binds query/database/table/tablet/schema identity, aggregate snapshot generation, serving node,
placement epoch, applied and observed positions, exact consistency policy and proof, projection,
aggregate input index, and optional event-time bounds.

The header CRC protects every length, count, identity, route, proof, flag, and predicate field
before any of them controls interpretation. Exact total length is derived from the authenticated
projection count. Complete CRC is checked before projection allocation or interpretation. Reserved
bytes and absent optional numeric fields are zero, and unknown versions or flags fail closed.

The codec validates structural and read-proof relationships. A later worker executor must still
bind schema/type, local snapshot generation, tablet/table identity, local applied position, current
placement epoch, and serving-node ownership before touching storage. CRC32C is not authentication.
Because indexes are group-scoped and tablet UUID is not group UUID, the accepted
[Dispatch v1 envelope](../formats/distributed-aggregate-fragment-dispatch-v1.md) supplies the
required group binding without changing these bytes. A bare fragment is not executable.

## Consequences and validation

This supplies canonical scan/projection/event-filter request bytes for the current aggregate use
without pretending arbitrary physical plans are portable. The maximum frame is 16,604 bytes and
decoding makes one bounded projection allocation after both integrity gates. Encoding owns its
bytes; decoding returns owned ordinals and values.

Golden tests freeze every header region and both checksums. Focused tests cover exact round trip,
truncation/trailing data, header and body corruption, checksum-valid unknown versions, duplicate
ordinals, invalid aggregate indexes/routes/read proofs, and caller projection limits. Full query,
focused sanitizer, and installed-consumer checks cover integration.

General expression/stage bytes, multi-key/non-FLOAT64 grouping, ordering/top-N, worker execution,
socket dispatch, cancellation, and durable query recovery remain separate work. The first distinct
single-key grouped intent is the accepted follow-up in
[ADR 0325](0325-distinct-grouped-float64-fragment-intent.md).

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## Migration and rollback

No earlier worker-fragment byte format exists. Exact version 1.0 is required. Rollback can disable
remote fragment dispatch but must not reinterpret these bytes as client protocol or exchange-result
frames.

## References

- [Distributed Aggregate Fragment v1](../formats/distributed-aggregate-fragment-v1.md)
- [Distributed Aggregate Exchange v1](../formats/distributed-aggregate-exchange-v1.md)
