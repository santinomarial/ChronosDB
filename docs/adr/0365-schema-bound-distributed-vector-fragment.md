# ADR 0365: Schema-bound distributed vector fragment

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0354](0354-authority-bound-distributed-vector-fragment.md),
  [ADR 0364](0364-schema-light-distributed-vector-results.md)

## Context

The result descriptor contract existed independently of the proof-bound fragment. A worker could
not know which caller-owned output names/type/nullability it was authorized to emit. Adding fields
to Fragment v1 would change accepted bytes.

## Decision

Fragment v2 is a distinct checksummed wrapper around one unchanged exact Fragment v1 plus one exact
Distributed Vector Result Schema v1. Independent payload CRCs and a complete wrapper CRC preserve
nested integrity and protocol separation.

`bind_distributed_vector_fragment_v2` first reuses every v1 admission, placement, Manifest, schema,
projection, proof, and aggregate-type check. It derives projected physical shapes from the same
committed schema, validates the supplied result descriptors against the bound plan, and only then
publishes owned dispatch and schema values. Names remain exact caller identities.

## Consequences and validation

The focused fragment case round-trips v2, proves v1/v2 confusion rejection, and rejects truncation,
lower frame bounds, and nested schema damage after recomputed outer checksums. The focused authority
case accepts exact grouped key/SUM descriptors and rejects nullability mismatch. Header
self-containment and installed consumption cover the API.

Partial-I/O for the larger v2 wrapper, compatible multi-tablet v2 ownership, cluster request bytes,
schema-light result batches, execution, authenticated lifecycle, and process integration remain.
No Phase 16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, and 18 apply.

## References

- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
- [Distributed Vector Result Schema v1](../formats/distributed-vector-result-schema-v1.md)
