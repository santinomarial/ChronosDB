# ADR 0537: Grouped shuffle reducer-job control envelope

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0536](0536-portable-grouped-shuffle-authority.md)

## Context

Portable authority bytes let another process reconstruct shuffle proof, but do not tell a reducer
which daemon owns the job, where completed partitions must return, which raw schema binds those
bytes, or how long the admitted job may remain live. Reconstructing those facts from process-local
fixtures would preserve the test boundary rather than create a production control product.

## Decision

Add the checksummed `CHDVGJC1` request envelope with canonical `PREPARE` and `SEAL` actions.
PREPARE nests one complete `CHDVGSA1` authority and one complete `CHDVRSC1` raw schema, binds the
claimed coordinator and target reducer node IDs, carries one numeric IPv4 result endpoint,
and supplies a bounded relative execution timeout. Decode validates the outer header before
trusting lengths, independently checks both nested frames, reconstructs their owned values, proves
the target is an authority destination, and exact-matches raw key and aggregate output shapes.

SEAL contains only the query/coordinator/target identity and requires every deployment field,
nested length, and nested checksum to be zero. The future job service must match it to an already
admitted PREPARE; the codec does not maintain process state.

The relative timeout begins after successful admission. Transport delivery and acknowledgment
still require their own deadline, avoiding either serialized `steady_clock` epochs or a hidden wall-
clock synchronization assumption.

Add the fixed checksummed `CHDVGJR1` response. It echoes the complete request identity and stable
status code. Only successful PREPARE may publish the reducer's live shuffle-listener endpoint; all
failure responses and every SEAL response require canonical endpoint absence. This makes listener
publication an explicit acknowledged boundary without claiming that the codec authenticates or
drives a connection.

## Consequences

A reducer daemon can now receive every immutable proof and deployment fact required to configure a
result-return job without sharing coordinator memory, and can return one exactly correlated
admission result. Existing authority and schema formats remain
independently reusable and versioned. Exact object identity is re-established after decode by
owning both values together in the PREPARE variant.

Encode and decode allocate only owned frame/nested values and classify injected allocation failure
as resource exhaustion. SEAL is compact and canonical. Idempotency, admission limits, progress,
cancellation, cleanup, and acknowledgment remain explicit service work outside the codec; ADR 0538
implements that process-local owner.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): query proof, raw schema, reducer identity,
  coordinator identity, result route, and execution budget cross the process boundary together.
- [Invariant 10](../architecture/invariants.md): outer header/whole-frame and independent nested
  checksums protect every allocation- and interpretation-driving field.
- [Invariant 14](../architecture/invariants.md): control requests have an explicit 1.0 version and
  strict compatibility policy.
- [Invariant 15](../architecture/invariants.md): outer and nested bytes, counts, names, and execution
  time are bounded before job publication.
- [Invariant 18](../architecture/invariants.md): raw grouped shape and destination ownership are
  revalidated against the decoded authority before PREPARE becomes observable.

## Validation plan

Round-trip a complete multi-source authority PREPARE and canonical SEAL. Reject frame damage,
unknown versions, noncanonical SEAL fields, mismatched raw schemas, and a lower caller timeout.
Round-trip successful PREPARE, failed PREPARE, and successful SEAL responses; reject response
damage and illegal endpoint publication. Sweep allocation failure through nested request and fixed
response encoding plus complete request decode. Run cluster, allocation-failure, sanitizer,
formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No existing durable or network bytes change. Rollback removes an as-yet unadvertised control
request format; reducer daemons must then reject remote job setup rather than infer missing fields.

## Unresolved questions

- Add bounded header-first request/response ownership and a mutual-TLS carrier.
- Dispatch the control protocol from the shared daemon query-control endpoint.

[ADR 0538](0538-bounded-grouped-shuffle-reducer-job-service.md) now owns finite job admission,
idempotent PREPARE/SEAL, ingress progress, cancellation, timeout cleanup, and result scheduling.

## References

- [Job-control format](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v1.md)
- [Portable authority](0536-portable-grouped-shuffle-authority.md)
- [Coordinator result lifecycle](0534-atomic-grouped-result-coordinator-lifecycle.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
