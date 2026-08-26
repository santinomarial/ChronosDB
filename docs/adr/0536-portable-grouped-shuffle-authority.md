# ADR 0536: Portable grouped shuffle authority

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0535](0535-independent-grouped-result-process-qualification.md)

## Context

The process gate independently reconstructed authority from deterministic fixtures. A production
coordinator cannot ask a reducer daemon to accept source streams or return a partition result by
assuming that the remote process has the same fragments in memory. The complete query, source,
destination, key, aggregate, and hash proof needs an explicit versioned product.

## Decision

Add the checksummed `CHDVGSA1` authority format and a move-only encoded owner. Encoding serializes
the existing immutable authority's plan-order sources, canonical destinations, typed keys, and
typed aggregate inputs without dumping native structs. Exact decoding validates the fixed header
checksum, version, total/body lengths, hard and caller counts, reserved bytes, descriptor types,
canonical absent inputs, and final frame checksum before reconstructing authority through the
existing public validator.

The format uses one-based stable aggregate operation codes and explicit logical-type parameters.
It carries no result schema, coordinator route, deadline, credentials, or resource policy; those
belong to a future reducer-job envelope so authority bytes remain reusable proof rather than
deployment configuration.

## Consequences

A reducer process can now reconstruct the same complete shuffle proof from bounded wire bytes
instead of fixture or catalog coincidence. Exact object identity remains process-local; subsequent
owners borrow the one decoded authority object for their complete lifetime.

The codec allocates the final encoded frame once. Decode verifies all size-driving fields before
reserving vectors and classifies authority-construction exhaustion without relabeling it as wire
corruption.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): every source, destination, key, aggregate, query,
  and hash field needed for stable shuffle authority crosses the process boundary explicitly.
- [Invariant 10](../architecture/invariants.md): header and whole-frame CRC32C cover interpretation
  fields and descriptors before they become authority.
- [Invariant 14](../architecture/invariants.md): authority uses an explicit 1.0 format and rejection
  policy.
- [Invariant 15](../architecture/invariants.md): frame, source, partition, key, aggregate, and
  retained authority memory are bounded before publication.
- [Invariant 18](../architecture/invariants.md): portability does not change canonical source order,
  partition identity, typed grouped semantics, or hash version.

## Validation plan

Round-trip multi-source, multi-destination, multi-key authority with input-free and typed nullable
aggregates. Reject frame damage, a checksummed unknown version, nonzero reserved bytes, and lower
caller source limits. Sweep allocation failure through encoding and every complete decode
allocation. Run cluster, allocation-failure, sanitizer, formatting, static-analysis, and diff
gates.

## Migration or rollback considerations

No existing durable or wire bytes change. Rollback removes the unused control-plane authority
product; reducer jobs must remain unavailable rather than infer authority from partial fields.

## Unresolved questions

- Define the authenticated reducer-job prepare/seal envelope that nests authority and exact raw
  result schema bytes plus coordinator result routing.
- Package bounded reducer-job admission, progress, cancellation, and cleanup in the daemon query
  control service.

## References

- [Authority format](../formats/distributed-vector-grouped-aggregate-shuffle-authority-v1.md)
- [Complete node-bound authority](0502-complete-node-bound-grouped-shuffle-authority.md)
- [Independent process qualification](0535-independent-grouped-result-process-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
