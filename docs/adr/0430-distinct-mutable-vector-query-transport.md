# ADR 0430: Distinct mutable vector query transport

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0429](0429-distinct-proof-bound-mutable-vector-fragment.md),
  [ADR 0368](0368-schema-bound-distributed-vector-query-transport-v2.md)

## Context

The proof-bound mutable fragment could be executed locally but had no bounded remote carrier.
Sending it under the accepted durable Fragment-v2 request magic would reinterpret that protocol's
Manifest authority. The existing schema-bound result response, however, carries no snapshot source
and is already sufficient to validate typed, correlated terminal row batches.

## Decision

Distributed Mutable Vector Query Transport v1 introduces a distinct request magic/version around
one exact mutable fragment. Source and target nodes, payload length, and nested authority receive
independent header, payload, nested, and complete-frame CRC validation. The target must equal the
fragment's serving node. A bounded streaming reader verifies its header before allocating, and a
move-only short-write cursor retains the complete request.

The receiver requires a previously authenticated principal, authorizes that principal for the
claimed source node, validates the local target, and invokes one embedding-owned worker. Worker
exceptions become terminal status rather than escaping. The receiver validates complete sequence,
query/tablet correlation, result schema, terminal closure, frame count, and byte bounds before
returning encoded output. Unavailable results may carry an advisory current-leader hint.

Successful and failure output deliberately reuses Distributed Vector Query Response v2. That
format is authority-agnostic and already exact-decodes result batches against the request-owned
schema. The finite sender retains the immutable fragment for every retry, rejects response
correlation or schema drift, and publishes results only after a complete terminal stream.

## Consequences

Mutable request bytes cannot be decoded as a durable Fragment-v2 request, and no retry can silently
rewrite the serving node or applied-position proof. Request framing and response retention are
linear in bounded bytes. Receiver and sender owners are single-thread-affine; no new synchronization
or memory-ordering algorithm is introduced.

This boundary does not itself perform TLS handshakes or socket I/O. A TLS carrier must produce the
authenticated peer result before passing request bytes, and packaged multi-tablet scheduling and
native split-leader composition remain subsequent work.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the exact nested applied authority is retained.
- [Invariant 6](../architecture/invariants.md): retries cannot mix snapshot publications.
- [Invariant 10](../architecture/invariants.md): header, nested payload, and complete frames have
  integrity coverage.
- [Invariant 14](../architecture/invariants.md): mutable requests have distinct magic/version.
- [Invariant 15](../architecture/invariants.md): request/response counts and bytes are bounded.
- [Invariant 18](../architecture/invariants.md): route, identity, schema, and terminal mismatches fail
  closed.

## Validation

Focused tests round-trip the distinct request, reject the legacy decoder and damaged nested bytes,
exercise every two-part streaming split, enforce typed short writes, prove authentication and
source authorization precede worker execution, carry unavailable leader hints, and accept only a
complete correlated terminal sender result. Allocation sweeps cover request encode/decode/reader,
receiver publication, and sender retention and preserve resource-exhaustion classification. Header
self-containment protects the public boundary.

## Migration and rollback

This is additive and not yet enabled by a packaged listener. Rollback removes the mutable carrier
without changing durable formats, existing vector-v2 requests, or the shared response format.

## References

- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Distributed Mutable Vector Fragment v1](../formats/distributed-mutable-vector-fragment-v1.md)
- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
