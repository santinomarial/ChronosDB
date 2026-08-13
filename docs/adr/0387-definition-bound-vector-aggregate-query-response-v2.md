# ADR 0387: Definition-bound vector aggregate query response v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0368](0368-schema-bound-distributed-vector-query-transport-v2.md),
  [ADR 0382](0382-schema-bound-ungrouped-vector-aggregate-exchange.md)

## Context

Fragment v2 already carries general vector plans, including ungrouped aggregates, in one canonical
`CHDVREQ2` request. Its existing `CHDVRSP2` response is frozen around schema-light native result
batches and requires a result schema to decode. Mergeable aggregate states instead require the
complete ordered aggregate definition vector and query resource authority for variable extrema.
Adding another payload kind to the deployed row response would make one response version depend on
caller-selected decode context and could let row and state payloads be confused.

## Decision

Aggregate requests reuse exact `CHDVREQ2` bytes. A capability-selected aggregate endpoint admits
only ungrouped aggregate plans and replies with a distinct `CHDVARP2` response. The response retains
the v2 route, correlation, status, leader-hint, integrity, and 112-byte header layout. Success kind
1 carries exactly one Distributed Vector Aggregate Exchange v1 value; failures carry no payload.

Every response API requires the complete expected definition vector, including failure encode and
decode. Exact decode and the reader additionally require a query resource context and bounded
nested-state decode limits. The nested query/tablet must exact-match the outer identity. The reader
validates a checksummed header plus physical, deployment, and caller bounds before exact allocation;
it owns one frame, leaves successor bytes caller-owned, and makes frame failure sticky. The typed
move-only cursor accepts no arbitrary bytes.

CRC32C remains damage detection, not authentication. Receiver authorization, plan-mode admission,
worker execution, stream closure, retry policy, TLS/TCP lifecycle, and process ownership remain
later boundaries.

## Alternatives considered

- **Add aggregate payload kind 2 to `CHDVRSP2`:** rejected because the frozen row response requires
  schema authority while aggregate states require definitions and resource authority.
- **Create a second request magic:** rejected because Fragment v2 already canonically identifies
  the plan mode and authority; duplicating all request bytes would create two encodings for the
  same dispatch.
- **Decode aggregate state from self-described bytes:** rejected because encoded definitions are
  values to verify against admitted plan authority, not a trusted type source.

## Consequences

Row response decoders reject aggregate response magic and aggregate response decoders reject row
magic. Variable extrema cannot outlive their query memory reservation. The largest response is
1,048,908 bytes and a failure remains 116 bytes. A listener or negotiated capability must select
the aggregate endpoint explicitly and must never retry rejected bytes through the row decoder.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): aggregate response bytes have distinct magic and
  explicit versioning.
- [Invariant 6](../architecture/invariants.md): fixed integrity and finite bounds precede declared
  frame allocation.
- [Invariant 10](../architecture/invariants.md): state definitions are exact-matched to admitted
  Fragment-v2 authority.
- [Invariant 14](../architecture/invariants.md): route and query/tablet correlation remain explicit
  across the cluster handoff.
- [Invariant 15](../architecture/invariants.md): row and aggregate response formats cannot silently
  downgrade or reinterpret each other.
- [Invariant 18](../architecture/invariants.md): definition, resource, reader, and cursor lifetimes
  are explicit.

## Validation plan

Freeze the magic/layout and round-trip a definition-bound state plus correlated failure. Reject
nested damage with valid outer checksums, future versions, cross-format magic, definition mismatch,
uncorrelated values, and lower frame bounds. Enumerate every read split and coalesced suffix, prove
sticky failure and cursor move/short-write behavior, inject allocation failure, compile the public
header standalone, and run formatting, static analysis, sanitizers, the full suite, and installed
consumer.

## Migration or rollback considerations

This is additive. Rollback disables the aggregate endpoint; `CHDVREQ2` row handling and
`CHDVRSP2` bytes remain unchanged. No implementation may reinterpret `CHDVARP2` as a row response.

## References

- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
- [Distributed Vector Aggregate Exchange v1](../formats/distributed-vector-aggregate-exchange-v1.md)
