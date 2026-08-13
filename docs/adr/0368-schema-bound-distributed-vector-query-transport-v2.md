# ADR 0368: Schema-bound distributed vector query transport v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0360](0360-distinct-distributed-vector-query-request.md),
  [ADR 0361](0361-correlated-distributed-vector-query-response.md),
  [ADR 0168](0168-authenticated-distributed-query-transport.md)

## Context

Distributed Vector Query Transport v1 carries table-shaped Fragment v1 and Vector Exchange v1
values. Fragment v2 and Result Exchange v2 now provide the schema-light result identity and cells
required for general vector plans, but wrapping them in v1 transport magic would make one accepted
version context-dependent. A response decoder also cannot validate Result Exchange v2 unless the
admitted Fragment-v2 schema is mandatory context.

## Decision

Distributed Vector Query Transport v2 uses distinct `CHDVREQ2` and `CHDVRSP2` magics and major
version 2. It retains the v1 outer header shapes and route/status/leader-hint meanings. A request
payload is exactly one Distributed Vector Fragment v2. A successful response payload is exactly one
Distributed Vector Result Exchange v2; failures carry no payload.

Every response encoder, exact decoder, reader, and typed write-cursor constructor requires the
expected result schema. The nested exchange must exact-match that schema, and its query/tablet must
also match the outer response. This prevents schema validation from being omitted at the transport
boundary. Failure responses still require a valid expected schema because they belong to the same
admitted query context.

Request and response readers retain only their fixed headers until magic, header CRC32C, exact
version, route, status/payload relationship, hard lengths, caller frame limit, and reserved bytes
pass. They then allocate exactly one declared frame, leave coalesced successor bytes caller-owned,
exact-decode before publication, and retain sticky frame failure. The response reader owns its
schema through an rvalue-only constructor. The move-only typed cursor encodes either a request or a
schema-supplied response and cannot accept arbitrary bytes.

CRC32C remains damage detection, not authentication. This decision freezes carrier bytes and
partial-I/O ownership; peer authentication, principal authorization, socket/TLS sessions, retries,
execution, and coordination remain later owners.

## Alternatives considered

- **Reuse v1 magic with a new payload kind:** rejected because v1 request has no payload-kind field
  and accepted v1 response kind 1 already means table-shaped Vector Exchange v1.
- **Infer the result schema from the nested response:** rejected because those descriptors are data
  to validate, not the Fragment-v2 authorization source.
- **Embed the expected schema again in the response envelope:** rejected because Result Exchange v2
  already repeats descriptors and exact comparison to the admitted schema is sufficient.
- **Accept raw encoded bytes in the write cursor:** rejected because a response could then bypass
  mandatory schema validation.

## Consequences

Cluster carriers can now preserve the complete schema-bound request/result contract without
changing or weakening v1. Maximum request size is 4,344,308 bytes and maximum response size is
16,777,416 bytes. Failure responses remain a fixed 116 bytes. Descriptor repetition remains the
same measured-future tradeoff accepted by Result Exchange v2.

Authenticated receiver/sender ownership, TLS/TCP lifecycle, schema-bound coordination, worker
execution, and process integration remain incomplete and are not claimed here.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the outer transport and both nested formats retain
  explicit, distinct versions and fixed-width fields.
- [Invariant 6](../architecture/invariants.md): fixed-header integrity and finite hard/caller bounds
  pass before declared-length allocation.
- [Invariant 10](../architecture/invariants.md): response descriptors are checked against the
  caller-owned admitted schema rather than fabricated table identity.
- [Invariant 14](../architecture/invariants.md): node route, query/tablet correlation, authority
  fragment, and result schema remain explicit at the cluster handoff.
- [Invariant 15](../architecture/invariants.md): v1 and v2 reject each other's magic and cannot
  silently downgrade.
- [Invariant 18](../architecture/invariants.md): reader and cursor ownership, schema lifetime, and
  serialization requirements are explicit.

## Validation plan

Focused tests round-trip Fragment-v2 requests, schema-light success payloads, and correlated failure
responses; reject truncation, nested damage, schema mismatch, outer correlation mismatch,
checksum-valid future versions, and v1/v2 confusion; enumerate every request and response split and
coalesced boundaries; prove lower bounds, sticky failure, typed short writes, and moved-from cursor
state; classify owned allocation failures; and compile/install the public API. ASan/UBSan, pinned
formatting, relevant static analysis, and a full serialized suite are required before completion.

## Migration or rollback considerations

V1 remains unchanged. A capability-selection or listener boundary must choose v2 explicitly; it
must never reinterpret a rejected v2 frame as v1. Rollback disables v2 carriage and can continue v1
only for results truthfully representable by the v1 table-shaped contracts.

## References

- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
- [Distributed Vector Fragment v2](../formats/distributed-vector-fragment-v2.md)
- [Distributed Vector Result Exchange v2](../formats/distributed-vector-result-exchange-v2.md)
- [ADR 0367](0367-bounded-distributed-vector-fragment-v2-ownership.md)
