# ADR 0366: Schema-bound distributed vector result exchange

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, networking, and distributed-systems maintainers
- **Extends:** [ADR 0364](0364-schema-light-distributed-vector-results.md)
- **Corrects:** the general-result payload assumption in
  [ADR 0350](0350-canonical-distributed-vector-batch-exchange.md)

## Context

Distributed Vector Exchange v1 wraps Columnar Batch v1 and therefore requires stored table and
schema identity. Fragment v2 now authorizes an exact schema-light result descriptor vector, but no
exchange can carry computed cells under that authorization. Native Protocol v1 already defines the
canonical schema-light cell representation, including duplicate output names, NULL rules, logical
type parameters, UTF-8 validation, and bounded decoding. Reimplementing those cells would create a
second semantic contract. Changing Exchange v1 would violate its accepted bytes.

## Decision

Distributed Vector Result Exchange v2 is a distinct `CHDXVEC2` checksummed frame in the cluster
layer. It retains the v1 query/tablet/sequence/terminal envelope shape, but its optional payload is
exactly one unchanged Native Protocol v1 `QUERY_RESULT` payload rather than Columnar Batch v1.

Every encoder, exact decoder, streaming reader, and write cursor requires the fragment-bound
Distributed Vector Result Schema v1 as a separate argument or owned construction value. A nonempty
payload is accepted only when every ordered native descriptor exactly equals that schema's name,
logical type parameters, and nullability. A terminal-only frame is legal under the supplied schema;
a nonterminal empty frame is not. The exchange does not serialize a table UUID, synthetic schema
UUID, expression, or native C++ object.

The cluster library owns this composition because it already depends on both the query and native
network libraries. The query library does not acquire a networking dependency. The reader is
single-owner and nonmovable, retains only the fixed integrity-protected header before allocating the
exact declared frame, leaves coalesced successor bytes caller-owned, and makes frame failures
sticky. The move-only cursor owns its encoded bytes and advances only by checked acknowledgement.

## Detailed rationale

Reusing the native payload gives distributed and client-facing results one logical-cell oracle.
Mandatory expected-schema arguments make authorization difficult to omit and make every batch
independently checkable against the same descriptor value carried by Fragment v2. A new magic and
major version keep mixed-version behavior unambiguous. Header and complete CRC32C values detect
accidental damage but are not authentication; the existing authenticated transport boundary remains
required before remote bytes can be trusted.

This closes the schema-light result-cell format gap without claiming worker execution, v2 fragment
transport, coordination, or process integration.

## Alternatives considered

- **Change Exchange v1 to accept native result payloads:** rejected because deployed or fixture
  bytes would become context-dependent and v1 decoders could no longer identify the nested format.
- **Create a new columnar computed-result format now:** deferred because the row-major native format
  is already canonical and no measurement demonstrates that a second encoding is required.
- **Embed Result Schema v1 beside every batch:** rejected because the native payload already repeats
  the exact descriptors. Exact comparison to the fragment-bound schema provides the binding without
  duplicate schema encodings.
- **Put the codec in the query library:** rejected because it would invert the existing dependency
  boundary by making query depend on the native network implementation.
- **Defer the decision:** rejected because Fragment v2 cannot progress to truthful general-result
  execution while only the table-shaped v1 exchange exists.

## Consequences

Workers and coordinators can now exchange reordered, repeated, aliased, zero-row, and aggregate-only
result shapes without inventing catalog identity. Every nonempty batch repeats descriptors, which
costs bytes but permits independent validation. The row-major payload is not claimed to be the
fastest possible vector transport. Future columnar result encoding requires a distinct negotiated
format and measurement evidence.

Fragment-v2 partial I/O and snapshot ownership, v2 cluster request/response carriage, a schema-bound
coordinator, worker execution, authenticated lifecycle, and process integration remain follow-up
work.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): fixed-width identities and explicit versioned wire
  fields remain portable; no native object representation is serialized.
- [Invariant 6](../architecture/invariants.md): header integrity and hard limits pass before any
  length-driven frame allocation; nested descriptors and cells are decoded under finite limits.
- [Invariant 10](../architecture/invariants.md): result descriptors are exact and no fabricated
  table or schema identity is introduced.
- [Invariant 14](../architecture/invariants.md): query/tablet/sequence and the fragment-bound schema
  remain explicit at the exchange handoff.
- [Invariant 18](../architecture/invariants.md): the reader and cursor have single-owner, bounded,
  fail-closed lifecycle semantics.

## Validation plan

Focused tests must round-trip duplicate-name mixed descriptors, canonical fixed/text/NULL cells, and
terminal-only empty streams; reject schema mismatch, nested damage, truncation, lower bounds, future
versions, and v1/v2 confusion; enumerate every partial-read split and coalesced-frame boundary; prove
short-write and moved-from cursor behavior; classify every owned codec allocation failure; compile
the public header independently; build/run through the installed external consumer; and run a
deterministic ASan/UBSan libFuzzer smoke corpus through exact and fragmented decoding.

The task is complete only when the normal and sanitizer-focused tests, pinned formatting, relevant
static analysis, and final diff review pass. No throughput claim is made without a separate
benchmark campaign.

## Migration or rollback considerations

Exchange v1 remains byte-for-byte unchanged and its decoder never accepts v2. V2 readers reject v1
magic, and checksum-valid unsupported v2 versions return `NOT_SUPPORTED`. Because ChronosDB is
pre-alpha and no deployed durable state uses this frame, rollout can gate v2 request/response
carriage explicitly. Rollback disables that carriage and continues using v1 only for truthfully
table-shaped results; no data conversion is required.

## Unresolved questions

- The v2 cluster request/response envelope and capability selection are owned by the next transport
  increment.
- Whether measured workloads justify a column-major result payload remains a Phase 18 measurement
  decision.

## References

- [Distributed Vector Result Exchange v2](../formats/distributed-vector-result-exchange-v2.md)
- [Distributed Vector Result Schema v1](../formats/distributed-vector-result-schema-v1.md)
- [Native Protocol v1](../protocol/native-v1.md)
- [ADR 0065](0065-self-describing-query-result-batches.md)
- [ADR 0365](0365-schema-bound-distributed-vector-fragment.md)
