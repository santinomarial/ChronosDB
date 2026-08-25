# ADR 0429: Distinct proof-bound mutable vector fragment

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, ingest, Raft, and distributed-systems maintainers
- **Extends:** [ADR 0353](0353-group-scoped-distributed-vector-fragment.md),
  [ADR 0375](0375-proof-revalidated-schema-bound-vector-row-worker-v2.md)

## Context

The existing distributed vector fragment is bound to one Manifest v2 generation whose durable
position must exactly equal the admitted Raft applied position. Reusing that field for a later
mutable `TabletState` publication would reinterpret an accepted network format and could mix a
durable snapshot with unrelated head state. Split-leader native reads instead need a remote unit
that can name the immutable committed/applied head publication directly.

## Decision

Distributed Mutable Vector Fragment v1 is a distinct checksummed format and in-memory authority
value. It binds query/database/table/tablet/schema/group identity, serving node, exact applied and
observed positions, placement epoch, consistency proof, projection, event-time predicate, vector
plan intent, and result schema. It deliberately contains no Manifest generation.

The binder accepts one `TabletSnapshot`, schema lineage, committed placement, group, read admission,
and plan. It requires the snapshot's `HeadCommitPosition` to be Raft-sourced from the exact group
and to equal the admitted applied position. Result schema is proved against the selected columns
and plan before publication.

The row worker independently validates the complete value, local route, current committed
placement, exact local linearizable barrier, active schema, and immutable tablet publication. It
then uses the established bounded head scan, exact event-time filter, and column-output operators.
It emits borrowed chunks only during the consumer call and publishes no partial success after a
failure. ORDER BY and LIMIT remain global coordinator semantics.

## Consequences

Mutable authority cannot be confused with durable CSEG authority or accepted by the legacy vector
decoder. The immutable `TabletSnapshot` pins every referenced generation during execution. Current
execution supports row-mode append-only heads; aggregate-state transport, authenticated carrier,
multi-tablet coordination, and native split-leader composition are subsequent boundaries.

Encoding and decoding are linear in projection, nested plan, and result-schema bytes under hard and
caller bounds. Execution is linear in visible head rows plus projected output. All owners are
single-thread-affine; no new synchronization or memory-ordering algorithm is introduced. Head
publication retains its existing release/acquire contract.

## Affected invariants

- [Invariant 4](../architecture/invariants.md): the selected publication records one exact applied
  Raft position.
- [Invariant 5](../architecture/invariants.md): uncommitted entries cannot enter a `TabletSnapshot`.
- [Invariant 6](../architecture/invariants.md): schema, heads, and position come from one immutable
  outer tablet publication.
- [Invariant 10](../architecture/invariants.md): both header and complete frame have CRC32C, and
  nested values retain independent integrity.
- [Invariant 11](../architecture/invariants.md): the worker retains the snapshot pin for all scans.
- [Invariant 14](../architecture/invariants.md): mutable authority has distinct magic/version and
  never reinterprets Fragment v1/v2.
- [Invariant 18](../architecture/invariants.md): a mismatched publication fails before output.

## Validation

Focused tests append two rows under one Raft group, bind the exact applied publication, round-trip
the canonical frame, reject legacy decoding and header damage, and execute the projected rows. A
request naming a later applied position is rejected before consumer output, and binding mixed
admission/publication positions fails closed.

## Migration and rollback

This is additive and is not yet emitted by a production carrier. Rollback removes the new request
and worker boundary without changing existing durable or network bytes.

## Retrospective note (2026-08-25)

[ADR 0490](0490-proof-revalidated-mutable-grouped-sufficient-state-worker.md) extends this exact
mutable authority into grouped sufficient-state binding and execution. It does not change the v1
fragment bytes or make them acceptable to the Manifest/CSEG grouped transport.

## References

- [Distributed Mutable Vector Fragment v1](../formats/distributed-mutable-vector-fragment-v1.md)
- [Correlated replicated read authority](0299-correlated-replicated-read-authority.md)
- [Authoritative co-located native query redirect](0428-authoritative-co-located-native-query-redirect.md)
