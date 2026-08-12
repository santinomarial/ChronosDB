# ADR 0214: Durable Complete Schema Definitions

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB schema, catalog, and Raft maintainers

## Context

Metadata Command v1 kind 2 stores schema identity, table identity, and schema version, but not the
table's SQL name, columns, logical types, nullability, parent, or column roles. That identity record
cannot reconstruct a query/ingest catalog after restart. Reinterpreting its frozen bytes or keeping
a second local catalog file would either break compatibility or create competing durability
authorities.

## Decision

The metadata Raft group assigns logical entry type `3` to the versioned, checksummed Schema
Definition v1 format. Each entry owns one complete immutable `TableSchema`, its SQL catalog name,
and quoted-name semantics. Metadata Command v1 and its type-2 Raft entry remain byte-for-byte
unchanged.

Committed definitions apply at consecutive Raft indexes alongside existing metadata commands. The
catalog enforces immutable schema identities, version-1 creation, direct v1 successor evolution,
stable names across schema evolution, unique name ownership across table identities, and coherent
active-schema publication. Recovery
replays the complete retained committed metadata log. A compacted prefix still fails closed until a
metadata application snapshot can carry the same definitions.

## Detailed rationale

An independent entry type preserves existing log compatibility while giving the larger schema
payload its own 16-MiB bound and evolution namespace. Keeping definitions in the metadata group
makes committed log order the sole catalog authority. Reusing `TableSchema::create` and
`validate_v1_successor` prevents the durable decoder and live state machine from inventing weaker
schema rules.

## Alternatives considered

- Extending Metadata Command v1 kind 2 would reinterpret or fork an accepted durable layout.
- Storing schema definitions in an adjacent catalog file would require an atomic cross-log commit
  protocol and ambiguous recovery precedence.
- Reconstructing schemas from data files cannot recover SQL naming and would treat derived storage
  as metadata authority.
- Deferring the decision would leave the packaged daemon unable to recover a usable data plane.

## Consequences

The metadata state machine can now answer complete-definition and active-definition lookups after
restart. Catalog size and string/role counts are bounded before retention. Recovery time remains
linear in retained metadata history, and metadata log compaction remains unavailable until its
application snapshot format includes complete definitions.

## Affected invariants

This decision strengthens invariants 1, 3, 4, 8, and 14: durable bytes are versioned and
checksummed, recovery derives state from one committed authority, schema evolution is validated,
variable input is bounded, and no partially installed catalog state is published.

## Validation plan

Codec tests require deterministic round trips, quoted-name preservation, rejection of every
truncation, checksum damage, unsupported versions, and configured limits. State tests cover exact
commit order and legal successors. Durable-runtime tests close and reopen the physical Raft log and
compare the reconstructed complete definition. Golden fixtures, fuzzing, allocation fault
injection, metadata snapshots, and large-catalog measurements remain required for phase exit.

## Migration or rollback considerations

Older binaries reject entry type `3` as unsupported and therefore fail closed instead of serving an
incomplete catalog. Mixed-version rollout must keep old nodes out of metadata leadership and apply
until all participants understand the new entry type. No deployed format conversion is needed in
the pre-alpha repository.

## Unresolved questions

Metadata application snapshot bytes, database namespaces, rename commands, cluster epochs, and
drop/tombstone semantics are deferred. None changes the immutable definition contract selected
here.

## References

- [ADR 0014](0014-logical-types-schema-identity-and-evolution.md)
- [ADR 0075](0075-durable-metadata-raft-commands.md)
- [Schema Definition v1](../formats/schema-definition-v1.md)
- [Metadata Command v1](../formats/metadata-command-v1.md)
- [Architecture invariants](../architecture/invariants.md)
