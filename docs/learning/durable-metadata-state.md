# Durable Metadata Raft State

## Purpose and interfaces

`DurableMetadataStateMachine` connects the dedicated Raft group to deterministic catalog maps.
Entry type 2 is decoded under [Metadata Command v1](../formats/metadata-command-v1.md); entry type 3
is decoded under [Schema Definition v1](../formats/schema-definition-v1.md). The owner provides
read-only node, schema identity, complete schema, active table-definition, tablet-placement, and
complete/legacy-compatible table-policy lookups after committed application.

## Ordering and durability

```text
committed metadata entries
  -> exact checksum/version/limit decode of the complete batch
  -> apply variants in consecutive log-index order
  -> synchronize Raft applied_index
  -> optional leader quorum-sync/application proof
```

Pre-decoding prevents a corrupt later entry in the current batch from being discovered only after
earlier catalog mutation. The underlying state machine enforces monotonic placement epochs, stable
tablet-to-table identity, canonical replica membership, leader-in-replica membership, immutable
schema identity ownership, direct schema succession, unique table-name ownership, and bounded maps.
Complete definitions share immutable `TableSchema` ownership; readers retain stable definitions
until state-machine teardown.
Complete table policy applies only after its schema, atomically updates the compatibility retention
view, and prevents a later legacy partial command from contradicting the complete authority.

`catalog_snapshot()` creates one owning, deterministic recovery projection after replay. It carries
the applied index, every complete definition in schema-ID order, active table/schema pairs in table
order, tablet placements in tablet order, and complete policies in table order. Definition strings
are copied while immutable schemas remain shared and pinned, so a composed runtime can retain the
projection without borrowing the state machine's internal maps.

## Recovery and failure behavior

The applied index is not a catalog snapshot. Startup constructs empty state and replays every
retained committed entry, then leaves or advances the durable applied index as appropriate. A
compacted prefix is rejected because no metadata application snapshot exists yet. A failed live
application poisons the owner; restart revalidates authoritative log bytes.

Command/definition size, names, columns, role arrays, endpoint bytes, replicas, nodes, schemas, and
tablets are explicitly bounded. Decoding validates fixed headers before length-driven work, owns
variable data, reconstructs schemas through the public semantic validator, and never dumps or loads
a native struct. A new definition updates schema identity, active-schema, and definition maps as one
coherent operation or reports resource exhaustion without advancing the applied index.

## Complexity and likely interview questions

Decode and apply are linear in entry bytes, column roles, and replica count; map updates are
`O(log N)`, with a linear catalog-name uniqueness check. Startup is linear in retained committed
history until snapshots exist. Building the recovery projection is linear in current catalog size
and allocates only the explicitly bounded vectors and copied table names.

- Why is metadata placement not safe as local last-writer-wins state?
- Why must the command batch preflight before mutation?
- Why does recovery replay commands when Raft says they were already applied?
- Why is a complete schema a separate Raft entry instead of a Metadata Command v1 extension?
- What must a metadata application snapshot bind before log reclamation is safe?
- Why does placement metadata not itself change Raft voting membership?
