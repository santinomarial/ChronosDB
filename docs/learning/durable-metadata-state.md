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
the applied index, cluster nodes in node-ID order, every complete definition in schema-ID order,
active table/schema pairs in table order, tablet placements in tablet order, and complete policies
in table order. Endpoints and definition strings are copied while immutable schemas remain shared
and pinned, so a composed runtime can retain the projection without borrowing the state machine's
internal maps.

`AsyncRaftMetadataApplication` owns the durable state machine on the asynchronous Raft worker. It
recovers and publishes the first complete projection before admission opens, then applies and
persists touched metadata-group batches before their completion is visible. Readers pin a
`shared_ptr<const MetadataCatalogSnapshot>` under a short mutex acquisition. Untouched Raft groups
reuse the exact prior projection; a retained projection remains valid after replacement or owner
shutdown, while new acquisition fails once the owner stops.

## Recovery and failure behavior

The applied index is not a catalog snapshot. Without compaction, startup constructs empty state and
replays every retained committed entry, then leaves or advances the durable applied index as
appropriate. Metadata Application Snapshot v1 retains exact original metadata and schema-definition
entries plus the Raft membership checkpoint rather than inventing a second latest-state catalog
grammar. Its locked storage installs exact immutable bytes before the owner compacts Raft to
matching metadata. Recovery requires that snapshot, recomputes its entry digest, decodes every
nested command, and then applies only the committed retained suffix. A failed live application
poisons the owner; restart revalidates authoritative snapshot and log bytes.

Obsolete application snapshots are reclaimed only against the current durable Raft boundary. The
owner exact-matches its adopted snapshot, revalidates that file, removes every older or future
canonical final, and synchronizes the directory. With a zero Raft snapshot it can remove all
pre-compaction crash orphans. Cleanup failure is retryable and does not reinterpret the already
durable catalog authority.

Command/definition size, names, columns, role arrays, endpoint bytes, replicas, nodes, schemas, and
tablets are explicitly bounded. Decoding validates fixed headers before length-driven work, owns
variable data, reconstructs schemas through the public semantic validator, and never dumps or loads
a native struct. A new definition updates schema identity, active-schema, and definition maps as one
coherent operation or reports resource exhaustion without advancing the applied index.

## Complexity and likely interview questions

Decode and apply are linear in entry bytes, column roles, and replica count; map updates are
`O(log N)`, with a linear catalog-name uniqueness check. Startup is linear in the installed snapshot
application entries plus retained committed suffix. Building the recovery projection is linear in current catalog size
and allocates only the explicitly bounded vectors and copied table names.

- Why is metadata placement not safe as local last-writer-wins state?
- Why must the command batch preflight before mutation?
- Why does recovery replay commands when Raft says they were already applied?
- Why is a complete schema a separate Raft entry instead of a Metadata Command v1 extension?
- What must a metadata application snapshot bind before log reclamation is safe?
- Why does placement metadata not itself change Raft voting membership?
- Why is an immutable catalog projection published only after `applied_index` persistence?
