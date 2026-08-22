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

Logical metadata entry type 4 carries [Tablet Group Binding v1](../formats/tablet-group-binding-v1.md).
Application requires an existing placement and then fixes the tablet's group permanently. The
owning catalog projection publishes bindings in tablet order, allowing routing to join a tablet's
current placement and node endpoints without inferring consensus identity. Metadata Application
Snapshot 1.1 retains those exact type-4 bytes; minor 0 remains restricted to metadata and schema
entries.

## Recovery and failure behavior

The applied index is not a catalog snapshot. Without compaction, startup constructs empty state and
replays every retained committed entry, then leaves or advances the durable applied index as
appropriate. Metadata Application Snapshot v1 retains exact original metadata and schema-definition
entries plus the Raft membership checkpoint rather than inventing a second latest-state catalog
grammar. Its locked storage installs exact immutable bytes before the owner compacts Raft to
matching metadata. Recovery requires that snapshot, recomputes its entry digest, decodes every
nested command, and then applies only the committed retained suffix. A failed live application
poisons the owner; restart revalidates authoritative snapshot and log bytes. Snapshot installation
withholds success at every failed filesystem stage. A directory-sync failure after the final rename
also poisons the live storage owner because the name may be visible without proved directory
durability; reopening revalidates that exact final before an idempotent retry can adopt it. Abrupt
process termination before the rename leaves no final authority after temporary cleanup, while
termination after the rename exposes only the exact validated immutable bytes. Repeated reopen and
retry therefore converge without manufacturing or losing an acknowledged snapshot.

Owned compaction has a cross-domain crash boundary: application bytes are installed first, while
Raft retains the replayable log until its own compaction record is written and synchronized. A
ten-cut real-process matrix terminates the owner after each application installation transition,
the Raft record write and sync, and the successful return. Before Raft selects the snapshot,
recovery rebuilds from the retained log; an exact retry creates the missing application file or
adopts the deterministic orphan already renamed into place. Once the process-restart image selects
the Raft snapshot, recovery fails closed unless that exact file agrees with all `SnapshotMetadata`,
then reconstructs the same catalog. Reopening a second time proves the converged authority is
stable. The completed-write cut is useful process-crash evidence but does not replace device or
power-loss qualification; the API acknowledges compaction only after Raft synchronization.

Injected-I/O composition additionally arms the application and Raft owners together. If the
application temporary is only partially written, or its final directory sync fails after rename,
the same compaction call returns failure without touching Raft. Recovery cleans the temporary or
adopts the exact final, reconstructs the catalog from the retained log, and retries while the Raft
fault remains armed. A pre-write Raft error leaves a clean retained log; a prefix write followed by
error leaves an incomplete final record that strict recovery rejects and explicit tail repair
removes. Both paths retain the installed application orphan, withhold compaction success twice, and
converge only when a later retry durably selects those exact bytes. This ordering is why the two
owners can be recovered independently without allowing Raft to reference missing catalog state.

The structural format is pinned independently for compatibility. One minor-0 fixture retains a
valid Metadata Command v1 cluster-node payload; one minor-1 fixture adds a valid Tablet Group
Binding v1 payload. Their nested envelopes and outer bytes were packed from the specifications with
independent SHA-256 and CRC32C implementations. Production encoding must equal each complete fixture
and decoding must reconstruct its exact group, snapshot metadata, voters, entry gaps, types, and
payloads. This catches compiler- or refactor-dependent layout drift that a round trip through the
same implementation could miss.

The structural codec also keeps allocation failure inside its result boundary. Dedicated sweeps
select every allocation made while encoding each minor and while decoding its voter vector, entry
container, and owned payloads. Every selected failure is `RESOURCE_EXHAUSTED`; a retry without that
failure reconstructs the exact snapshot, so no partially decoded state escapes.

Obsolete application snapshots are reclaimed only against the current durable Raft boundary. The
owner exact-matches its adopted snapshot, revalidates that file, removes every older or future
canonical final, and synchronizes the directory. With a zero Raft snapshot it can remove all
pre-compaction crash orphans. Cleanup failure is retryable and does not reinterpret the already
durable catalog authority. Reclamation validates the authoritative file through open, stat, size,
and exact read before listing deletion candidates. An enumeration, any individual unlink, or final
directory-sync failure leaves only a valid prefix of the planned obsolete deletions; the owner stays
usable, never removes the authority, and an exact retry converges. The zero-authority path follows
the same rule while treating every canonical final as an orphan. Real-process termination after
enumeration, any completed unlink, directory synchronization, or reported success reopens to that
exact namespace prefix. Retrying and reopening again preserves the middle authoritative snapshot or
converges to an empty zero-authority directory, respectively.

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
- Why must tablet group identity be separate from mutable placement and leader hints?
