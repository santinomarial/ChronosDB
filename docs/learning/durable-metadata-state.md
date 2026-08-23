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
removes. A complete write that reports `EIO`, plus data-sync errors before and after the real sync,
withhold success but may leave a complete compacted record in the immediate restart image. Recovery
accepts that record only after full validation and requires its exact application snapshot before
reconstructing the catalog. The complete 10-by-5 matrix crosses every post-ownership application
install failure—from temporary creation through validation, writes, readback, synchronization,
close, rename, and final directory sync—with those five Raft outcomes on retry. It converges from
each observed image and survives another reopen; it does not reinterpret a failed compaction as
acknowledged durability or qualify power loss. This ordering is why the two owners can be recovered
independently without allowing Raft to reference missing catalog state.

Repeated failure does not weaken that convergence argument. A real-filesystem test performs two
separate partial application-snapshot writes, reopening each time to remove the prior temporary,
then performs two separate partial Raft compaction-record writes. Strict recovery rejects each
incomplete Raft tail without mutation; explicitly authorized repair removes exactly the 16-byte
prefix and preserves the installed application orphan. A final unfaulted retry adopts that orphan,
installs matching Raft authority, reconstructs the catalog, and survives a second reopen.

Recovery failures in both owners also compose without hidden in-memory continuation. An eight-case
matrix first leaves a partial application temporary and fails its next cleanup either before unlink
or after unlink at directory sync. After a clean application reopen, it installs the exact orphan,
leaves a partial Raft compaction record, and fails repair during size inspection, truncation,
repaired-file sync, or repair-directory sync. Depending on the ambiguous operation, the next owner
observes the temporary present or absent and the Raft prefix present or already removed. It restarts
validation from those bytes, finishes compaction, reconstructs the catalog, and reopens again.

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

Structure-aware fuzzing crosses the checksum gates rather than relying only on random bytes to do
so. It generates valid minor-0 and minor-1 snapshots with decodable nested commands and bindings,
mutates any byte, optionally refreshes entry/header/file checksums, truncates at input-selected
boundaries, and checks lowered caller limits. Any hostile input that decodes must converge to stable
canonical bytes after semantic re-encoding. CI runs a deterministic sanitizer-backed smoke; longer
campaigns remain a separate qualification record.

Scale qualification tests the format's actual entry ceiling rather than a small proxy: 65,536
retained application entries, nine voters, valid nested command bytes, nondecreasing terms, and an
exact re-encode. Both a 65,535 caller limit and entry 65,537 fail with `RESOURCE_EXHAUSTED`. Separate
local-only benchmarks time encode and owned decode at 1,024, 16,384, and 65,536 entries while
reporting the complete encoded size. The same maximum also crosses the real durable storage owner:
exact install and load, byte-identical retry, owner teardown, lock reacquisition, latest-file
discovery, and checked owned recovery. Separate local-only real-time benchmarks measure fresh
installations including readback, file sync, no-replace rename, and directory sync, plus restart
recovery including lock acquisition, discovery, and decode, at the same three entry counts. These
shapes characterize one host and filesystem only; they do not claim device-qualified durability or
production throughput.

Table-policy recovery preserves command order rather than retaining only the last map value. A
twelve-case filesystem matrix moves legacy partial retention, schema installation, first and
replacement complete policies, and matching legacy projections across the application-snapshot
boundary. It also commits suffix records that diverge independently in system-history or retry
retention. Valid schedules reopen to the exact latest complete policy and its derived legacy view;
invalid suffixes poison the live application owner, leave its applied index at the snapshot
boundary, and fail a fresh snapshot-plus-suffix recovery closed.

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
