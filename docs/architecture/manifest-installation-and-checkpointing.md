# Manifest Installation and Checkpointing

> **Status: accepted Phase 6 design; sealed-head conversion, filesystem installation, read-only
> checkpoint coverage proof, checkpointed WAL reopen/reclamation, manifest selection, bounded
> sealed-head scheduling, atomic database storage publication, and the end-to-end single-part
> flush coordinator, filesystem crash matrix, and caller-catalog columnar startup composition
> implemented.** This document
> defines ownership,
> durable ordering, recovery, and publication around the normative
> [Manifest v1 bytes](../formats/manifest-v1.md). It refines
> [ADR 0017](../adr/0017-manifest-generations-installation-and-checkpoints.md), the
> [mutable-head publication contract](mutable-head-publication.md), and the
> [WAL recovery state machine](wal-recovery.md). Compaction, retry pruning, and reclamation of
> installed CSEG/manifest files remain outside this phase.

## Safety objective

At every process-crash point, recovery selects either the previous complete manifest generation or
the new complete generation. A selected manifest references only complete durable CSEG files. A
sealed head remains query-visible until one atomic snapshot publication substitutes the installed
part set. WAL bytes are removable only after a durable checkpoint represents their rows,
application positions, schemas, and protected retry outcomes.

Availability yields to this proof. Missing/corrupt referenced state, unsupported durable semantics,
ambiguous names, failed synchronization, or an unexpected post-WAL transition fails closed before
query service or later cleanup.

## Ownership and synchronization

One aggregate database owner holds `manifest/LOCK` and the existing WAL writer/`wal/LOCK` for the
database lifetime. Locks are acquired in manifest-then-WAL order and released in reverse order.
The storage worker owns manifest/part state; the serialized WAL owner retains its active file and
executes checkpoint-driven closed-segment cleanup on an explicit request. No thread opens or closes
an independent descriptor for either lock inode. Together they serialize:

- part candidate creation and installation;
- manifest generation construction and installation;
- startup temporary cleanup and namespace classification;
- checkpoint advancement and WAL segment removal; and
- publication of the current database storage snapshot.

Out-of-band modification while these locks are held is unsupported and detected where possible by
name/type/content revalidation. Append, rotation, checkpoint-coordinate capture, and closed-segment
deletion are ordered by the WAL owner; part/manifest installation and database publication are
ordered by the storage owner. Their handoff is bounded and never performs a filesystem namespace
mutation concurrently with WAL rotation.

Shard workers remain the only mutable-tablet writers. A sealed `HeadSnapshot` is immutable and
copyable, so the shard transfers an owning pin through a bounded flush queue. The storage worker may
sort, encode, and perform blocking I/O without accessing live head boundaries. Backpressure occurs
before a new WAL admission if the sealed-generation or flush queue bound is full.

`SealedHeadFlushQueue` implements that handoff as a fixed-capacity MPSC/single-consumer queue. A
shard first reserves one slot without changing topology. It stages the exact sealed pin, publishes
the new outer tablet epoch with release ordering, and only then makes the slot ready under the queue
mutex. Reservations have monotonically increasing sequence numbers; a later ready producer cannot
overtake an earlier unfinished reservation. One move-only consumer lease is allowed at a time.
Dropping or explicitly retrying the lease preserves its original enqueue age and position. Capacity
is released only when `complete` validates the exact non-forgeable replacement receipt issued by
aggregate database publication.

Readers acquire an immutable database snapshot descriptor through one release/acquire publication.
That descriptor owns the selected manifest generation, installed-part handles/identities, and exact
head snapshots. Old descriptors continue to own their old heads and parts. Phase 6 does not delete
final CSEG or manifest files, so publication proves lifetime without a concurrent file-unlink edge.

## Durable directory prerequisites

Database creation establishes `parts/` and `manifest/` beneath an already opened database root,
synchronizes the root after their names are created, creates/acquires `manifest/LOCK`, and installs
manifest generation 1 for the existing WAL identity. Generation 1 may contain no tablet, part,
retry, or record coverage and uses the exact empty-prefix WAL coordinate `(segment 1, offset 64,
record sequence 0)`.

Opening and creating are distinct. An existing database without a final manifest generation is not
silently initialized. Likewise, an existing WAL identity cannot be paired with a newly generated
database/manifest identity by an ordinary recovery path.

All temporary/final renames stay within one directory and use atomic no-replace semantics. The
reference Linux durability contract requires regular local files, one filesystem for each rename,
truthful `fsync`/directory synchronization, and hardware/storage behavior consistent with the
documented `LOCAL_SYNC` assumptions. macOS execution is correctness evidence, not an unqualified
power-loss claim.

## Part installation protocol

The storage worker receives an exact owned `EncodedCsegPart`, expected table/tablet/schema identity,
the retained catalog schema, and the sealed-head recovery coverage it is intended to replace.

Before filesystem mutation it:

1. exact-decodes the CSEG image;
2. performs complete schema-independent semantic validation;
3. performs exact schema/tablet binding;
4. recomputes row count, event-time extrema, WAL identity, and minimum/maximum record sequence; and
5. verifies every input row identity belongs to the sealed head being flushed.

It then installs one immutable file:

1. create the exact recognized temporary basename exclusively in `parts/`;
2. write every encoded byte at explicit offset, treating a partial hard failure as candidate
   failure;
3. read back the exact file size and bytes and repeat complete CSEG/content/schema validation;
4. synchronize the complete temporary file with `fsync`/`sync_all`;
5. atomically rename it to the identity-derived final basename without replacement; and
6. synchronize `parts/`.

Step 6 is the **part installation durability boundary**. A failure before it never permits a
manifest reference. A failure after a partial write leaves only a recognized temporary. A crash
after rename but before directory sync may leave a valid unreferenced final or no final name; both
are safe because the manifest has not named it. A collision with an existing final identity is not
treated as success unless recovery independently proves it is the exact already-installed immutable
object selected by the current manifest; ordinary installation never overwrites it.

The protocol is per part. If a flush produces multiple parts, all cross their individual durability
boundaries before the generation that adds the set is constructed.

## Deterministic sealed-head conversion

A sealed generation is schema-bound and retains user columns plus exact WAL ID, record sequence,
batch row ordinal, and operation metadata. Conversion materializes physical rows without changing
values or identities, sorts by the schema physical ordering key followed by the CSEG v1 system
identity, selects canonical granule boundaries, and encodes PLAIN pages under the explicit raw or
Zstandard policy.

Sorting affects storage order only. It does not change commit order, retry outcome, or snapshot
visibility. The output row multiset and each `RowVersionIdentity` are compared with the sealed-head
input before installation. An output split, if needed, uses deterministic contiguous sorted ranges,
gives every file a distinct nonzero `PartId`, and preserves the exact union. No Phase 6 flush merges
two existing parts, removes an installed part, resolves versions, creates a delta classification, or
edits an installed file.

## Building a manifest generation

A generation builder starts from one owning selected-state snapshot. It applies one checked logical
edit and produces a new immutable full state:

- add the newly durable part descriptors;
- advance only tablet recovery boundaries proven fully represented by installed parts and exact
  retry outcomes;
- record the schema active at each advanced boundary;
- copy every existing protected retry entry and add newly covered outcomes;
- optionally advance the global reclaim coordinate through the longest completely represented WAL
  prefix; and
- leave every other tablet/part/retry entry byte-for-byte logically unchanged.

The builder rejects removal/replacement, identity changes, schema regression/branching, duplicate
parts/retries, backward positions, a part row beyond its tablet boundary, a missing protected retry
outcome, or a global coordinate that crosses an uncovered application record. It preflights the WAL
prefix and decodes every covered `COLUMNAR_APPEND` command against the proposed state before any
manifest file write.

The coverage proof for record `R` requires:

- its physical framing and application command are verified;
- its target tablet descriptor has `durable_record_sequence >= R`;
- a first-applied command's exact rows occur in installed parts with matching WAL/sequence/row
  ordinals; and
- the retry descriptor preserves its identity, digest, target, original outcome sequence, and row
  count. A matching duplicate has no additional rows and matches that same descriptor.

No other application kind is currently assigned to this checkpoint state machine. Unsupported
required kinds reject advancement before durable mutation.

## Manifest installation protocol

Given proposed generation `N + 1`, the storage owner:

1. revalidates every referenced final part name/type and exact descriptor/content/schema binding;
2. validates the transition from selected generation `N` and encodes one canonical Manifest v1
   image;
3. creates the exact generation temporary basename exclusively in `manifest/`;
4. writes the complete bytes and exact-readback decodes them from the file;
5. synchronizes the temporary manifest file;
6. atomically renames it to the final generation basename without replacement; and
7. synchronizes `manifest/`.

Step 7 is the **manifest publication durability boundary**. Only after it succeeds may the process
release-publish an in-memory database snapshot descriptor selecting the new generation and removing
the covered sealed-head references from new snapshots.

A crash before the rename leaves a temporary and selects `N`. A crash after rename may expose the
complete `N + 1` final name; selecting it is safe because part durability preceded manifest writing.
If the rename is lost, `N` remains highest. The process does not report flush/checkpoint success
until the directory sync completes even though recovery may safely accept a complete final name
observed in a crash image.

An expected installation error publishes neither the manifest nor in-memory replacement. The
sealed head stays pinned and visible, and retry policy may construct a fresh candidate identity. An
unexpected error after the durable generation but before in-memory publication fails the live owner
closed; restart recovery selects the durable truth instead of attempting an in-process rollback.

## Atomic head-to-part visibility

The old database snapshot descriptor owns the sealed `HeadSnapshot` and generation `N`. The new
descriptor owns generation `N + 1`, its part set, and excludes exactly the sealed head rows replaced
by the edit. The storage owner initializes the complete new descriptor, then release-publishes one
`shared_ptr<const DatabaseStoragePublication>` (or an equivalent single atomic object).

A reader acquire-loads exactly one descriptor. Observing the old pointer yields the head and old
parts; observing the new pointer yields the replacement part and remaining/new heads. Because all
new descriptor initialization and durable-boundary success precede the release store, no reader can
observe both logical copies or neither. Owning pointers keep the selected memory/files valid.

`DatabaseStoragePublisher` uses one atomically loaded/stored
`shared_ptr<const DatabaseStoragePublication>` as that exact object. Tablet refresh and durable
Manifest replacement both initialize a complete immutable descriptor before its release store.
The latter verifies the exact new part/head identity, schema, row count, WAL extrema, event-time
extrema, and remaining-head durable boundary before removing a sealed-head pin. Deterministic
publication-hook tests pause immediately before the store, and sanitizer runs retain old snapshots
while readers acquire the old or new complete epoch. A collection of independently loaded head and
manifest pointers does not satisfy this contract.

The new database epoch exposes a non-forgeable `SealedGenerationRetirementReceipt` for each exact
replacement. Only after that epoch is release-published may the shard writer pass the receipt to
`TabletState`. Tablet retirement verifies tablet, schema, generation, row count, WAL identity, and
record-sequence bounds, then release-publishes an outer tablet epoch without the sealed pin.
Repeated receipt consumption is idempotent. This second publication releases bounded mutable-state
backpressure; it is not the query visibility boundary, which already occurred at the aggregate
database pointer. The same receipt authorizes removal of the completed item from the flush queue;
hostile tablet/schema/generation/row/WAL bounds leave the item in flight and retryable.

## Checkpoint and WAL reclamation

Checkpoint advancement is a manifest state transition. Its global reclaim coordinate is the end of
the longest physically consecutive WAL prefix whose logical effects and protected retry outcomes
are represented. The coordinate may trail some tablet durable boundaries because other tablets
flush independently.

After the new manifest crosses its publication durability boundary, the storage owner may ask the
serialized WAL owner to remove a closed segment only when every record in it is at or before the
selected global coordinate. It never removes the active highest segment. Removal order is:

1. verify the selected manifest still owns the same WAL identity and coordinate;
2. verify candidate closed segments and their maximum record sequences are covered;
3. unlink eligible final segments in increasing sequence order; and
4. synchronize the WAL directory.

Deletion is cleanup, not checkpoint publication. A crash can retain extra covered segments. Recovery
ignores/verifies them according to the manifest context and may repeat deletion. Power-loss
persistence need not preserve unlink call order, so any absent/present subset wholly before the
coordinate is accepted; a missing coordinate segment is accepted only when it was completely
covered and its immediate successor begins at the exact next sequence. Any missing required suffix
is corruption. Directory-sync failure leaves the storage owner failed closed and cannot advance a
reclamation metric or delete further files until recovery.

The active segment is not prefix-truncated. If the coordinate lies within it, the segment remains
and manifest-aware recovery begins at the recorded byte offset. Once rotation closes it and a later
segment exists, it becomes eligible if fully covered.

No CSEG part or manifest-generation deletion is included. A checkpoint does not override an active
reader, backup, subscription, or future replication pin.

## Startup recovery

The implemented `recover_manifest_columnar_database` owner composes this sequence for the currently
assigned `COLUMNAR_APPEND` application kind and caller-supplied retained catalog. It holds
`manifest/LOCK` before opening the WAL, converts only the selected validated Manifest descriptors
to exact ingest recovery seeds, requires every seeded retry original and tablet boundary after the
global checkpoint to occur in the verified suffix, cleans recognized part/Manifest temporaries,
optionally revalidates and removes checkpoint-covered closed WAL segments, and creates one
unpublished-until-return aggregate storage epoch. The returned move-only owner
retains Manifest storage, WAL/tablet/retry state, and the database publisher in destruction order.
Catalog persistence, application-kind dispatch, and server service activation remain outside this
boundary. WAL removal is conservative and disabled unless the caller explicitly enables it.

Normal service remains unavailable while the storage owner performs:

1. open the durable database, `parts/`, and `manifest/` directories and acquire `manifest/LOCK`;
2. classify every manifest/part entry without following symlinks and reject malformed reserved or
   unrelated state;
3. enumerate consecutive final manifests, select the highest, exact-decode it, and reject rather
   than fall back on any failure;
4. require the configured database ID, WAL ID, and retained schema catalog to bind exactly;
5. verify every referenced final CSEG file, descriptor, contents, schema, and per-tablet relationship;
6. classify unreferenced valid final parts as retained orphans;
7. preflight the required WAL suffix from the checkpoint coordinate, including application support;
8. restore fresh tablet/part/retry state from the manifest and replay the suffix in global record
   order, verifying covered per-tablet no-ops;
9. remove recognized temporaries, synchronize each changed directory, and optionally remove still-
   present covered WAL segments; and
10. publish one complete recovered database descriptor and reopen the WAL writer at verified suffix
    end.

Physical verification, semantic preflight, and all part/schema checks complete before the first
logical replay callback. Recovery constructs fresh unpublished state; a failure discards it. Repeated
recovery over unchanged bytes selects the same generation, produces identical tablet/retry/row
state, and performs only idempotent cleanup.

Recognized temporary cleanup never promotes content. A cleanup sync failure prevents service even
if logical state was otherwise valid. Orphan final parts are observable in the recovery report and
remain untouched.

## Failure classification and observability

The implementation keeps these outcomes distinct:

- **incomplete:** valid short in-memory manifest/CSEG/WAL prefix, never a valid final installed file;
- **corruption:** checksum, reserved byte, malformed name/type, missing reference, contradictory
  descriptor/state, or required suffix failure;
- **unsupported:** checksum-valid future required version/flag/application semantic;
- **resource limit:** configured count/byte/working-memory or queue bound;
- **I/O failure:** open/read/write/sync/rename/remove/lock error; and
- **invalid context:** database/WAL/catalog/tablet/schema identity supplied by the caller does not
  match durable state.

Metrics and recovery reports include selected generation, manifest/tablet/part/retry counts, durable
and reclaim positions, referenced/orphan/temporary counts and bytes, installation attempts/failures,
file and directory sync counts/durations, flush queue depth/age, head-to-part rows/bytes, compression
ratio, checkpoint lag, retained WAL bytes/segments, and cleanup failures. No metric advances past a
durability/publication boundary that failed.

## Explicit non-scope

This design does not implement or authorize:

- installed part or manifest generation reclamation;
- retry horizon/pruning;
- compaction, input removal, delta parts, corrections, or tombstones;
- durable schema/catalog creation or migration;
- query execution, indexing, or version resolution;
- object storage, backups, encryption, or remote repair;
- network acknowledgments; or
- Raft snapshots, replicated checkpoints, or distributed manifest coordination.
