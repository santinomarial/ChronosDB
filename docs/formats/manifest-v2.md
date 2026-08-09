# ChronosDB Manifest v2

> **Status: accepted source-neutral registry, checked canonical layout, strict checksummed byte
> codec, exact single-part CSEG admission, and add-only generation transition validation are
> implemented. Complete referenced-part coverage and crash-ordered local filesystem installation
> are also implemented. Highest-generation fail-closed local recovery selection and
> generation-pinned temporal part loading for bounded current/as-of resolution are implemented;
> application replay/publication, v1 migration, authorized retention/compaction, and reclamation
> integration remain pending.**

Manifest v2 is the immutable database storage generation that can authorize CSEG v2 temporal parts
whose authoritative application source is WAL or Raft. It retains Manifest v1's magic, 256-byte
header envelope, generation naming/selection, one-GiB limit, full-file CRC32C trailer, descriptor
sort order, immutable-generation installation, and no-fallback recovery rule. Unless overridden
here, [Manifest v1](manifest-v1.md) remains normative.

## Header and layout

Header format major/minor is `2/0`. Tablet, part, and retry descriptors are respectively 128, 224,
and 144 bytes. Canonical offsets use those sizes and the unchanged eight-byte trailer. Every
arithmetic operation is checked, every descriptor table is contiguous, and the total length is
eight-byte aligned and no greater than one GiB.

The owned encoder and borrowed prefix/exact decoder implement canonical descriptor bytes, the early
header CRC32C and complete-file CRC32C, exact truncation requirements, runtime allocation limits,
reserved/flag validation, and corrupt-versus-unsupported registry classification. V1 and v2 entry
points are version-strict and never reinterpret each other's descriptor tables.

Header fields through `database_id` and descriptor offsets retain their v1 offsets. File flag bit 0
is `HAS_WAL_RECLAIM_CHECKPOINT`; all other bits are unsupported. With the bit set, bytes 88–127
retain the v1 `wal_id`, record sequence, segment number, and byte offset meanings and prove one
database-wide removable WAL prefix. Without it, those bytes are zero. This optional global
coordinate remains necessary because independently advanced WAL tablets cannot authorize deletion
of gaps that targeted another tablet. Raft reclamation is per tablet.

## Tablet descriptor

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | table ID |
| 16 | 16 | tablet ID |
| 32 | 16 | recovery schema ID |
| 48 | 8 | recovery schema version |
| 56 | 16 | source ID: WAL ID or Raft group UUID |
| 72 | 8 | durable applied position |
| 80 | 8 | reclaim position |
| 88 | 8 | first part index |
| 96 | 8 | part count |
| 104 | 8 | durable physical version count |
| 112 | 1 | commit source: WAL 1 or Raft 2 |
| 113 | 3 | zero |
| 116 | 4 | flags, currently zero |
| 120 | 8 | zero |

The source ID is nonzero and the durable position is the highest application command completely
represented by this tablet snapshot. Source lineage cannot change inside a tablet history without
an accepted migration. For WAL tablets, `reclaim_position` is zero and the optional global header
coordinate controls deletion. For Raft tablets it is no greater than the durable position and is
the installed application-snapshot boundary through which retained log entries may be discarded.
It is zero until such a snapshot is durably installed. The version count is the exact sum of part
row counts and includes corrections and tombstones; it is not a current-visible row count.

## Part descriptor

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 64 | part, table, tablet, and schema UUIDs |
| 64 | 8 | schema version |
| 72 | 8 | exact file length |
| 80 | 8 | physical row/version count |
| 88 | 8 | minimum commit position |
| 96 | 8 | maximum commit position |
| 104 | 8 | minimum event time |
| 112 | 8 | maximum event time |
| 120 | 8 | minimum system commit time |
| 128 | 8 | maximum system commit time |
| 136 | 16 | source ID |
| 152 | 32 | SHA-256 of exact installed CSEG bytes |
| 184 | 2 | CSEG format major |
| 186 | 2 | CSEG format minor |
| 188 | 1 | commit source |
| 189 | 3 | zero |
| 192 | 4 | flags, currently zero |
| 196 | 28 | zero |

Identity/schema/length/count/time fields bind the exact validated CSEG image. The content digest
prevents an object or local file with the same part identity but different bytes from satisfying a
descriptor; CSEG checksums still provide internal framing and page corruption detection. Version
2.0 currently admits only CSEG `2/0`; the explicit version fields make that rejection auditable and
leave future format assignment unambiguous. Existing CSEG v1 parts require conversion to fresh v2
part identities because they do not carry logical identity or system commit time. Every v2 row must
match the descriptor and owning tablet source. Commit extrema are nonzero, ordered, and no greater
than the tablet durable position. System and event extrema are ordered and recomputed from decoded
pages before admission.

## Retry descriptor

The first 96 bytes retain v1 client/batch/table/tablet/request-digest fields. Bytes 96–111 are the
nonzero source ID, 112–119 the nonzero commit position, 120–123 the applied row count, byte 124 the
commit source, bytes 125–127 zero, bytes 128–131 flags (zero), and bytes 132–143 zero. Retry source
must match the target tablet and its position cannot exceed the durable applied boundary.

## Installation, transition, and recovery boundary

V2 preserves v1's part-before-manifest sync/rename/directory-sync order and immutable snapshot
publication. A byte codec alone cannot authorize visibility. Complete admission must exact-decode
the declared CSEG version, bind schema/tablet/source and digest, validate temporal semantics, and
recompute all extrema. Transitions are monotonic within each source lineage; they cannot regress an
application/reclaim boundary, mutate an installed descriptor, or remove history without the
accepted compaction/retention proof.

`describe_manifest_v2_temporal_part_image()` implements that exact single-image boundary for CSEG
`2/0`: it structurally and semantically validates the complete image, binds the supplied physical
schema and tablet, requires every row to share the tablet's WAL/Raft source lineage, hashes all
bytes with SHA-256, and derives commit/event/system extrema. The companion validation entry point
requires byte-for-byte descriptor equality with that derived result and checks the owning tablet's
durable boundary. The complete-generation validator composes that boundary across exactly one image
per descriptor in canonical order, canonical part filenames, exact retained-schema coverage, and
each owning tablet. The locked local filesystem owner applies that proof before mutation,
revalidates exact temporary-file readback, synchronizes the file, renames without replacement, and
synchronizes the containing directory for both CSEG v2 parts and add-only Manifest v2 successors.
It streams referenced-part validation rather than retaining every CSEG image simultaneously.

The implemented ordinary transition validator requires an exact successor generation and database
identity, retained schema bindings, immutable tablet WAL/Raft source lineage, monotonic application
and reclaim boundaries, forward-only recovery schemas, exact preservation of every prior part, and
exact preservation of every protected retry outcome. Fresh parts and retries on an existing tablet
must begin strictly after its predecessor durable boundary. It permits new tablets, parts, and
retries that satisfy those rules and the codec model. It deliberately cannot authorize part
removal: temporal compaction or retention needs an independent history-equivalence/expiry proof
before a specialized transition may remove any version.

Startup selects the highest final generation and fails closed rather than falling back. WAL suffix
recovery begins after the optional global coordinate. Each Raft tablet rebuilds or installs state at
its durable application snapshot before replaying committed entries after its reclaim position.
Neither coordinate permits deletion until its exact manifest generation is durable and the
corresponding application state is recoverable.

The v2 installer requires the selected predecessor to already be Manifest v2. It does not
reinterpret or automatically migrate a v1 predecessor. The local recovery loader selects only the
highest consecutive final generation, exact-decodes without fallback, binds the expected database,
retained schemas, and exact per-tablet WAL/Raft owners, and streams full validation over every
referenced CSEG v2 object before returning an owning unpublished generation. Exact part-image loads
retain that generation, reprove the requested descriptor and CSEG bytes, and can feed the bounded
tablet resolver. The resolver conservatively prunes parts whose minimum system time is later than
the requested boundary, then validates and decodes every candidate before applying exact row-level
winner rules. A boundary before all retained versions returns `NOT_FOUND`; it is not represented as
a proven empty table. A complete pinned part set can also be decoded into canonical cross-part
version order and atomically seed a fresh temporal provider when the caller supplies an independent
tablet-wide retained-system-time proof. Application suffix replay, Raft snapshot reconstruction,
and complete query-epoch publication remain pending.
