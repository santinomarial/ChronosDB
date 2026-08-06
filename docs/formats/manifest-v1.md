# ChronosDB Manifest v1

> **Status: accepted specification; implementation pending.** This document is the normative
> byte-level and directory contract for one single-node Manifest v1 generation, installed CSEG
> part names, and the checkpoint coordinate used to recover a WAL suffix. The crash ordering and
> ownership model are specified in
> [manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
> and accepted by [ADR 0017](../adr/0017-manifest-generations-installation-and-checkpoints.md).

## Scope

Manifest v1 is a database-wide immutable snapshot of:

- installed CSEG parts grouped by tablet;
- the schema and WAL application boundary from which each tablet can resume recovery;
- committed `COLUMNAR_APPEND` retry outcomes required to preserve idempotency; and
- one global physical WAL coordinate through which the manifest proves complete recovery coverage.

It does not encode a mutable head, CSEG bytes, a schema catalog, a compaction edit, a retry-pruning
horizon, a query snapshot, a subscription checkpoint, or a Raft snapshot. Phase 6 initially writes
full manifest snapshots; a **version edit** is the logical transition between two snapshots, not a
separate durable record stream.

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. Ranges are half-open. All
integers are unsigned little-endian unless explicitly signed. Identifiers use their exact 16 UUID
network-order bytes and are compared bytewise. Arithmetic is checked before allocation, I/O, or
subspan formation. Native C++ object representations are never serialized.

## Database storage directories

An already durably created database root contains two Phase 6 subdirectories:

```text
parts/
manifest/
```

The creation protocol synchronizes the database-root directory after each required subdirectory
name exists and before either directory can establish a `LOCAL_SYNC` durability claim. All
operations below are relative to already opened directory descriptors; basenames cannot contain a
slash, `.` component, or `..` component.

### Installed part names

An installed CSEG part is named exactly:

```text
part-<part-id>.cseg
```

`<part-id>` is the `PartId` formatted as exactly 32 lowercase hexadecimal digits in durable byte
order. The filename identity MUST equal the nonzero CSEG header `part_id`. A final name is a regular
file, is installed with an atomic no-replace rename, and is never modified or replaced.

A candidate part has the exact temporary grammar:

```text
.part-<part-id>.cseg.tmp-<nonce>
```

`<nonce>` is exactly 32 lowercase hexadecimal digits and has no durable meaning. A temporary name
is never an installed part and is never promoted by recovery. Under the manifest writer lock,
startup MAY remove recognized temporary files and synchronize `parts/`. An unreferenced valid final
part is retained and reported as an orphan in v1; automatic final-part deletion is not part of this
format.

### Manifest names

One installed manifest generation is named exactly:

```text
manifest-<generation>.cman
```

`<generation>` is exactly 20 ASCII decimal digits with leading zeroes and is in
`1..18446744073709551615`. It MUST equal the generation encoded in the file. A candidate uses:

```text
.manifest-<generation>.cman.tmp-<nonce>
```

with the same 32-lowercase-hex nonce grammar. `manifest/LOCK` is a regular advisory-lock file whose
bytes have no durable meaning. Final and temporary manifest entries are regular files. Symlinks,
directories, malformed reserved names, and unrelated entries fail closed.

Final manifest generations begin at 1 and are consecutive through the highest final name. Manifest
v1 does not delete prior generations. Recovery removes recognized temporaries only after acquiring
`manifest/LOCK`; it never fills a missing generation from a temporary. The selected generation is
the highest final name. If it is incomplete, corrupt, unsupported, or references an invalid/missing
part, recovery fails and MUST NOT fall back to an older generation.

The no-fallback rule makes corruption visible and prevents rollback after a newer checkpoint has
made an older WAL prefix reclaimable. A crash before a final manifest name persists yields the old
highest generation; a crash image containing the complete new final name may select it because all
referenced parts crossed their durability boundaries first.

## Fixed constants and limits

| Name | Value |
| --- | ---: |
| Magic | bytes `43 48 52 4e 4d 46 53 54` (`CHRNMFST`) |
| Format major/minor | `1 / 0` |
| Header length | 256 bytes |
| Tablet descriptor length | 96 bytes |
| Part descriptor length | 128 bytes |
| Retry descriptor length | 128 bytes |
| Trailer padding | 4 zero bytes |
| File CRC32C | 4 bytes |
| Alignment | 8 bytes |
| Maximum file length | 1,073,741,824 bytes (1 GiB) |

The maximum tablet, part, and retry count is independently `8,388,605`, but the combined canonical
layout MUST fit the maximum file length. Runtime decode limits MAY be lower and produce a resource-
limit outcome rather than reinterpret valid bytes.

## File layout

The exact order is:

```text
256-byte header
tablet descriptors
part descriptors
retry descriptors
4 zero trailer bytes
4-byte file CRC32C
```

All descriptor sizes are multiples of eight, so no inter-section padding exists. The file length is
always a multiple of eight.

### Header

| Offset | Size | Field | Manifest v1 rule |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | Exact `CHRNMFST` bytes. |
| 8 | 2 | `format_major` | `1`. Zero is corruption; another nonzero value is unsupported. |
| 10 | 2 | `format_minor` | `0`; another value is unsupported. |
| 12 | 4 | `header_length` | `256`. |
| 16 | 4 | `file_flags` | Zero; any bit is unsupported. |
| 20 | 4 | `reserved_0` | Zero. |
| 24 | 8 | `total_length` | Exact complete file length, at most 1 GiB. |
| 32 | 8 | `generation` | Nonzero and equal to the final filename generation. |
| 40 | 8 | `previous_generation` | Zero for generation 1; otherwise `generation - 1`. |
| 48 | 8 | `tablet_count` | Number of 96-byte tablet descriptors. |
| 56 | 8 | `part_count` | Number of 128-byte part descriptors. |
| 64 | 8 | `retry_count` | Number of 128-byte retry descriptors. |
| 72 | 16 | `database_id` | Nonzero stable identity for this database storage history. |
| 88 | 16 | `wal_id` | Nonzero WAL v1 identity protected by this manifest history. |
| 104 | 8 | `reclaim_record_sequence` | Global completely covered WAL prefix; zero means no record. |
| 112 | 8 | `reclaim_segment_number` | Segment containing the reclaim coordinate. |
| 120 | 8 | `reclaim_byte_offset` | End offset immediately after `reclaim_record_sequence`. |
| 128 | 8 | `tablets_offset` | Canonically `256`. |
| 136 | 8 | `parts_offset` | Exact end of tablet descriptors. |
| 144 | 8 | `retries_offset` | Exact end of part descriptors. |
| 152 | 8 | `trailer_offset` | Exact end of retry descriptors. |
| 160 | 88 | `reserved_1` | Zero. |
| 248 | 4 | `header_crc32c` | CRC32C of bytes `[0, 248)`. |
| 252 | 4 | `reserved_2` | Zero. |

The canonical offsets are calculated with checked arithmetic:

```text
tablets_offset = 256
parts_offset   = tablets_offset + tablet_count * 96
retries_offset = parts_offset + part_count * 128
trailer_offset = retries_offset + retry_count * 128
total_length   = trailer_offset + 8
```

The early header CRC is validated before counts, offsets, or total length can direct allocation or
locate the complete-file checksum.

### Checkpoint coordinate

For `reclaim_record_sequence == 0`, the coordinate is exactly segment 1, byte offset 64: the end of
the initial segment header before any record. For a nonzero sequence, the segment number is nonzero
and the byte offset is the eight-byte-aligned end immediately after that complete WAL record,
between 64 and the WAL v1 64 MiB segment limit inclusive.

The coordinate is meaningful only with the header `wal_id`. It asserts that every application
record through that sequence has a recoverable effect in this manifest: its target tablet boundary
and parts represent the rows, its retained retry outcome represents idempotency, and an exact
duplicate represents no additional rows. Publication code proves that relationship before the
manifest durability boundary. The coordinate is permission to ignore or later remove only the
covered WAL prefix; it is not permission to delete an active segment or any required suffix.

## Tablet descriptor

Tablet descriptors are sorted by `tablet_id` durable bytes with no duplicate. One descriptor is:

| Relative offset | Size | Field | Manifest v1 rule |
| ---: | ---: | --- | --- |
| 0 | 16 | `table_id` | Nonzero table identity. |
| 16 | 16 | `tablet_id` | Nonzero tablet identity. |
| 32 | 16 | `recovery_schema_id` | Exact schema active at the durable tablet boundary. |
| 48 | 8 | `recovery_schema_version` | Nonzero version matching the retained catalog schema. |
| 56 | 8 | `durable_record_sequence` | Highest record for this tablet represented by the manifest; zero is allowed before its first mutation. |
| 64 | 8 | `first_part_index` | First index in the global part descriptor array. |
| 72 | 8 | `part_count` | Consecutive parts owned by this tablet. |
| 80 | 8 | `durable_row_count` | Sum of the referenced part row counts for this tablet. |
| 88 | 4 | `tablet_flags` | Zero; any bit is unsupported. |
| 92 | 4 | `reserved` | Zero. |

The part ranges are consecutive, nonoverlapping, and cover the global part array exactly in tablet
descriptor order. Parts within one range are sorted by `part_id` durable bytes. Empty ranges are
canonical and use the next global part index. `durable_record_sequence` is a recovery/application
boundary, not a promise that every global WAL sequence targeted this tablet.

The recovery schema is the schema at this durable boundary, not necessarily the schema of a newer
uncheckpointed live head. WAL suffix replay performs accepted direct-successor transitions from
this schema. Catalog names and schema definitions remain external and must bind by exact identity
and version before state is exposed.

## Part descriptor

| Relative offset | Size | Field | Manifest v1 rule |
| ---: | ---: | --- | --- |
| 0 | 16 | `part_id` | Nonzero and equal to filename and CSEG header identity. |
| 16 | 16 | `table_id` | Equal to the owning tablet descriptor and CSEG header. |
| 32 | 16 | `tablet_id` | Equal to the owning tablet descriptor and CSEG header. |
| 48 | 16 | `schema_id` | Equal to the CSEG header. |
| 64 | 8 | `schema_version` | Nonzero and equal to the CSEG header. |
| 72 | 8 | `file_length` | Exact installed CSEG length. |
| 80 | 8 | `row_count` | Nonzero and equal to the CSEG header. |
| 88 | 8 | `minimum_record_sequence` | Minimum system `RECORD_SEQUENCE` value in the part. |
| 96 | 8 | `maximum_record_sequence` | Maximum system `RECORD_SEQUENCE` value in the part. |
| 104 | 8 | `minimum_event_time` | Signed two's-complement value equal to CSEG metadata. |
| 112 | 8 | `maximum_event_time` | Signed two's-complement value equal to CSEG metadata. |
| 120 | 4 | `part_flags` | Zero; any bit is unsupported. |
| 124 | 4 | `reserved` | Zero. |

Record-sequence extrema are nonzero, ordered, use the manifest `wal_id` for every row, and do not
exceed the owning tablet's durable boundary. Installation performs exact CSEG structural/content/
schema validation and recomputes the record-sequence extrema before this descriptor can be
published. `durable_row_count` and all duplicate fields are checked against installed bytes.

Manifest v1 has no base/delta/compaction classification flag. Assigning one is a future compatible
minor/flag decision only after its semantics are accepted; readers reject unknown required bits.

## Retry descriptor

Retry descriptors are sorted by `(client_id, client_batch_id)` durable bytes with no duplicate.
They preserve committed outcomes whose WAL commands are covered by a tablet durable boundary.

| Relative offset | Size | Field | Manifest v1 rule |
| ---: | ---: | --- | --- |
| 0 | 16 | `client_id` | Nonzero nominal client identity. |
| 16 | 16 | `client_batch_id` | Nonzero nominal batch identity. |
| 32 | 16 | `table_id` | Mutation table identity. |
| 48 | 16 | `tablet_id` | Mutation tablet identity and an existing tablet descriptor. |
| 64 | 32 | `request_digest` | Exact canonical `COLUMNAR_APPEND` SHA-256 mutation digest. |
| 96 | 16 | `wal_id` | Equal to the manifest header `wal_id`. |
| 112 | 8 | `record_sequence` | Original applied outcome sequence, nonzero and no later than the tablet durable boundary. |
| 120 | 4 | `applied_row_count` | Exact nonzero original batch row count. |
| 124 | 4 | `retry_flags` | Zero; any bit is unsupported. |

Manifest v1 does not prune retry descriptors. A configured bound causes pre-WAL backpressure before
the manifest cannot preserve another protected identity. Durable pruning requires a separately
accepted idempotency-horizon and retention protocol; omission is never inferred from memory pressure.

## Complete-file integrity

Bytes `[trailer_offset, trailer_offset + 4)` are zero. The little-endian CRC32C at
`[total_length - 4, total_length)` covers every byte `[0, total_length - 4)`, including the header
CRC and zero trailer padding. A decoder validates the header CRC before trusting layout fields,
then requires the exact canonical length and validates the complete CRC before interpreting any
descriptor. Exact decoding rejects a suffix; prefix decoding returns one borrowed manifest and its
exact consumed length.

CRC32C detects accidental corruption and is not authentication. The database directory remains an
operator-controlled trusted namespace, but malformed bytes still fail safely and within limits.

## State-transition validation

An encoded manifest proves one self-consistent snapshot. Publishing generation `N + 1` additionally
compares it with selected generation `N`:

- database and WAL identities are unchanged;
- generation and previous-generation fields advance exactly once;
- tablet durable boundaries, global reclaim sequence, and physical reclaim coordinate never move
  backward;
- a tablet's table identity never changes and schema movement follows the retained catalog lineage;
- Phase 6 additions never remove or replace an installed part;
- retry descriptors are a superset because pruning is not yet defined; and
- every new/retained part and retry descriptor satisfies the new tablet boundary.

Part removal/replacement belongs to compaction and requires the later Phase 7 transition validator;
the v1 byte format can describe the resulting full snapshot, but Phase 6 APIs reject such edits.

## WAL suffix recovery and reclamation

Manifest-aware recovery starts from the selected generation, validates every referenced part and
restores tablet/retry state, then processes WAL records strictly after the global reclaim coordinate.
For a record whose sequence is no later than its target tablet's durable boundary, replay verifies
the retained matching retry outcome and adds no rows. Later records apply normally in global order.

The WAL directory may contain any subset of wholly covered closed prefix segments or may have
removed them after a durable manifest checkpoint. Missing/gaps entirely before the coordinate do
not define suffix continuity; any present covered file remains subject to name/type/identity checks
before optional cleanup. The first required retained bytes are either:

- the coordinate segment, whose header matches `wal_id` and whose bytes at
  `reclaim_byte_offset` begin record `reclaim_record_sequence + 1` (or reach clean segment end); or
- its immediate successor when the coordinate segment was completely covered and removed, whose
  header `first_record_sequence` is exactly `reclaim_record_sequence + 1`.

Every later final segment remains consecutive and follows WAL v1 framing. Missing required suffix,
identity mismatch, sequence gap, corruption, or unsupported semantics fails before application.
Covered closed segments may be deleted only after the selecting manifest directory sync. Deletion
is followed by WAL-directory sync; a crash may leave extra covered files but never permits a
required suffix to be absent. The active highest segment is never removed.

This rule changes no WAL v1 segment or record byte. It supplies the external checkpoint context
that WAL v1 deliberately deferred.

## Compatibility and evidence

Manifest major 1/minor 0, the compatibility prefix, sizes, offsets, registries, checksum ranges,
ordering, and filename grammars are frozen once a final generation is installed. Unknown nonzero
major/minor/required flags are unsupported; zero identities/codes, dirty reserved bytes, malformed
cross-fields, checksum failures, and impossible state are corruption. A valid short prefix is
incomplete with the exact next required size. Resource limits are reported separately.

Acceptance requires an independently reviewed golden, deterministic properties, every-boundary
truncation, bit flips, splices, reordered/duplicate descriptors, hostile counts/offsets, unknown
versions/flags, fuzzing, sanitizers, static analysis, self-contained public headers, installation
and external-consumer coverage, and codec/state-transition benchmarks. Filesystem implementation
additionally requires process-crash evidence at every write/readback/sync/rename/directory-sync/
checkpoint/removal boundary.
