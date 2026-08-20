# ChronosDB CSEG v2

> **Status: accepted temporal registry, checked layout, strict metadata/part codecs, schema binding,
> complete temporal row/order validation, and schema-aware projected granule reading are
> implemented; bounded single-lineage current/as-of winner resolution is implemented, while
> Manifest v2 installation and multi-source snapshot integration remain pending.**

CSEG v2.0 is the immutable temporal-history part format. It retains CSEG v1's eight-byte magic,
fixed 256-byte header, 96-byte column descriptors, 64-byte granule descriptors, 80-byte page
descriptors, metadata/header/page CRC32C coverage, eight-byte alignment, PLAIN physical encoding,
and NONE/Zstandard compression rules. Unless this document overrides a field, the [CSEG v1
specification](cseg-v1.md) remains normative for layout, bounds, checksums, pages, and compression.

## Version and counts

Header `format_major` is `2` and `format_minor` is `0`. A v1 reader must report this as unsupported
after validating the compatible header prefix; it must not inspect v2 descriptors as v1. The
header's `stored_column_count` is exactly `user_column_count + 8`, and every granule has that many
pages. Maximum user columns remain 4,096, so maximum stored columns are 4,104. Logical identities
are limited to 1,024 bytes in every encoder and decoder configuration.

The canonical layout planner uses the unchanged formulas:

```text
columns_offset = 256
granules_offset = columns_offset + stored_column_count * 96
pages_offset = granules_offset + granule_count * 64
page_count = granule_count * stored_column_count
metadata_length = pages_offset + page_count * 80 + 8
```

All arithmetic is checked and the metadata/file end remains canonically eight-byte aligned.

The metadata encoder and borrowed decoder implement the compatible header prefix, descriptor
tables, canonical offsets, metadata trailer, CRC32C coverage, runtime allocation limits, exact-file
and prefix modes, and schema binding. They require exactly major 2/minor 0 and the registry below.
The v1 entry points remain strict: they classify major 2 as unsupported and never reinterpret v2
descriptors.

The owned part composer and borrowed prefix/exact part decoder use the unchanged PLAIN and
NONE/Zstandard page encodings. The decoder authenticates and interprets every stored page, rejects
nonzero alignment padding and trailing bytes in exact mode, and reports the exact next required
size for valid truncations.

The canonical two-row raw temporal fixture freezes a 2,048-byte complete file and full-file CRC32C
`0x3242794c`. The test computes that fingerprint with a tableless reflected-Castagnoli oracle as
well as the production checksum routine, so changes to metadata, page order, stored bytes, or zero
padding are visible even when the file still round-trips through the same implementation.

An independently generated field-level fixture additionally freezes the 1,912-byte metadata
length, complete-file length, absolute header fields and identities, layout coordinates, all eight
temporal system descriptor cores and reserved regions, header CRC32C `0x2e0f2f20`, and metadata
CRC32C `0x5d84d7ac`. Its expected offsets and values are literal bytes derived from the specification
rather than the production format constants. A companion fixture pins every field and reserved
region in the canonical granule and all nine page descriptors, including exact aligned page
coordinates and variable-width buffer lengths, plus the zero metadata-trailer padding.

Checksum-valid hostile metadata tests distinguish assigned future format, storage, encoding,
compression, and flag values as unsupported. Zero registry codes, incorrect temporal system-column
shape, nonzero reserved bytes, contradictory stored-column/page counts, and overlapping page
coordinates are corruption after both header and metadata integrity are repaired.

For the canonical raw fixture, a one-bit mutation at every byte in the stored-page and alignment
padding region must fail exact part decoding as corruption. This sweep proves complete stored-byte
integrity and canonical zero padding; checksum-repaired semantic page mutations remain separate
from this integrity contract.

A checksum-repaired semantic matrix changes page bodies and then repairs both the stored-page and
metadata CRC32C fields. Exact part decoding must succeed before complete validation rejects zero or
unsupported commit sources, a zero source identity, a zero commit position, zero or unsupported
operations, and an empty logical identity with the specified corruption/unsupported classification.
This proves that integrity success is not treated as semantic validity.

Complete acceptance additionally binds the exact table schema/tablet, validates every system tuple,
distinguishes corrupt zero values from unsupported assigned-domain extensions, bounds nonempty
logical identities, recomputes granule and part event-time extrema, and requires strict ordering by
the schema physical key followed by `(commit_source, source_id, commit_position, row_ordinal)`
across pages and granules. It applies an explicit working-memory limit before semantic page
decompression. This does not yet provide selective/projected reading, current/as-of winner
resolution, or installation.

The projected reader authenticates metadata without touching page bodies, binds a retained schema
lineage, plans exact decoded/owned/borrowed bytes, reads only requested user pages, and always reads
and semantically validates all eight temporal system pages for a selected granule. Empty user
projections therefore still authenticate row identities and operations. V1 and v2 open entry points
remain version-strict. The reader exposes physical history rows; choosing current/as-of winners is a
higher-level query operation.

The query-layer reference resolver implements that higher-level operation for one explicit
authoritative source lineage. It requires a complete schema-order projection, filters by an
inclusive system commit-time boundary, selects one logical-identity winner by system commit time
then commit position, and removes a winning tombstone. It returns an owned scalar snapshot and
rejects mixed sources. Manifest-backed lineage discovery and vector-output integration remain
pending.

## Column registry

User columns retain storage kind 1 and all v1 schema/ordering rules. The required non-null system
suffix is:

| Ordinal | Storage kind | Meaning | Logical type |
| ---: | ---: | --- | --- |
| 0 | 2 | `COMMIT_SOURCE` | `UINT8` |
| 1 | 3 | `SOURCE_ID` | `UUID` |
| 2 | 4 | `COMMIT_POSITION` | `UINT64` |
| 3 | 5 | `ROW_ORDINAL` | `UINT32` |
| 4 | 6 | `OPERATION` | `UINT8` |
| 5 | 7 | `LOGICAL_IDENTITY` | `BINARY` |
| 6 | 8 | `RECEIVE_TIME` | `TIMESTAMP_NS` |
| 7 | 9 | `SYSTEM_COMMIT_TIME` | `TIMESTAMP_NS` |

System descriptors have zero `column_id`, zero type parameters and flags, and absent schema and
ordering ordinals. Event time remains one non-null schema user column and participates in the
physical ordering key.

## Row semantics

`COMMIT_SOURCE` 1 means WAL and 2 means Raft. `SOURCE_ID` is respectively the WAL UUID or stable
Raft group UUID. `COMMIT_POSITION` is respectively WAL record sequence or Raft log index and is
nonzero. `ROW_ORDINAL` disambiguates rows in one command. Together these fields form the physical
version identity and deterministic commit order within one source.

`OPERATION` values are original 1, correction 2, replacement 3, and tombstone 4. Zero and unknown
values are invalid or unsupported as classified by the complete decoder. `LOGICAL_IDENTITY` is
nonempty and at most 1,024 bytes; a configured decoder limit may be stricter but never larger. It
groups versions independently of physical source identity. `RECEIVE_TIME` records ingestion
receipt; `SYSTEM_COMMIT_TIME` records the application-selected wall time. Neither replaces
`COMMIT_POSITION` as authoritative order.

Rows remain sorted by the schema physical ordering key followed by the v2 physical source tuple.
Current/system-time readers must resolve the latest visible version per logical identity and remove
a winning tombstone. Timestamp ties are resolved by commit position. Cross-source comparison is
valid only inside an application snapshot that defines one authoritative tablet lineage; readers
must not invent an order between unrelated sources.

## Compatibility and installation

CSEG v1 bytes and operation semantics are immutable. Conversion writes a fresh v2 part identity and
never edits an installed v1 file. Manifest v1 does not admit v2 files. Manifest v2 must bind the
part's format, source lineage, durable application boundary, and exact installed bytes before any v2
part becomes authoritative or permits WAL/Raft-log reclamation.
