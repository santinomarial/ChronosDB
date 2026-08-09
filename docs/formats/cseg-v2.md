# ChronosDB CSEG v2

> **Status: accepted temporal registry, checked layout, strict metadata codec, and complete
> structural part composition/decoding are implemented; temporal row/order validation, projected
> reading, and Manifest v2 installation remain pending.**

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
size for valid truncations. This structural acceptance does not yet validate temporal value
domains, physical temporal ordering, event-time extrema, catalog lineage, or installation.

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
