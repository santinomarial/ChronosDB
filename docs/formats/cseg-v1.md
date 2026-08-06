# ChronosDB CSEG v1

> **Status: accepted specification; constants, identity, layout, compression, metadata, PLAIN,
> stored-page, and structural part codecs implemented.** This document is the normative byte-level
> definition of one immutable CSEG v1 part. Bounded raw/Zstandard page compression and the
> canonical metadata
> directory plus standalone PLAIN/stored-page codecs are implemented together with canonical owned
> file composition and borrowed prefix/exact structural decoding. Bounded complete semantic and
> schema-binding validation plus schema-aware projected granule reading are also implemented; the
> inspector remains pending.
> [ADR 0016](../adr/0016-cseg-v1-layout-integrity-and-compression.md)
> accepts the layout, integrity, ordering, and compression decisions. Manifests, installation,
> flush orchestration, compaction, and reclamation are separate Phase 6 and Phase 7 contracts.

## Scope and normative language

CSEG v1 stores one nonempty, schema-bound, tablet-bound sequence of physical row versions in one
file. Rows are divided into granules. Every granule has one independently checksummed page for
every user and system column. The metadata prefix is independently checksummed and is sufficient
to establish exact page bounds before a page is decompressed or interpreted.

The terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. Ranges are half-open. All
offsets are byte offsets from the first magic byte. `MiB` and `GiB` mean powers of 1,024. Unless a
field says otherwise, integers are unsigned little-endian. Signed integers use two's-complement
little-endian bytes. Identifiers use 16 UUID network-order bytes and are compared bytewise.

CSEG v1 does not serialize a native C++ structure, pointer, enum representation, or platform-sized
integer. Reserved fields and alignment bytes are zero. Arithmetic is checked before use.

## Fixed constants and limits

| Name | Value |
| --- | ---: |
| File header size | 256 bytes |
| Column descriptor size | 96 bytes |
| Granule descriptor size | 64 bytes |
| Page descriptor size | 80 bytes |
| Metadata trailer size | 8 bytes: four zero bytes, then CRC32C |
| Alignment | 8 bytes |
| Format | major `1`, minor `0` |
| Maximum file length | 68,719,476,736 bytes (64 GiB) |
| Maximum user columns | 4,096 |
| System columns | 4 |
| Maximum rows per granule | 65,536 |
| Maximum granules | 1,048,576 |
| Maximum uncompressed page length | 67,108,864 bytes (64 MiB) |
| Maximum stored page length | 67,108,864 bytes (64 MiB) |
| Maximum Zstandard window size | 67,108,864 bytes (64 MiB) |
| Maximum row count | 68,719,476,736 (`2^36`) |

The exact page count is `granule_count * stored_column_count` and must fit `uint32`. A runtime may
apply lower writer or reader limits, but it cannot accept a noncanonical or oversized object and
call it CSEG v1.

## Canonical file layout

```text
256-byte file header
96-byte stored-column descriptor x stored_column_count
64-byte granule descriptor x granule_count
80-byte page descriptor x page_count
4 zero metadata-trailer bytes
4-byte metadata CRC32C
page 0 stored bytes and zero alignment
...
page N-1 stored bytes and zero alignment
```

`stored_column_count` is exactly `user_column_count + 4`. The four system columns follow the user
columns in their fixed order below. Page descriptors and page bodies are granule-major, then
stored-column-major. Page `g * stored_column_count + c` is the page for granule `g` and stored
column `c`.

The column, granule, and page descriptor arrays are contiguous with no gaps. The metadata trailer
makes `metadata_length` a multiple of eight. The first page begins at `metadata_length`. Every page
begins at the current eight-byte boundary and is followed by the minimum zero padding needed to
reach the next boundary. `total_length` is the end of the final page's alignment. Empty gaps,
aliases, overlaps, reordered pages, additional trailers, and trailing bytes are invalid.

The exact checked metadata length is:

```text
256 + 96 * stored_column_count + 64 * granule_count + 80 * page_count + 8
```

## File header

| Offset | Size | Field | CSEG v1 rule |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | Bytes `43 48 52 4e 43 53 45 47` (ASCII `CHRNCSEG`). |
| 8 | 2 | `format_major` | `1`. |
| 10 | 2 | `format_minor` | `0`. |
| 12 | 4 | `header_length` | `256`. |
| 16 | 4 | `file_flags` | `0`. |
| 20 | 4 | `reserved_0` | Zero. |
| 24 | 8 | `total_length` | Exact complete file length, a multiple of eight. |
| 32 | 8 | `metadata_length` | Exact offset of page 0 and end of the metadata trailer. |
| 40 | 8 | `row_count` | `1..2^36`, also exactly covered by granules. |
| 48 | 4 | `user_column_count` | `1..4096`. |
| 52 | 4 | `stored_column_count` | `user_column_count + 4`. |
| 56 | 4 | `granule_count` | Nonzero and within the fixed limit. |
| 60 | 4 | `page_count` | Exact checked product described above. |
| 64 | 16 | `part_id` | Nonzero opaque CSEG part identity. |
| 80 | 16 | `table_id` | Nonzero `TableId`. |
| 96 | 16 | `tablet_id` | Nonzero `TabletId`. |
| 112 | 16 | `schema_id` | Nonzero `SchemaId`. |
| 128 | 8 | `schema_version` | Positive version for `schema_id`. |
| 136 | 8 | `columns_offset` | `256`. |
| 144 | 8 | `granules_offset` | `columns_offset + 96 * stored_column_count`. |
| 152 | 8 | `pages_offset` | `granules_offset + 64 * granule_count`. |
| 160 | 8 | `page_data_offset` | Equal to `metadata_length`. |
| 168 | 4 | `event_time_column_ordinal` | User schema ordinal; names a non-null `TIMESTAMP_NS`. |
| 172 | 4 | `ordering_column_count` | `1..user_column_count`. |
| 176 | 8 | `minimum_event_time` | Exact minimum signed `TIMESTAMP_NS` value in the part. |
| 184 | 8 | `maximum_event_time` | Exact maximum signed `TIMESTAMP_NS` value in the part. |
| 192 | 56 | `reserved_1` | Zero. |
| 248 | 4 | `header_crc32c` | CRC32C of bytes `[0,248)`. |
| 252 | 4 | `reserved_2` | Zero. |

The header CRC is checked before any encoded count, length, or offset controls allocation or
iteration. A checksum-valid unknown nonzero major/minor version or required flag is unsupported. A
bad magic, zero major version, bad CRC, invalid identity, or contradictory known field is
corruption. Minor version zero is the assigned v1 value.

## Stored-column descriptors

| Relative offset | Size | Field | CSEG v1 rule |
| ---: | ---: | --- | --- |
| 0 | 16 | `column_id` | User `ColumnId`, or all zero for a system column. |
| 16 | 2 | `storage_kind` | Code from the registry below. |
| 18 | 2 | `logical_type` | Frozen logical type code from Columnar Batch v1. |
| 20 | 2 | `type_parameter_0` | Decimal precision; otherwise zero. |
| 22 | 2 | `type_parameter_1` | Decimal scale; otherwise zero. |
| 24 | 4 | `column_flags` | Bits defined below; all other bits zero. |
| 28 | 4 | `schema_ordinal` | Exact user ordinal, or `UINT32_MAX` for a system column. |
| 32 | 4 | `ordering_ordinal` | Physical-key ordinal, or `UINT32_MAX`. |
| 36 | 60 | `reserved` | Zero. |

Storage-kind codes are:

| Code | Meaning | Required logical type | Position and nullability |
| ---: | --- | --- | --- |
| 1 | `USER` | Descriptor's schema type | Schema ordinal; schema nullability |
| 2 | `WAL_ID` | `UUID` (18) | First system column; non-null |
| 3 | `RECORD_SEQUENCE` | `UINT64` (9) | Second system column; non-null |
| 4 | `ROW_ORDINAL` | `UINT32` (8) | Third system column; non-null |
| 5 | `OPERATION` | `UINT8` (6) | Fourth system column; non-null |

User descriptors occupy indices `0..user_column_count-1`, have unique nonzero `column_id` values,
`storage_kind = USER`, and `schema_ordinal` equal to their descriptor index. System descriptors
have a zero `column_id`, zero type parameters, `schema_ordinal = UINT32_MAX`, and occur exactly in
the registry order.

`column_flags` assigns bit 0 to `NULLABLE`, bit 1 to `EVENT_TIME`, and bit 2 to
`PHYSICAL_ORDERING`. Exactly the header-named user descriptor has `EVENT_TIME`; it is non-null and
has logical type `TIMESTAMP_NS`. Every physical ordering descriptor is a user column and has a
unique `ordering_ordinal` in `0..ordering_column_count-1`. Other descriptors use `UINT32_MAX`.
The event-time descriptor must participate in the physical ordering key, matching the v1 schema
contract. A metadata-checksum-valid unknown flag bit is unsupported, not silently ignored.

The four system columns preserve the current single-node physical row-version identity:

```text
(table_id, tablet_id, wal_id, record_sequence, row_ordinal)
```

`OPERATION` code `1` means `APPEND_ROWS`; no other CSEG v1 operation code is assigned. Correction
and tombstone storage require a later accepted contract and cannot be smuggled into a reserved
value.

## Granule descriptors and canonical boundaries

| Relative offset | Size | Field | CSEG v1 rule |
| ---: | ---: | --- | --- |
| 0 | 8 | `first_row` | Exact cumulative row count before this granule. |
| 8 | 4 | `row_count` | `1..65536`. |
| 12 | 4 | `page_count` | Equal to `stored_column_count`. |
| 16 | 8 | `first_page_index` | `granule_ordinal * stored_column_count`. |
| 24 | 8 | `minimum_event_time` | Exact signed minimum within this granule. |
| 32 | 8 | `maximum_event_time` | Exact signed maximum within this granule. |
| 40 | 24 | `reserved` | Zero. |

Granules exactly and contiguously cover `row_count`. The first has `first_row = 0`; each successor
starts at the previous end; the final end equals the header row count. Event-time minima and
maxima must agree with the decoded event-time page, and the header extrema must agree with the
granule extrema.

The canonical writer chooses each granule as the longest remaining row prefix of at most 65,536
rows for which every uncompressed page is at most 64 MiB. A single row that cannot satisfy the page
limit is rejected. This rule makes granule boundaries independent of compression ratio and
provider behavior.

## Page descriptors

| Relative offset | Size | Field | CSEG v1 rule |
| ---: | ---: | --- | --- |
| 0 | 4 | `granule_ordinal` | Owning granule index. |
| 4 | 4 | `stored_column_ordinal` | Stored-column descriptor index. |
| 8 | 2 | `physical_encoding` | `1` (`PLAIN`). |
| 10 | 2 | `compression` | Code from the compression registry. |
| 12 | 4 | `page_flags` | `0`. |
| 16 | 4 | `row_count` | Equal to the owning granule row count. |
| 20 | 4 | `null_count` | Exact null count; zero for system/non-null columns. |
| 24 | 8 | `page_offset` | Exact canonical aligned page start. |
| 32 | 8 | `stored_length` | Nonzero stored bytes, within the fixed limit. |
| 40 | 8 | `uncompressed_length` | Exact decoded payload length, within 64 MiB. |
| 48 | 8 | `validity_length` | Exact validity-buffer length. |
| 56 | 8 | `offsets_length` | Exact variable-offset-buffer length. |
| 64 | 8 | `values_length` | Exact values-buffer length. |
| 72 | 4 | `page_crc32c` | CRC32C of exactly the stored bytes. |
| 76 | 4 | `reserved` | Zero. |

The three uncompressed buffer lengths sum exactly to `uncompressed_length`. The complete page
descriptor is protected by the metadata CRC, including its sizes, interpretation codes, location,
and stored page checksum. The page CRC is verified before decompression. A
metadata-checksum-valid nonzero `page_flags` value is unsupported.

Physical encoding codes are:

| Code | Encoding |
| ---: | --- |
| 1 | `PLAIN` |

A checksum-valid unknown nonzero physical encoding is unsupported. Zero is corruption. New
encodings require an accepted specification because they change the meaning of uncompressed page
bytes.

Compression codes are:

| Code | Compression |
| ---: | --- |
| 1 | `NONE` |
| 2 | `ZSTD` |

For `NONE`, `stored_length == uncompressed_length` and stored bytes are the page payload. For
`ZSTD`, stored bytes contain exactly one standard Zstandard frame with no dictionary or skippable
frame. The frame content size must be present and equal `uncompressed_length`; the frame checksum
must be enabled. A conforming decoder supports both codes, rejects dictionary use and concatenated
or trailing frames, rejects a window larger than 64 MiB, bounds both window and advertised output
before provider allocation, supplies exactly the declared output capacity, and requires exact input
consumption and output length. A checksum-valid unknown nonzero compression code is unsupported;
zero is corruption.

The canonical writer uses single-threaded Zstandard level 3, enables content size and frame
checksum, and disables dictionary identifiers. Compression policy is an explicit writer input:
`NONE` stores every page raw; `ZSTD` attempts that canonical frame and stores it only when the
complete frame is smaller than the raw page, otherwise it uses `NONE`. A page marked `ZSTD`
therefore has `stored_length < uncompressed_length`. The same input rows, policy, provider version,
and implementation produce deterministic bytes. Provider upgrades must run compatibility fixtures
and may change newly written compressed bytes without changing their decoded values or the
validity of older parts.

## PLAIN page payload

After decompression, the page is exactly:

```text
validity bytes, if nullable
offsets bytes, if variable-width
values bytes
```

There is no internal alignment or padding. Buffer rules, logical type codes, value widths, bit
order, decimal representation, UUID byte order, UTF-8 requirements, floating bit preservation, and
null-slot zeroing are exactly those in [Columnar Batch v1](columnar-batch-v1.md). Each page is a
standalone slice for its granule:

- a nullable page has `ceil(row_count / 8)` validity bytes and exact `null_count`;
- a non-null page has no validity bytes;
- a variable page has `(row_count + 1) * 4` little-endian offsets beginning at zero and ending at
  `values_length`;
- fixed and Boolean pages have no offsets;
- null variable rows have equal adjacent offsets;
- unused validity/Boolean high bits and every null fixed/Boolean slot are zero; and
- all lengths are exact, including an empty variable values buffer.

Unlike Columnar Batch v1, an all-empty variable page still has nonempty offsets, so every CSEG page
has a nonzero uncompressed and stored representation.

The implemented in-memory PLAIN codec owns exactly the canonical encoded payload and decodes an
already checksum-verified, decompressed payload into a borrowed identity-free physical-column
view. It deliberately does not infer a schema identity or verify the page CRC: the complete-part
reader must establish descriptor integrity, verify the stored-page CRC, and decompress before
calling it. Physical validation is shared with Columnar Batch v1 so null counts, packed-bit
cleanliness, offsets, UTF-8, decimal bounds, and zeroed null slots cannot drift between formats.
The payload owner must outlive the decoded view and all cell views obtained from it.

The composed stored-page codec deterministically feeds that payload through the explicit raw or
Zstandard policy, computes the descriptor CRC32C over exactly the final stored bytes, and exposes
the complete descriptor metadata other than ordinals and the layout-derived file offset. Its
decoder verifies assigned compression and fixed lengths, then stored-byte CRC32C, then bounded
canonical decompression, and only then PLAIN physical interpretation. Raw results borrow their
stored input without allocation; Zstandard results own their bounded output, whose physical view
remains stable across moves. Thus the raw input owner must outlive a raw decoded page, while a
compressed decoded page is self-contained.

The structural part codec composes the authenticated metadata prefix and descriptor-ready stored
pages into one exact owned file image, inserting only the required zero alignment. Prefix decoding
first completes metadata authentication and directory validation, waits for exactly the declared
file prefix, and then validates every stored-page CRC, bounded decompression, complete PLAIN
payload, and following zero padding before returning a borrowed part view. Exact decoding also
rejects trailing bytes. This stage deliberately does not claim complete-part acceptance: catalog
binding, system-row semantics, event-time recomputation, and global physical ordering are the
separate validation stages required below.

The complete validator operates only on a structurally decoded part. It requires an explicit
working-memory ceiling, checks the aggregate uncompressed ordering/system pages for each granule
before semantic decompression, and retains only those pages plus one copied boundary row. It
rejects zero WAL identities and record sequences, distinguishes zero from unknown nonzero
operation codes, recomputes exact granule and header event-time extrema, and requires every
adjacent complete sort tuple to increase strictly across page and granule boundaries. Its
schema-bound entry point first applies the exact catalog binding below. Consequently successful
full validation implies every page was physically checked by structural decoding and every value
needed for v1 semantic acceptance was checked by the second stage.

System pages use the same PLAIN rules. `WAL_ID` slots contain nonzero 16-byte UUID network-order
values. `RECORD_SEQUENCE` values are positive. `ROW_ORDINAL` is zero-based within its source WAL
command. Every `OPERATION` byte is `1` in v1.

## Physical row order

Rows are sorted lexicographically ascending by:

1. user columns in `ordering_ordinal` order;
2. `WAL_ID` as 16 unsigned bytes;
3. `RECORD_SEQUENCE` as an unsigned integer; and
4. `ROW_ORDINAL` as an unsigned integer.

The system suffix makes the order total for valid row-version identities. Duplicate complete sort
tuples are invalid. User-key comparison is:

- null after every non-null value;
- `false` before `true`;
- signed, unsigned, decimal, timestamp, and date values by mathematical value;
- floating values by numeric ascending order, with `-0` and `+0` equal, every NaN after positive
  infinity, and all NaNs equivalent before the system suffix is considered;
- `SYMBOL` and `STRING` by lexicographic unsigned UTF-8 bytes;
- `BINARY` by lexicographic unsigned bytes; and
- `UUID` by lexicographic unsigned network-order bytes.

Sorting never changes stored floating bits or Unicode bytes. Page/granule boundaries do not reset
the comparison: the final row of one granule must be less than the first row of the next under the
complete tuple.

## Integrity and safe decoding

The metadata trailer is four zero bytes followed by the CRC32C of every byte in
`[0, metadata_length - 4)`. Its range includes the stored header CRC, all descriptors, and the four
zero trailer bytes, and excludes only the metadata CRC itself. Page CRCs use the same reflected
Castagnoli parameters as [WAL v1](wal-v1.md#primitive-encoding-and-crc32c). CRC32C detects
accidental corruption; it is not authentication.

A safe prefix decoder performs these stages:

1. require eight bytes before comparing magic, then require the complete 256-byte fixed header;
2. verify the header CRC before trusting counts, lengths, or offsets;
3. classify version/flag support, enforce fixed limits, and recompute every metadata offset and
   count with checked arithmetic;
4. if the declared metadata bytes are absent, report incomplete without reading beyond the supplied
   prefix;
5. verify the metadata CRC before interpreting any descriptor;
6. validate descriptor registries, identities, exact canonical ordering, page bounds, page
   non-overlap, and all cross-field relationships, then report incomplete if the canonical declared
   file bytes are absent;
7. verify each requested page CRC before decompressing it, enforce both page-size limits before
   provider entry, validate the complete PLAIN payload, and check the page's following zero
   alignment; and
8. for full validation, recompute event-time metadata and verify the global physical row order.

No encoded count, stored length, uncompressed length, or compression-frame field may cause an
allocation before its applicable configured limit and checked bound succeeds. Metadata decoding is
borrowed and allocation-free apart from implementation bookkeeping chosen within an explicit
runtime limit. Page decompression owns a bounded output buffer. A borrowed part or page view is
valid only while its complete encoded storage and any decoded page owner remain alive and
immutable.

Prefix decoding consumes exactly `total_length` bytes and may leave a following byte sequence to
its caller. Exact decoding additionally requires no trailing byte. Outcomes are distinct:

- **incomplete:** a valid prefix does not yet contain the fixed header, declared metadata, or
  complete declared file;
- **corruption/invalid:** bad magic or checksum, zero major version/code, noncanonical offset/order/
  padding, invalid known value, sort violation, impossible count/length, or internal contradiction;
  and
- **unsupported:** a checksum-valid unknown nonzero version, required flag, storage kind, logical
  type, physical encoding, compression, or operation code.

An unsupported page interpretation is reported before that page is decompressed. A full validator
must not return success after validating only projected pages; projection is a reader operation,
not complete-part acceptance. A projected granule read always validates its four system pages in
addition to requested user pages before exposing rows; it never skips operation or version
semantics merely because the query did not project them.

The in-memory projected reader borrows one complete immutable file image but touches only the
authenticated metadata at open. Opening a prefix reports the exact complete-file requirement and
exact opening rejects a suffix; neither operation reads, checksums, or decompresses page bodies.
Each granule read accepts unique destination-schema user ordinals in caller order, applies an
explicit aggregate decoded-buffer limit before page decoding or synthesized-buffer allocation,
and validates each selected page CRC, bounded PLAIN decoding, and following zero alignment. The
four system pages are always included and their nonzero identity/sequence and assigned operation
semantics are checked before any projected row is exposed. Corruption in an unrelated user page
does not block a projection that does not request that page; this selective result must never be
used as installation or complete-part validation evidence.

## Schema binding

Physical decoding is schema-independent. Before installation, replay coverage, or typed query use,
the caller resolves `(table_id, schema_id, schema_version)` and validates an exact binding:

- table identity, schema identity, version, and user column count match;
- every user descriptor matches schema ordinal, `ColumnId`, logical type parameters, and
  nullability;
- the event-time ordinal and physical-ordering flags/ordinals exactly match the schema roles; and
- the caller's target tablet equals the nonzero header `tablet_id`.

Names are not stored and do not participate in binding. An allowed rename therefore preserves
binding through `ColumnId`. A part never combines schema versions; nullable tail projection across
versions belongs to the reader/catalog layer. The projected reader obtains the exact ancestor-to-
descendant mapping from a validated `SchemaLineage`: existing ordinals borrow or own their decoded
source page according to compression, while added nullable tail ordinals become canonical all-null
physical vectors of the destination type. It performs no cast, default evaluation, or name lookup.

## Immutability, compatibility, and evidence

A CSEG file is a candidate until a later installation protocol validates, durably places, and
atomically references its `part_id`. Once installed, neither its bytes nor its logical contents may
change. Re-encoding, repair, compaction, and tiering create a new nonzero part identity.

The header, descriptor sizes, registries, system-column order, page payloads, sort semantics,
checksums, limits, and compatibility behavior are frozen for major 1/minor 0. An old reader rejects
unknown required semantics; it never guesses, skips a page, or reinterprets a code. A future format
that retains the eight-byte magic must retain the 256-byte outer header, version locations,
`header_length`, `file_flags`, `header_crc32c` location, and checksum range `[0,248)` so an older
reader can classify it as unsupported. A format that cannot retain that prefix uses a new container
magic and migration procedure.

Implementation acceptance requires independently reviewed raw and Zstandard golden fixtures,
cross-endian equality, schema-binding and sorted round trips for every logical type, deterministic
property tests, truncation/splice/reorder/bit-flip cases across every metadata and page region,
decompression-limit and frame-hostility tests, fuzzing, sanitizers, an inspector, and declared
encode/decode/selective-read/allocation/compression benchmarks. Fixture generation alone is not
independent evidence; expected bytes and checksum ranges require a separate review path.
