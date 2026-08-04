# Columnar Batch v1 Format

> **Status: accepted; canonical in-memory vectors and batches implemented, byte codec not
> implemented.** This is the normative byte specification for immutable schema-shaped ingestion
> batches. It does not change the frozen [WAL v1](wal-v1.md) framing.

## Scope and conventions

A batch contains one or more rows for exactly one table and immutable schema version. It contains
all user columns exactly once in schema ordinal order. Tablet identity, retry identity, WAL
position, durability mode, hidden system metadata, and operation outcome belong to the enclosing
[columnar append command](../architecture/columnar-ingestion.md#columnar-append-command-v1), not to
this format.

All integers are unsigned little-endian unless a value encoding says otherwise. Sizes and offsets
are byte counts from the first magic byte. Arithmetic is checked before use. Reserved fields and
padding are zero. Identifiers are 16-byte UUID network-order byte strings and must be nonzero. The
format never serializes a C++ object representation.

The v1 limits are:

- total length is a nonzero multiple of 8 and at most `16,776,992` bytes when embedded in the v1 WAL
  command;
- row count is `1..2^32-1` subject to the total-length limit;
- column count is `1..4096`; and
- every individual buffer and offset must lie entirely before the four-byte batch trailer.

An enclosing protocol may impose a smaller total length, row count, column count, or variable-value
limit. It may not accept a noncanonical encoding and relabel it v1.

## Physical layout

```text
96-byte batch header
80-byte column descriptor × column_count
column 0 buffers and alignment
...
column N-1 buffers and alignment
4 zero bytes of terminal padding
4-byte batch CRC32C trailer
```

The descriptor table ends on an eight-byte boundary. Each present buffer starts at the next current
eight-byte boundary and is followed by zero bytes up to the next eight-byte boundary. Buffers occur
strictly in descriptor order and, within one descriptor, in `validity`, `offsets`, `values` order.
Absent buffers have offset and length zero. After the final aligned buffer there are exactly four
zero bytes followed by the trailer, making `total_length` a multiple of eight. Empty gaps, aliases,
overlaps, reordered buffers, and trailing bytes are invalid.

### Batch header

| Offset | Size | Field | v1 rule |
| ---: | ---: | --- | --- |
| 0 | 8 | `magic` | Bytes `43 48 52 4e 43 42 31 00` (`CHRNCB1\0`). |
| 8 | 2 | `format_major` | `1`. |
| 10 | 2 | `format_minor` | `0`. |
| 12 | 4 | `header_length` | `96`. |
| 16 | 4 | `batch_flags` | `0`. |
| 20 | 4 | `row_count` | Nonzero. |
| 24 | 4 | `column_count` | `1..4096`. |
| 28 | 4 | `column_descriptor_length` | `80`. |
| 32 | 8 | `total_length` | Exact complete batch length. |
| 40 | 16 | `table_id` | Nonzero `TableId`. |
| 56 | 16 | `schema_id` | Nonzero `SchemaId`. |
| 72 | 8 | `schema_version` | Positive version for `schema_id`. |
| 80 | 8 | `descriptors_offset` | `96`. |
| 88 | 4 | `header_crc32c` | CRC32C of bytes `[0,88)`. |
| 92 | 4 | `reserved` | Zero. |

The header CRC is checked before trusting `total_length`, counts, or descriptor-table arithmetic.
The magic and fixed header bytes needed to locate the CRC are checked before computing it.

### Column descriptor

| Relative offset | Size | Field | v1 rule |
| ---: | ---: | --- | --- |
| 0 | 16 | `column_id` | Nonzero and unique in the batch. |
| 16 | 2 | `logical_type` | Code from the type registry below. |
| 18 | 2 | `physical_encoding` | `1` (`PLAIN`). |
| 20 | 2 | `type_parameter_0` | Decimal precision; otherwise zero. |
| 22 | 2 | `type_parameter_1` | Decimal scale; otherwise zero. |
| 24 | 4 | `column_flags` | Bit 0 is `NULLABLE`; all other bits zero. |
| 28 | 4 | `null_count` | Exact number of null rows. |
| 32 | 8 | `validity_offset` | Start of validity bytes, or zero when non-nullable. |
| 40 | 8 | `validity_length` | Exact validity length, or zero when non-nullable. |
| 48 | 8 | `offsets_offset` | Start of variable offsets, otherwise zero. |
| 56 | 8 | `offsets_length` | Exact variable-offset length, otherwise zero. |
| 64 | 8 | `values_offset` | Start of values, or zero iff `values_length` is zero. |
| 72 | 8 | `values_length` | Exact values length. |

A nullable column always has a validity bitmap of `ceil(row_count / 8)` bytes, including when
`null_count` is zero. Bit `i` is `(byte[i / 8] >> (i % 8)) & 1`; one means present and zero means
null. Unused high bits in the last byte are zero. A non-nullable column has no validity buffer and
`null_count = 0`.

## Logical type registry and value bytes

| Code | Type | Parameters | PLAIN values |
| ---: | --- | --- | --- |
| 1 | `BOOL` | zero, zero | `ceil(rows/8)` bytes, same LSB-first bit order as validity. |
| 2 | `INT8` | zero, zero | One two's-complement byte per row. |
| 3 | `INT16` | zero, zero | One little-endian two's-complement `int16` per row. |
| 4 | `INT32` | zero, zero | One little-endian two's-complement `int32` per row. |
| 5 | `INT64` | zero, zero | One little-endian two's-complement `int64` per row. |
| 6 | `UINT8` | zero, zero | One byte per row. |
| 7 | `UINT16` | zero, zero | One little-endian `uint16` per row. |
| 8 | `UINT32` | zero, zero | One little-endian `uint32` per row. |
| 9 | `UINT64` | zero, zero | One little-endian `uint64` per row. |
| 10 | `FLOAT32` | zero, zero | IEEE 754 binary32 bits as little-endian `uint32`. |
| 11 | `FLOAT64` | zero, zero | IEEE 754 binary64 bits as little-endian `uint64`. |
| 12 | `DECIMAL` | precision `1..38`, scale `0..precision` | Signed 128-bit two's-complement scaled integer, least-significant byte first. |
| 13 | `TIMESTAMP_NS` | zero, zero | Signed little-endian `int64` nanoseconds from Unix epoch UTC. |
| 14 | `DATE` | zero, zero | Signed little-endian `int32` days from `1970-01-01` in the proleptic Gregorian calendar. |
| 15 | `SYMBOL` | zero, zero | Variable-width valid UTF-8 bytes. No dictionary identifiers occur in v1 batches. |
| 16 | `STRING` | zero, zero | Variable-width valid UTF-8 bytes. |
| 17 | `BINARY` | zero, zero | Variable-width uninterpreted bytes. |
| 18 | `UUID` | zero, zero | Sixteen UUID network-order bytes per row. |

Fixed-width columns have no offsets buffer and a values length exactly `row_count * width`, except
`BOOL`, whose exact length is `ceil(row_count / 8)`. A null fixed-width slot is all zero; a null
Boolean bit is zero. Unused high Boolean bits are zero. Floating encodings preserve IEEE bit
patterns, including signed zero and NaN payloads. A non-null decimal scaled integer must satisfy
`abs(unscaled) < 10^precision`; the value is `unscaled × 10^-scale`.

`SYMBOL`, `STRING`, and `BINARY` have an offsets buffer of exactly `(row_count + 1) * 4` bytes
containing little-endian `uint32` offsets and a values buffer containing the concatenated data.
The first offset is zero, offsets are nondecreasing, and the last equals `values_length`. A null row
has equal adjacent offsets. Valid empty and null values are distinguished only by validity. Each
non-null `SYMBOL` or `STRING` slice is independently valid UTF-8; no normalization is implied. If
every slice is empty, `values_offset` and `values_length` are both zero and no values region occurs.

## Integrity and canonical validation order

The trailer is the CRC32C of every byte in `[0, total_length - 4)`, including the stored header CRC,
descriptors, buffers, and zero padding. CRC32C uses the same reflected Castagnoli parameters as
[WAL v1](wal-v1.md#checksums). The stored trailer itself is excluded.

A safe decoder performs these stages:

1. require at least 96 bytes and validate magic, fixed version-location fields, and header CRC;
2. validate supported version/flags, limits, exact total length, and checked descriptor-table size;
3. validate the batch CRC before interpreting column values;
4. validate every descriptor, type parameter, exact buffer length, canonical offset/order,
   non-overlap, zero padding, validity/null count, and variable offsets; and
5. validate value domains, including UTF-8 and decimal precision.

No count or encoded length may drive allocation before the applicable configured limit and all
checked bounds succeed. A borrowed decoded view is valid only while the complete encoded batch
storage remains alive and immutable.

Bad magic, CRC, length, offset, padding, duplicate identity, invalid known value, or internal
contradiction is corruption/invalid input. A checksum-valid unknown major/minor version, required
flag, logical type, or physical encoding is unsupported, not silently skipped. An enclosing WAL
record containing either condition cannot be partially applied.

## Schema validation

Independent decoding proves byte safety and yields identities, types, nullability, and values. It
does not authorize the schema. Before WAL admission or replay application, the caller resolves
`(table_id, schema_id, schema_version)` and requires:

- an exact match of column count, ordinal `ColumnId`, type, parameters, and nullability;
- every non-null constraint, event-time domain, routing rule, and configured size limit;
- every row routes to the enclosing command's one `TabletId`; and
- no duplicate logical deduplication key occurs within an `APPEND_ROWS` batch.

Catalog state used for recovery must itself be durable and available before application replay;
its storage format is outside this specification.

## Compatibility and golden evidence

The header, descriptors, type codes, bit order, canonical padding, CRC ranges, and value encodings
are frozen for major 1/minor 0. Extensions use a new accepted version or encoding code and must state
whether old readers reject or can skip them. Implementations require byte-for-byte golden fixtures,
cross-endian equality, boundary/property tests, hostile corruption tests, and fuzzing before this
format is writable in production paths.
