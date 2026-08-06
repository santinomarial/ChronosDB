# Canonical Columnar Memory Model and Batch Codec

> **Status: implemented foundation and codec.** The `chronos_columnar` target implements validated
> borrowed column views, immutable owned vectors, immutable schema-shaped owned batches, and the
> complete pure in-memory Columnar Batch v1 encoder/decoder. It deliberately does not implement WAL
> columnar commands, mutable heads, or publication.

## Purpose and authority

This library is the common in-memory boundary between future ingress builders and the implemented
columnar-batch encoder/decoder. Its canonical buffer and byte rules come directly from
[columnar-batch v1](../formats/columnar-batch-v1.md), while schema ordinal meaning comes from the
immutable [`chronos_schema`](schema-foundation.md) model. It adds no production dependency.

The CMake target is `chronos_columnar`, exported and installed as `chronos::columnar`. Its public
headers are:

- `chronos/columnar/column_vector.hpp` for bitmap sizing, safe cells, borrowed views, canonical
  buffer owners, and vector validation; and
- `chronos/columnar/columnar_batch.hpp` for configured limits and schema-pinned owned batches;
- `chronos/columnar/columnar_batch_format.hpp` for the frozen v1 format constants; and
- `chronos/columnar/columnar_batch_codec.hpp` for layout planning, exact owned encoding,
  prefix/exact borrowed decoding, and schema binding.

Factories return `common::Result<T>`. There is no constructible invalid vector or batch and there
are no mutating public accessors.

## Canonical buffers

The model retains the value regions in the same representation that a v1 byte codec will consume:

- nullable validity and BOOL values are packed LSB-first, with unused high bits zero;
- fixed-width numeric values are exact little-endian bytes, DECIMAL is signed 128-bit
  two's-complement least-significant-byte first, and UUID is its 16 network-order bytes;
- variable offsets are exact little-endian UINT32 bytes and values are concatenated row slices; and
- null fixed-width slots and null Boolean bits are zero, while null variable rows have equal
  adjacent offsets.

The vector model is a canonical *buffer* representation. The batch encoder adds the 96-byte header,
80-byte descriptors, aligned zero padding, and CRC trailer without changing those value bytes.

Keeping canonical bytes avoids unaligned native loads, native-endian assumptions, struct dumping,
and a second representation inside the codec. It also preserves every floating bit pattern,
including signed zero and NaN payloads. The cost is that typed consumers must explicitly interpret
the byte slice; the current row helper intentionally does not pretend a native object lives there.

## Borrowed and owned vectors

`ColumnVectorView::create` borrows three immutable spans: validity, offsets, and values. It checks:

1. a nonzero row count and exact nullable validity shape/null count;
2. canonical unused bits;
3. the exact fixed, Boolean, or variable buffer sizes using checked arithmetic;
4. zero null slots/bits;
5. first, monotonic, bounded, and final UINT32 offsets plus null-row equality;
6. independent Unicode scalar-value UTF-8 for every non-null STRING and SYMBOL slice; and
7. `abs(unscaled) < 10^precision` for every non-null DECIMAL without compiler-specific 128-bit
   arithmetic.

BINARY accepts every byte sequence. UUID column values may be nil because value-domain UUIDs differ
from nonzero catalog identities. FLOAT32/FLOAT64 accept all bit patterns. Empty valid variable
values and nulls remain distinct through validity.

A view allocates nothing on successful validation and does not own lifetime. The caller must keep
all buffers alive and immutable. `OwnedPhysicalColumn::create` validates supplied
`std::vector<byte>` buffers before moving them into an immutable identity-free owner. Phase 9 query
chunks use this owner for computed columns without inventing a durable `ColumnId`.
`OwnedColumnVector` composes the same physical owner with its real schema column identity.
`view()` borrows its owner, so moving or destroying the owner invalidates outstanding views and
byte-valued cells.

Physical-column, identified-vector, and batch owners are move-only. This makes expensive ownership
transfer explicit, prevents an unaccounted copy from acquiring a different allocation capacity,
and encourages queues/readers to retain one owner through `shared_ptr<const OwnedColumnarBatch>`
when shared lifetime is needed.

## Row inspection

`cell(row)` performs a checked row lookup. It returns one of null, Boolean, or borrowed bytes:

- Boolean is extracted from the packed value bitmap;
- a fixed-width result is the exact canonical slot; and
- a variable-width result is the exact bounds-checked slice selected by adjacent offsets.

Calling `boolean()` or `bytes()` for the wrong cell kind returns `INVALID_ARGUMENT`; an invalid row
or column ordinal returns `OUT_OF_RANGE`. This shape makes null handling explicit and never returns
a pointer produced by an unchecked offset.

## Schema-shaped batches and memory bounds

`OwnedColumnarBatch::create` pins a `shared_ptr<const TableSchema>` and requires exactly one vector
for every schema column in schema ordinal order. Column ID, type parameters, nullability, and row
count must match exactly. Because a valid schema is nonempty, the common batch row count is derived
from its first vector and remains nonzero.

`ColumnarBatchLimits` bounds rows, columns, logical canonical buffer bytes, and retained vector
capacity. The factory uses checked addition and rejects a limit breach with `RESOURCE_EXHAUSTED`
before retaining the supplied vectors. `buffer_bytes()` counts validity, offsets, and values sizes;
`retained_buffer_bytes()` counts their capacities so a caller cannot hide an oversized reserve
behind a short logical size. Both exclude fixed object/allocator bookkeeping and the encoded
header, descriptors, alignment, and CRC bytes. The layout planner separately proves with checked
arithmetic that the complete serialized length is within 16,776,992 bytes before output allocation.

The default buffer bound is the accepted maximum v1 embedded-batch length. An enclosing service may
use smaller values. A limit cannot be configured above the accepted v1 column or byte ceiling.

## Canonical layout and exact encoding

`plan_columnar_batch_v1_layout` starts after the checked descriptor-table end and places every
present validity, offsets, and values buffer in schema/descriptor order. Each present buffer begins
at the current eight-byte boundary and advances through checked end and alignment calculations.
Absent buffers are `(0,0)`. Four terminal zero bytes and the four-byte trailer complete a total
length divisible by eight. A result above the accepted embedded-batch maximum is rejected before
the encoder allocates output.

`encode_columnar_batch_v1` owns exactly one complete batch in `EncodedColumnarBatch`; no WAL or
application envelope is included. It zero-initializes the exact logical byte range, writes fields
individually in little endian, copies identifier network-order bytes and canonical vector buffers,
stores the header CRC32C over `[0,88)`, and finally stores the batch CRC32C over every byte before
the trailer. Encoding the same immutable batch is deterministic and preserves floating bit patterns.

## Borrowed physical decoding and schema binding

`decode_columnar_batch_v1_prefix` accepts a stream prefix and ignores bytes after the first complete
batch. `decode_columnar_batch_v1_exact` additionally rejects trailing bytes. Their explicit error
kind distinguishes:

- `INCOMPLETE`, with the currently known required size;
- `INVALID`, for corruption or a canonical contradiction;
- `UNSUPPORTED`, for a checksum-valid unknown version, flag, type, or encoding; and
- `RESOURCE_LIMIT`, when a caller's accepted configured bound is exceeded.

The decoder checks the 96-byte availability, magic, and header CRC before trusting lengths or
counts. It then validates frozen fields, format/configured bounds, and checked descriptor size;
requires the complete declared prefix; validates the batch CRC; and only then allocates bounded
descriptor metadata. Every offset must equal the one canonical cursor and every gap/padding byte
must be zero across the whole batch before value domains are interpreted and revalidated through
`ColumnVectorView::create`.

`DecodedColumnarBatchView` owns only its small vector of column-view objects. The encoded storage is
borrowed and must remain alive and immutable. Decoding proves physical safety without catalog
authority. `validate_columnar_batch_schema` is the separate exact binding step for table/schema
identity, version, column count, schema ordinal, column identity, type parameters, and nullability.
Routing, event-time policy, deduplication, and enclosing command checks remain outside the codec.

## Ownership, concurrency, and the mutable-head boundary

Successfully constructed owners never mutate their schema or buffers. Concurrent readers are safe
when an owner remains alive; the classes contain no synchronization and make no publication claim.
An ingress queue must transfer or retain the owner, never only a borrowed view into reactor scratch.

Packed validity and BOOL storage is safe here precisely because it is immutable after construction.
It must not be copied as the write-in-place representation of a future concurrently readable mutable
head: two logical rows can share one byte, which would create a C++ data race during publication.
The accepted mutable-head contract requires independently addressable unpublished storage or an
equivalent explicit race-free proof before compacting a sealed generation.

## Failure behavior and complexity

Malformed vector buffers, inconsistent schema shape, and invalid limits return `INVALID_ARGUMENT`.
Configured capacity breaches return `RESOURCE_EXHAUSTED`; row/ordinal mistakes return
`OUT_OF_RANGE`. Construction has no I/O or externally visible side effect, and a failed batch
factory does not retain its rvalue inputs.

For `R` rows, `B` buffer bytes, and `C` columns:

- bitmap/null/domain validation is `O(R)` per column;
- UTF-8 validation is `O(variable value bytes)`;
- owned-vector construction moves already allocated buffers in `O(1)` after validation;
- batch shape and accounting are `O(C)`; and
- layout and encoding are `O(C + B)`, with two CRC passes over the header/complete bytes as
  specified;
- decoding is `O(C + B + C × R)` in the worst case for structural, checksum, bitmap, offset,
  UTF-8, and decimal validation and allocates `O(C)` view metadata; and
- row inspection is `O(1)` plus the returned slice length only when a caller processes it.

There is no per-row owning allocation in the model. Variable data uses one offsets vector and one
values vector per column.

## Verification and remaining evidence

The `chronos_columnar_tests` target covers self-contained headers, every logical storage shape,
canonical null/high-bit rules, offset corruption, UTF-8/BINARY distinction, positive and negative
DECIMAL boundaries, safe row inspection, schema mismatch/order, exact accounting, and configured
bounds. Codec coverage adds a literal independently generated 400-byte golden fixture, exact
layout assertions, every truncation point, unaligned input, prefix/exact behavior, configured
limits, exact schema binding, every frozen type code, checksum-valid hostile structure/value
corruption, every one-byte fixture mutation, and fixed-seed generated round trips that compare all
buffers. Installation checks the codec headers and exported target; a separate consumer configures,
links, and exercises decoding against the staged package. An optional libFuzzer target drives both
prefix and exact decoders under sanitizer presets.

The ordinary warning-as-error, clang-tidy, ASan/UBSan, and TSan configurations apply through the
shared CMake helpers. Release microbenchmarks measure canonical encode and full physical decode for
64, 1,024, and 65,536 row batches and report both rows and encoded bytes processed; no result or
performance claim is published here. Future optimization evidence should vary type, null density,
variable-byte distribution, and schema width and record allocation/resource data under the
benchmark metadata contract. Mutable heads still need the separate release/acquire publication
proof, deterministic interleavings, and ThreadSanitizer.

## Likely review questions

**Why store offsets as bytes instead of `uint32_t` objects?** They are already the accepted canonical
little-endian representation. Byte access works for unaligned decoded storage and avoids native
endianness and object-lifetime assumptions.

**Why validate a borrowed view?** A future decoder can expose encoded regions without copying only
after their structural and value invariants are proven. A view factory gives ingress builders the
same oracle.

**Why does a null fixed slot have to be zero?** It eliminates multiple byte representations of the
same logical batch, stabilizing future CRC/digest bytes and making corruption tests deterministic.

**Why pin the schema?** Vector identity and ordinal meaning must remain available for the whole
batch lifetime even if a catalog lineage later advances.

**Does the default in-memory byte bound prove the batch can be serialized?** No. It bounds retained
canonical buffers. The layout planner separately adds and aligns the header, descriptors, buffers,
terminal padding, and CRC and enforces the complete format limit before encoding.
