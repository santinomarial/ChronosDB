# Canonical Columnar Memory Model

> **Status: implemented foundation.** The `chronos_columnar` target implements validated borrowed
> column views, immutable owned vectors, and immutable schema-shaped owned batches. It deliberately
> does not implement the columnar-batch v1 header, descriptors, padding, CRC, byte encoder/decoder,
> WAL columnar commands, mutable heads, or publication.

## Purpose and authority

This library is the common in-memory boundary between future ingress builders and the future
columnar-batch encoder/decoder. Its canonical buffer rules come directly from
[columnar-batch v1](../formats/columnar-batch-v1.md), while schema ordinal meaning comes from the
immutable [`chronos_schema`](schema-foundation.md) model. It adds no production dependency.

The CMake target is `chronos_columnar`, exported and installed as `chronos::columnar`. Its public
headers are:

- `chronos/columnar/column_vector.hpp` for bitmap sizing, safe cells, borrowed views, canonical
  buffer owners, and vector validation; and
- `chronos/columnar/columnar_batch.hpp` for configured limits and schema-pinned owned batches.

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

This is a canonical *buffer* representation, not serialized batch output. The library creates none
of the 96-byte batch header, 80-byte descriptors, aligned padding, or CRC trailer.

Keeping canonical bytes avoids unaligned native loads, native-endian assumptions, struct dumping,
and a second representation inside the future codec. It also preserves every floating bit pattern,
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
all buffers alive and immutable. `OwnedColumnVector::create` validates supplied `std::vector<byte>`
buffers before moving them into an immutable owner. `view()` borrows that owner, so moving or
destroying the owner invalidates outstanding views and byte-valued cells.

Vector and batch owners are move-only. This makes expensive ownership transfer explicit, prevents
an unaccounted copy from acquiring a different allocation capacity, and encourages queues/readers
to retain one owner through `shared_ptr<const OwnedColumnarBatch>` when shared lifetime is needed.

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
behind a short logical size. Both exclude fixed object/allocator bookkeeping and future header,
descriptor, alignment, and CRC bytes. The future encoder must separately prove that its complete
serialized length is within 16,776,992 bytes before allocating output.

The default buffer bound is the accepted maximum v1 embedded-batch length. An enclosing service may
use smaller values. A limit cannot be configured above the accepted v1 column or byte ceiling.

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
- row inspection is `O(1)` plus the returned slice length only when a caller processes it.

There is no per-row owning allocation in the model. Variable data uses one offsets vector and one
values vector per column.

## Verification and remaining evidence

The `chronos_columnar_tests` target covers self-contained headers, every logical storage shape,
canonical null/high-bit rules, offset corruption, UTF-8/BINARY distinction, positive and negative
DECIMAL boundaries, safe row inspection, schema mismatch/order, exact accounting, and configured
bounds. Fixed-seed property tests generate packed nullable Boolean vectors and variable STRING
vectors and report seed/trial on failure. Installation checks the header and exported target; a
separate consumer configures, links, and runs against the staged package.

The ordinary warning-as-error, clang-tidy, ASan/UBSan, and TSan configurations apply through the
shared CMake helpers. No benchmark result or performance claim is made. Before optimizing, measure
validation throughput by type, null density, row count, variable-byte distribution, and schema
width, plus allocation count and retained/object overhead under the benchmark metadata contract.

The future byte codec still needs golden/cross-endian fixtures, corruption matrices, fuzzing, exact
serialized-length admission, CRC coverage, and encoder/decoder round trips. Mutable heads still need
the separate release/acquire publication proof, deterministic interleavings, and ThreadSanitizer.

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

**Does the default byte bound prove the batch can be serialized?** No. It bounds retained canonical
buffers. The future encoder must add and align its header, descriptors, buffers, terminal padding,
and CRC with checked arithmetic and enforce the complete format limit.
