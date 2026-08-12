# Arrow and Parquet interoperability

## Purpose and public interface

`chronos::interop` converts an immutable `OwnedColumnarBatch` to an Arrow IPC file or Parquet file
and imports either format into a canonical batch. It is an optional ecosystem boundary, not a scan
path or primary storage engine. The four file functions in `arrow_parquet.hpp` expose no
third-party types.

An import always receives a pinned `TableSchema`. That schema owns table/schema/column identities,
roles, names, types, nullability, and lineage; an external file owns none of them. The provider first
checks the external field sequence against that schema, combines external chunks, normalizes null
slots and offsets, and then delegates final buffer checks and accounting to
`OwnedColumnarBatch::create`.

## Data mapping and invariants

Integer and floating types keep their widths. `TIMESTAMP_NS` maps to an Arrow nanosecond timestamp
without timezone, `DATE` to Date32, `DECIMAL(p,s)` to Decimal128(p,s), UUID to fixed binary(16), and
STRING/SYMBOL/BINARY to UTF-8/UTF-8/binary. Field metadata distinguishes exported SYMBOL and UUID
for tools, but the exact target schema controls import semantics. Null validity remains LSB-first;
null fixed slots are zeroed and null variable rows receive equal adjacent offsets before canonical
validation.

Arrow validates external arrays/tables, and ChronosDB independently validates the resulting
canonical vectors. Unsupported field order, count, name, type, nullability, zero rows, corrupt
framing, and configured resource-limit violations fail closed. This preserves invariants 10, 14,
and 18 and leaves CSEG/Manifest contracts untouched.

## Ownership, lifetime, and failure behavior

The provider copies canonical buffers into Arrow-owned aligned buffers for export. Imported Arrow
objects live only for the synchronous call; the returned batch owns its normalized storage and
shared target schema. Calls have no shared mutable state except a relaxed atomic used solely to
make process-local temporary names distinct.

Exports write and close a temporary file before an atomic same-directory rename. A failed call
removes only its uniquely named temporary. Imports stat the source before parsing and bound file
bytes plus final rows, columns, logical bytes, and retained capacity. Arrow/Parquet errors are
translated without claiming partially decoded data is usable. Compressed Parquet may transiently
allocate beyond its file size, so a process/container memory limit remains necessary for hostile
inputs.

## Complexity and tradeoffs

Conversion is linear in rows plus canonical bytes and currently copies data in both directions.
Parquet adds compression/encoding CPU and may have a much smaller file than decoded memory. Avoiding
copies could improve throughput, but would couple Arrow buffer lifetime/alignment and sliced-array
semantics to public batches. Evidence must justify that optimization.

The dependency is optional because its transitive surface is large. CSEG remains preferable for
recovery, snapshot identity, checksummed pages, pruning, temporal system columns, and atomic
Manifest installation; Arrow/Parquet are preferable at ecosystem boundaries.

Likely review questions include why the target schema is mandatory, why SYMBOL maps to UTF-8, why
Parquet cannot replace CSEG, what bounds are enforced before and after decode, how partial exports
are hidden, and which transient allocations remain owned by Arrow.
