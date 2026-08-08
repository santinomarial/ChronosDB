# Native query result batches

Protocol v1 query results are self-describing because a SQL output is not necessarily a stored
table schema. Each payload owns no decoded cell bytes: `QueryResultBatchView` owns descriptor/cell
view arrays while borrowing the immutable payload. The payload must outlive the decoded view.

The encoder first validates shape, type widths, NULL rules, UTF-8, and checked total size. It then
allocates exactly one output vector. The decoder validates its finite limits and envelope before
reserving descriptor/cell arrays, and never allocates proportional to an unchecked count.

Rows are stored row-major. Each cell has a u32 length followed by canonical bytes; `0xffffffff`
means NULL. Fixed widths match the frozen logical type registry, Boolean is exactly 0 or 1, and
STRING/SYMBOL bytes are valid UTF-8. Zero rows are valid and retain descriptors so clients learn the
result schema. Successful completion is a separate empty `QUERY_END`.

Encoding/decoding is linear in descriptors plus payload bytes and retains `O(columns + cells)` view
metadata. The row-major baseline is simple and exact; a future columnar encoding requires explicit
feature negotiation and evidence.

