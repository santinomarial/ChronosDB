# Distributed Vector Grouped Aggregate Shuffle Result Acknowledgment v1

> **Status: accepted and implemented for exact codec and bounded partial-I/O ownership.** This
> success receipt follows one complete `CHDVGRR1` result stream. Mutual-TLS and TCP owners are
> enclosing responsibilities.

`CHDVGRK1` is a fixed 132-byte success acknowledgment emitted only after the coordinator has
authenticated, validated, and privately retained one complete reduced-partition result stream.
Integers are unsigned little-endian, UUIDs use canonical network-order bytes, and reserved bytes
are zero.

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGRK1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | total length | `132` |
| 24 | 8 | acknowledgment source | Coordinator/result target node |
| 32 | 8 | acknowledgment target | Reducer/result source node |
| 40 | 16 | query ID | Exact immutable shuffle authority |
| 56 | 4 | partition ID | Exact authority partition |
| 60 | 4 | partition count | Exact immutable authority |
| 64 | 2 | hash version | Exact immutable authority; currently `1` |
| 66 | 2 | flags | Zero |
| 68 | 4 | accepted frame count | `1..65,536` |
| 72 | 8 | accepted outer bytes | Complete stream extent, at most one GiB |
| 80 | 4 | raw result schema CRC32C | Canonical schema bytes excluding their trailing CRC |
| 84 | 40 | reserved | Zero |
| 124 | 4 | header CRC32C | Bytes `[0,124)` |
| 128 | 4 | frame CRC32C | Bytes `[0,128)` |

Typed decode reconstructs the original reducer-to-coordinator route. Query, partition, nodes,
partition count, hash version, and raw result schema must match the exact immutable attempt
authority. Accepted frame and byte extents must equal the sender's complete stream before success
can publish. The format carries success only; failure closes the attempt without fabricating a
receipt.

Both checksums pass before interpretation. Unknown versions are unsupported; damaged,
noncanonical, schema-drifted, or authority-drifted bytes are corruption. The fixed reader consumes
at most one receipt and fails sticky on a suffix. The move-only cursor owns all short-write
progress. CRC32C detects accidental damage and is not authentication.

Version 1.0 is the only accepted layout. Writers emit exactly 1.0 with zero flags and reserved
bytes. Changing success meaning, extent semantics, or correlation requires a new version or an
explicitly negotiated format.
