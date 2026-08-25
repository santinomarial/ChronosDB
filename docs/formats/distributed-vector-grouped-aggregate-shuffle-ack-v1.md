# Distributed Vector Grouped Aggregate Shuffle Acknowledgment v1

> **Status: accepted and implemented for exact codec and bounded partial-I/O ownership.** The
> acknowledgment is a success receipt for one complete authorized shuffle edge and is carried by a
> bounded mutual-TLS connected session; TCP and finite retry owners remain enclosing responsibilities.

`CHDVGAK1` is a fixed 132-byte success acknowledgment emitted only after the destination has
validated and privately retained one complete `CHDVGSF1` source-partition stream. Integers are
unsigned little-endian, UUIDs use canonical network-order bytes, and reserved bytes are zero.

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGAK1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | total length | `132` |
| 24 | 8 | acknowledgment source | Original destination node |
| 32 | 8 | acknowledgment target | Original source node |
| 40 | 16 | query ID | Exact immutable shuffle authority |
| 56 | 16 | source tablet ID | Exact original edge |
| 72 | 4 | partition ID | Exact original edge |
| 76 | 4 | partition count | Exact immutable authority |
| 80 | 2 | hash version | Exact original edge; currently `1` |
| 82 | 2 | flags | Zero |
| 84 | 4 | accepted frame count | `1..4,096` |
| 88 | 8 | accepted outer bytes | Complete stream extent, bounded by one GiB |
| 96 | 28 | reserved | Zero |
| 124 | 4 | header CRC32C | Bytes `[0,124)` |
| 128 | 4 | frame CRC32C | Bytes `[0,128)` |

The wire route is reversed, but typed decode reconstructs the original source-to-destination edge.
Query, tablet, partition, nodes, partition count, and hash version must match one exact immutable
authority. Accepted frame and byte extents must match the sender's immutable attempt before success
can publish. The format carries success only: rejection, incomplete input, or receiver failure
closes the attempt without fabricating an acknowledgment.

Header and complete-frame integrity pass before interpretation. Unknown versions are unsupported;
damaged or noncanonical bytes and authority drift are corruption. Invalid typed encoder input is an
invalid argument, and allocation failure is resource exhaustion. The fixed reader consumes at most
one receipt, reports a coalesced suffix to its caller, and fails sticky; the move-only cursor owns
all short-write progress. CRC32C is damage detection, not peer authentication.

Version 1.0 is the only accepted layout. Writers emit exactly 1.0 with zero flags/reserved bytes;
readers reject every other version. Changing success meaning, extent semantics, or authority
correlation requires a new version or separately negotiated format.
