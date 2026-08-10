# Tablet Movement External-Prefix Reference v1

All integers are unsigned little-endian. The complete value is bounded by the configured reference
limit, never above 1 MiB. It contains no snapshot payload bytes.

## Header (64 bytes)

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHRMOVR\0` |
| 8 | 2 | major `1` |
| 10 | 2 | minor `0` |
| 12 | 4 | header size `64` |
| 16 | 8 | total size |
| 24 | 8 | payload size |
| 32 | 4 | header CRC32C with this field zero |
| 36 | 4 | payload CRC32C |
| 40 | 24 | zero reserved |

## Payload

The fixed 112-byte prefix is followed by canonical voter and learner IDs.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | tablet UUID |
| 16 | 8 | current placement epoch |
| 24 | 8 | source node ID |
| 32 | 8 | target node ID |
| 40 | 1 | movement phase code |
| 41 | 7 | zero reserved |
| 48 | 4 | voter count |
| 52 | 4 | learner count |
| 56 | 8 | snapshot manifest generation |
| 64 | 8 | snapshot applied Raft index |
| 72 | 8 | snapshot applied Raft term |
| 80 | 8 | total snapshot bytes |
| 88 | 4 | whole snapshot content CRC32C |
| 92 | 4 | zero reserved |
| 96 | 8 | referenced received snapshot bytes |
| 104 | 8 | original snapshot-session placement epoch |
| 112 | variable | sorted voter IDs (`u64` each), then sorted learner IDs |

The final four bytes are CRC32C over the header and payload. There are no trailing extensions.
Unknown versions are unsupported. Invalid sizes, counts, reserved bytes, checksums, ordering,
membership, phases, or epoch relationships are corruption when decoding.

The adding-target phase is not representable because no snapshot session exists. For transferring,
catching-up, and ready phases, current and session epochs are equal. Target-promoted is exactly
session epoch plus one; complete is exactly plus two. The session epoch plus tablet/source/target/
snapshot fields derives the exact durable chunk owner.

This value authenticates metadata and received length only. Recovery must separately exact-load the
same session's contiguous chunk prefix, require the same length, and perform full movement-state and
whole-content CRC validation before adoption.

## Reference generation envelope

Durable ordering wraps exactly one reference in a distinct version 1.0 envelope. Its 64-byte header
is followed by the complete reference and a four-byte CRC32C trailer.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHRMVRG\0` |
| 8 | 2 | major `1` |
| 10 | 2 | minor `0` |
| 12 | 4 | header size `64` |
| 16 | 8 | total envelope size |
| 24 | 8 | nonzero checkpoint generation |
| 32 | 8 | nested reference size |
| 40 | 4 | nested reference CRC32C |
| 44 | 4 | header CRC32C with this field zero |
| 48 | 16 | zero reserved |

The trailer covers the header and nested reference. The complete envelope must fit the configured
reference limit. `CHRMVRG` is never interpreted as the self-contained `CHRMOVG` envelope. The
tablet-owned generation namespace exact-dispatches these two magics under one contiguous sequence;
filename generation and embedded tablet must agree.
