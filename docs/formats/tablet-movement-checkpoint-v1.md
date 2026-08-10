# Tablet Movement Checkpoint v1

## Byte order and limits

All integers are unsigned little-endian. The complete value is bounded by the configured checkpoint
limit (never above 1 GiB + 1 MiB), received bytes by the movement snapshot limit, and voter/learner
counts by the movement replica limit. Decoders validate bounds before owned allocation.

## Header (64 bytes)

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHRMOVE\0` |
| 8 | 2 | major `1` |
| 10 | 2 | minor `0` |
| 12 | 4 | header size `64` |
| 16 | 8 | total size |
| 24 | 8 | payload size |
| 32 | 4 | CRC32C of header with this field zero |
| 36 | 4 | payload CRC32C |
| 40 | 24 | zero reserved |

The header CRC includes the installed payload CRC.

## Payload

The fixed 104-byte prefix is followed by voter IDs, learner IDs, and received snapshot bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 16 | tablet UUID |
| 16 | 8 | placement epoch |
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
| 88 | 4 | whole snapshot CRC32C |
| 92 | 4 | zero reserved |
| 96 | 8 | received snapshot bytes |
| 104 | variable | sorted voter IDs (`u64` each), sorted learner IDs, then received prefix |

Phase codes are the `TabletMovementPhase` values 1 through 6. Semantic relationships are mandatory,
not advisory. Unknown codes, duplicates, noncanonical ordering, impossible phase membership, or
snapshot length/checksum mismatch fail closed.

## Trailer

The final 4 bytes are CRC32C over every preceding header and payload byte. There are no trailing
extensions in v1.0.

## Compatibility

Major 1, minor 0 is exact. Unknown major/minor values are unsupported; invalid relationships,
reserved bytes, truncation, trailing data, or checksum mismatch are corruption. Native structs are
never serialized.
