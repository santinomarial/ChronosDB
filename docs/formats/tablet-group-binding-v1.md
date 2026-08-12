# Tablet Group Binding v1

> **Status: canonical codec, committed metadata application, retained-log recovery, and Metadata
> Application Snapshot 1.1 retention are implemented.**

Tablet Group Binding v1 is the payload for logical Raft entry type `4` in the dedicated metadata
group. It gives one logical tablet an immutable Raft group identity. Placement entry type `2`
continues to own replica membership, placement epoch, and advisory leader hint; neither record is
reinterpreted as the other. All integers are unsigned little-endian.

## Envelope

The value is exactly 84 bytes: a 48-byte header, a 32-byte payload, and a 4-byte trailer.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 54 47 42 00` (`CHRNTGB\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `48` |
| 16 | 4 | Total size `84` |
| 20 | 4 | Payload size `32` |
| 24 | 8 | Required zero |
| 32 | 4 | CRC32C of the payload |
| 36 | 4 | CRC32C of the header with this field zero |
| 40 | 8 | Required zero |
| 48 | 16 | Nonzero tablet UUID |
| 64 | 16 | Nonzero Raft group UUID |
| 80 | 4 | CRC32C of every preceding byte |

Decoders verify the fixed header checksum before trusting framing, require exact size and reserved
bytes, verify payload and whole-value checksums, and reject nil identities. Unknown major/minor
versions fail closed.

## Application and compatibility

The binding applies only after a placement for the same tablet is committed. The first binding fixes
the group identity permanently. A later exact binding is an ordered idempotent replay and advances
the metadata applied index; a different group for that tablet is rejected without advancing it.
Replica movement and leader changes update placement epochs but never alter the group binding.

Metadata Application Snapshot 1.1 retains type-4 entries exactly. Snapshot minor 0 remains byte-for-
byte unchanged and continues to admit only types 2 and 3. Older readers reject minor 1 rather than
silently omitting a binding. Binding a previously placed pre-1.1 tablet requires a new committed
type-4 entry; no identity is inferred from UUID similarity or leader hints.
