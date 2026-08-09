# Raft Tablet Application Snapshot v1

> **Status: canonical codec, lock-protected local durable installation, and exact
> snapshot-plus-suffix tablet recovery are implemented.**

All integers are unsigned little-endian. UUID fields use their canonical 16-byte durable order.
The complete object is bounded by the caller and by the 1 GiB format maximum.

## Header

The fixed header is 160 bytes:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHRRTAS\0` |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `160` |
| 16 | 8 | Complete byte size including trailer |
| 24 | 4 | Application entry count |
| 28 | 4 | Voter count |
| 32 | 16 | Raft group UUID |
| 48 | 16 | Table UUID |
| 64 | 16 | Tablet UUID |
| 80 | 8 | Last included Raft index |
| 88 | 8 | Last included Raft term |
| 96 | 8 | Application manifest generation |
| 104 | 32 | Application part-set checksum |
| 136 | 8 | Membership configuration index |
| 144 | 8 | Entry-area offset |
| 152 | 4 | Header CRC32C with this field zero |
| 156 | 4 | Required zero |

The header is followed by `voter_count` sorted, unique, nonzero `UINT64` node IDs. The entry-area
offset must equal `160 + voter_count * 8`.

## Application entries

Each entry begins on an 8-byte boundary with a 24-byte header:

| Relative offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Original logical Raft index |
| 8 | 8 | Original Raft term |
| 16 | 4 | Payload byte size |
| 20 | 4 | Payload CRC32C |

The exact nonempty `COLUMNAR_APPEND v1` payload follows, then zero padding through the next 8-byte
boundary. Indexes are nonzero, strictly increasing, and no later than the header's last included
index. Terms are nonzero, nondecreasing, and no greater than the included term; an application entry
exactly at the last included index must carry the same term as the header. Membership-only indexes
may be absent from this application list.

The final four bytes are CRC32C over every preceding byte, including the stored header checksum,
voters, entry headers, payloads, and zero padding. No trailing bytes are allowed.

The codec authenticates framing and entry bytes but deliberately does not reinterpret nested
`COLUMNAR_APPEND v1` payloads. The tablet application installer performs exact command decoding,
schema/tablet binding, retry validation, and ordered publication before accepting the snapshot.

Local durable files use `snapshot-<20-digit-index>.rtas`. Installation writes and exact-validates a
deterministic `.tmp`, synchronizes it, atomically renames without replacement, then synchronizes the
directory. Existing identical bytes are an idempotent retry; changed bytes at one index are
corruption. Local creation copies any exact previously compacted application entries, appends the
supported applied retained-log commands through the new boundary, installs these bytes first, and
only then durably compacts Raft to their identical metadata.
