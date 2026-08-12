# Metadata Command v1

> **Status: codec and committed metadata-group application are implemented.**

Metadata Command v1 is the application payload for logical Raft entry type `2` in the dedicated
metadata group. It durably orders cluster nodes, schema identities, tablet placement epochs and
leader hints, legacy retention policy, and complete table policy. Complete immutable schemas use
the separate additive
[Schema Definition v1](schema-definition-v1.md) entry type `3`; existing type-2 bytes are never
reinterpreted. It is independent of WAL v1 and never uses native object representations. All
integers are little-endian.

## Envelope

Every command is a maximum-64-KiB exact byte string with a 48-byte header, one kind-specific
payload, and a 4-byte trailer.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 4d 44 43 00` (`CHRNMDC\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `48` |
| 16 | 4 | Total command size |
| 20 | 4 | Payload size |
| 24 | 1 | Command kind |
| 25 | 7 | Required zero |
| 32 | 4 | CRC32C of payload |
| 36 | 4 | CRC32C of header with this field zero |
| 40 | 8 | Required zero |

The final 4-byte trailer is CRC32C over header plus payload. A decoder verifies the fixed header
checksum before trusting length fields, then exact size relationships, reserved bytes, payload and
trailer checksums, bounded fields, canonical membership order, and exact payload exhaustion.

## Payload kinds

- **1 — cluster node:** `node_id:u64`, `endpoint_length:u32`, then exact endpoint bytes. Node ID and
  length are nonzero; endpoint length is runtime-bounded.
- **2 — schema:** table UUID (16), schema UUID (16), schema version (8). All identities and the
  version are nonzero.
- **3 — tablet placement:** table UUID (16), tablet UUID (16), placement epoch (8), replica count
  (4), leader-present byte (1), three zero bytes, leader node (8; zero iff absent), then ascending
  unique nonzero replica node IDs (8 each). A present leader must be a replica.
- **4 — retention:** table UUID (16), nonnegative system-history nanoseconds (`i64`), and nonzero
  retry-retention positions (`u64`). This legacy partial record remains decodable. After kind 5 is
  present for a table, a later kind 4 must exactly match those two complete-policy fields.
- **5 — complete table policy:** table UUID (16), positive partition interval nanoseconds (`i64`),
  positive event-data retention nanoseconds (`i64`), positive system-history retention nanoseconds
  (`i64`), nonnegative allowed lateness nanoseconds (`i64`), nonzero retry-retention positions
  (`u64`), then eight required-zero bytes. Application requires an installed complete schema for
  the table and derives the legacy retention view from this record.

Encoders canonicalize replica order. Unknown major/minor versions or kinds report unsupported;
checksum or semantic damage reports corruption. Adding fields requires a compatible minor rule or a
new command kind/version; existing bytes are never reinterpreted.

## Application and recovery

Only committed entries apply, strictly by Raft log index. The metadata state machine pre-decodes the
available committed batch, applies it, and only afterward durably advances the group's applied index.
Until a metadata application snapshot is defined, recovery starts from empty state and replays the
complete retained committed log, including Schema Definition v1 entries; a compacted prefix is
rejected.
