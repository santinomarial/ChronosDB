# Tablet Reconfiguration Action v1

## Framing

All integers are unsigned little-endian. Complete action bytes are bounded by the configured limit,
never above 128 KiB.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHRRACT\0` |
| 8 | 2 | major `1` |
| 10 | 2 | minor `0` |
| 12 | 4 | header size `96` |
| 16 | 8 | total size |
| 24 | 16 | tablet UUID |
| 40 | 8 | movement placement epoch |
| 48 | 1 | action kind |
| 49 | 7 | zero reserved |
| 56 | 16 | destination Raft group UUID |
| 72 | 8 | payload size |
| 80 | 4 | payload CRC32C |
| 84 | 4 | header CRC32C with this field zero |
| 88 | 8 | zero reserved |

The variable payload follows the header. The final 4 bytes are CRC32C over the entire header and
payload.

## Action payloads

Action kind 1 (begin joint membership) stores `u32 voter_count`, four zero reserved bytes, then
sorted unique nonzero `u64` voter IDs. Kind 2 (finalize joint membership) has an empty payload. Kind
3 (publish placement) stores Raft entry type `u8` (exactly 2), seven zero reserved bytes, then one
complete Metadata Command v1 value whose decoded variant is Tablet Placement Metadata.

Identity kind and operation kind must agree. Unknown kinds, unrelated Raft operations, invalid
nested metadata, a nested tablet/next-epoch mismatch, noncanonical voters, invalid identities,
length mismatch, damage, or trailing data fail closed.

## Compatibility

Major 1, minor 0 is exact. Unknown version values are unsupported after header integrity succeeds.
No native object layout is serialized. Future action kinds or payload revisions require an explicit
version policy and cannot reinterpret existing v1 bytes.
