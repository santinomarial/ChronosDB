# Tablet Movement Snapshot Chunk v1

All integers are unsigned little-endian. The complete value is bounded by the configured encoded
limit, never above 16 MiB.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHRMCHK\0` |
| 8 | 2 | major `1` |
| 10 | 2 | minor `0` |
| 12 | 4 | header size `128` |
| 16 | 8 | total encoded size |
| 24 | 16 | tablet UUID |
| 40 | 8 | movement placement epoch |
| 48 | 8 | source node ID |
| 56 | 8 | target node ID |
| 64 | 8 | snapshot manifest generation |
| 72 | 8 | snapshot applied Raft index |
| 80 | 8 | snapshot applied Raft term |
| 88 | 8 | total snapshot bytes |
| 96 | 4 | whole snapshot content CRC32C |
| 100 | 4 | zero reserved |
| 104 | 8 | chunk offset |
| 112 | 4 | chunk payload bytes |
| 116 | 4 | payload CRC32C |
| 120 | 4 | header CRC32C with this field zero |
| 124 | 4 | zero reserved |

The exact nonempty payload follows. The final 4 bytes are CRC32C over the header and payload.
Offset plus payload length must not exceed total snapshot bytes. All identity and snapshot boundary
fields except content CRC are nonzero; source and target differ. Header relationships and CRC are
checked before payload ownership. Unknown versions are unsupported, while invalid bounds, reserved
bytes, checksum mismatch, truncation, or trailing bytes are corruption.

## Durable chunk namespace

ADR 0123 assigns one locked directory to one exact session. A final chunk is named
`chunk-<20-digit-offset>.mchk`, where the decimal offset is zero-padded and must equal the embedded
offset. Its only canonical interrupted-install name appends `.tmp`. Recognized malformed names,
non-regular entries, embedded/name disagreement, a first offset other than zero, or any later gap
are corruption. Unrelated names are outside this namespace and ignored.

Final files are immutable. Only the exact current prefix end may be installed; an existing offset
is idempotent only when its full encoded bytes match. File synchronization precedes no-replace
rename, directory synchronization establishes durable success, and cleanup of canonical
temporaries is directory-synchronized. Recovery exact-decodes every final in numeric offset order.
Completion additionally requires exact total length and whole-snapshot content CRC32C.

These rules make chunks resumable transfer authority only. They do not make completed bytes an
installed RTAS, Manifest, or CSEG object.
