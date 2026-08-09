# Multiplexed Raft Persistent-State Record v1

> **Status: in-memory codec implemented; segmented file owner and sync policy are not implemented.**
> This format does not alter WAL v1, CSEG v1, or Manifest v1.

The record is a node-level physical envelope for one logical Raft group checkpoint. Groups may be
interleaved and batched in one future segmented append stream. Physical sequence never replaces the
group's logical term/index. All integers are little-endian and native structs are never dumped.

## Header (64 bytes)

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `43 48 52 4e 4d 52 4c 00` (`CHRNMRL\0`) |
| 8 | 2 | Major `1` |
| 10 | 2 | Minor `0` |
| 12 | 4 | Header size `64` |
| 16 | 4 | Total record size including 4-byte trailer |
| 20 | 4 | Persistent-state payload size |
| 24 | 8 | Node-global physical sequence |
| 32 | 16 | Logical group UUID |
| 48 | 4 | CRC32C of payload |
| 52 | 4 | CRC32C of header with this field zero |
| 56 | 8 | Required-zero bytes |

The trailer is CRC32C over header plus payload. The maximum complete record is 16 MiB.

## Persistent-state payload

The fixed 96-byte prefix contains current term, voted-for node (`0` means none), commit index,
applied index, snapshot last index/term, manifest generation, 32-byte immutable-part-set checksum,
log-entry count, and four required-zero bytes. Each entry then contains logical index (8), term (8),
type (1), seven required-zero bytes, payload length (4), four required-zero bytes, and payload.

The decoder validates header integrity before trusting lengths, then exact size relationships,
payload and full-record integrity, required-zero bytes, bounded entry count, and exact exhaustion.
`RaftNode::create` performs the semantic validation: contiguous logical indexes, bounded entries,
valid term/vote/snapshot state, and `applied <= commit <= last`.

## Installation limitation

`MultiRaftRuntime` returns this full logical state with a persist-before-send contract. No current
owner appends it to a synchronized segmented file, reclaims segments, or uses it to expose
`QUORUM_SYNC`; those durability steps are explicitly deferred.

