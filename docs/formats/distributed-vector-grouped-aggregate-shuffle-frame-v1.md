# Distributed Vector Grouped Aggregate Shuffle Frame v1

> **Status: accepted and implemented for exact encoding, decoding, bounded partial-I/O ownership,
> immutable authority validation, and atomic authorized complete-stream ownership.** Mutual-TLS
> session ownership, terminal acknowledgment/retry, and destination reduction remain enclosing
> responsibilities.

This frame carries one canonical
[Distributed Vector Grouped Aggregate Exchange v1](distributed-vector-grouped-aggregate-exchange-v1.md)
message across one authorized remote source-tablet/partition edge. It binds the nested group state
to a whole-query ID, source node, destination node, partition count, and canonical hash version.
All integers are unsigned little-endian, UUIDs use canonical network-order bytes, and reserved
bytes are zero.

The hard maximum frame length is 128 bytes plus the 64-MiB nested-frame ceiling plus a four-byte
trailer. Deployments may impose lower outer and nested limits.

## Layout

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGSF1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | total length | Exactly header + nested payload + four-byte trailer |
| 24 | 8 | source node ID | Nonzero and different from destination |
| 32 | 8 | destination node ID | Nonzero and different from source |
| 40 | 16 | query ID | Nonzero; exact immutable shuffle authority |
| 56 | 16 | source tablet ID | Nonzero; exact source entry in the authority |
| 72 | 4 | partition ID | Less than partition count; exact destination entry |
| 76 | 4 | partition count | Nonzero; exact immutable authority |
| 80 | 2 | hash version | Exact authorized version; currently `1` |
| 82 | 2 | flags | Zero |
| 84 | 4 | payload length | Exact nested `CHDVGEX1` byte length |
| 88 | 4 | payload CRC32C | Exact nested payload bytes |
| 92 | 32 | reserved | Zero |
| 124 | 4 | header CRC32C | Bytes `[0,124)` |
| 128 | variable | payload | One complete canonical `CHDVGEX1` message |
| final - 4 | 4 | frame CRC32C | Every preceding outer-frame byte |

## Authority and routing

The codec requires one immutable `DistributedVectorGroupedAggregateShuffleAuthority`. Source
tablet/node, destination partition/node, query ID, partition count, hash version, ordered group-key
definitions, and ordered aggregate definitions must match that authority exactly. A local edge may
exist in the authority, but this remote carrier rejects equal source and destination nodes; local
delivery must use the in-process path.

The nested message's query and tablet IDs must match the outer frame. For every nonempty group, the
decoder recomputes
`canonical_vector_group_key_hash_v1(keys) % partition_count` and requires the outer partition ID.
This second check prevents checksum-valid or authenticated-but-misrouted group state from entering
a reducer. An empty terminal has no key and is valid for any exact authorized partition because it
closes an otherwise empty source edge.

Each frame carries one group or one empty terminal, not a complete source-partition stream. The
enclosing sender must preserve the nested contiguous sequence/ordinal contract. The eventual
destination reducer must accept exactly one complete stream from every plan-ordered source and
merge those sources in authority order; timeout is not a closure signal.

## Validation and ownership

Header magic, header CRC, version, canonical fixed fields, hard and caller limits, reserved bytes,
and immutable route authority pass before a reader allocates the declared total length. After exact
framing, the nested-payload CRC and complete-frame CRC pass before nested decode. Nested decode
then applies its own schema, integrity, position, canonical-cell, and query-memory checks. The
outer routing hash is recomputed only from successfully decoded canonical keys.

Checksum-valid unknown major or minor versions are unsupported. Damaged bytes, noncanonical
fields, authority drift, payload correlation drift, and wrong-partition routing are corruption on
decode. Invalid typed values supplied to the encoder are invalid arguments. Lower deployment
bounds and allocation failures are resource exhaustion where applicable. No failed decode exposes
a partial message, and all nested query credit is released with the failed or destroyed result.

The header-first reader borrows immutable authority that must outlive its single-thread-affine
owner and owns its query resource context. It buffers only the fixed header until integrity,
authority, and allocation-driving lengths pass, then retains one exact frame. It reports only the
consumed prefix, leaves a coalesced successor caller-owned, resets after complete success, and makes
frame failure sticky. The move-only write cursor owns one completely encoded frame, exposes only
its unwritten suffix, rejects over-advance without progress, and leaves its moved-from owner
complete.

The complete-stream sender exact-decodes one canonical partitioner output, requires an empty
terminal or contiguous nonempty positions, and constructs all same-edge outer cursors before
exposing the first byte. The receiver requires an already authenticated principal, authorizes that
principal for the first frame's source node, requires the configured local destination, locks every
later frame to the same edge, and withholds all query-accounted messages until terminal. Missing or
duplicate position, edge drift, count/byte overflow, terminal suffix, decode failure, and allocation
failure discard the whole prefix. This owner does not itself perform TLS or acknowledge receipt.

CRC32C detects accidental damage; it does not authenticate either peer. An enclosing mutually
authenticated connection must bind certificate principals to the exact source and destination
node IDs before accepting application bytes. This format does not define address resolution,
deadlines, retry acknowledgments, deduplication, reducer closure, or destination-failure policy.

## Compatibility

Version 1.0 is the only accepted layout. Writers emit exactly 1.0, zero flags, and zero reserved
bytes. Readers reject any other major or minor version rather than inferring compatibility. Any
change to field meaning, routing hash identity, required authentication binding, or nested payload
kind requires a new version or a separately negotiated envelope; the `CHDVGEX1` format is versioned
independently.
