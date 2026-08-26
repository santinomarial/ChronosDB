# Distributed Vector Grouped Aggregate Shuffle Result v1

> **Status: accepted and implemented for exact encoding, decoding, bounded header-first reads, and
> partial-write ownership.** Authenticated request/response sessions, complete partition-stream
> ownership, retry, and lifecycle integration remain enclosing responsibilities.

`CHDVGRR1` carries one reduced partition chunk from its authority destination node to one explicit
coordinator node. A nonempty payload is exactly one Native Protocol v1 `QUERY_RESULT` batch whose
descriptors match the separately proof-bound raw grouped result schema. An empty payload is
canonical only as a terminal marker. All integers are unsigned little-endian, UUIDs use canonical
network-order bytes, and reserved bytes are zero.

The hard maximum frame length is 128 bytes plus the 64-MiB Native payload ceiling plus a four-byte
trailer. Deployments may impose lower frame, row, column, name, and payload limits.

## Layout

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | `CHDVGRR1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | header length | `128` |
| 16 | 8 | total length | Exactly header + payload + four-byte trailer |
| 24 | 8 | source node ID | Exact authority destination for the partition |
| 32 | 8 | target node ID | Nonzero explicit coordinator, different from source |
| 40 | 16 | query ID | Exact immutable shuffle authority |
| 56 | 4 | partition ID | Less than partition count |
| 60 | 4 | partition count | Exact immutable shuffle authority |
| 64 | 2 | hash version | Exact immutable authority; currently `1` |
| 66 | 2 | flags | Bit 0 terminal; every other bit zero |
| 68 | 8 | sequence | Nonzero; enclosing receiver requires contiguous order |
| 76 | 4 | payload length | Zero only for a terminal marker |
| 80 | 4 | payload CRC32C | Exact payload bytes |
| 84 | 4 | schema CRC32C | Canonical encoded raw result schema |
| 88 | 36 | reserved | Zero |
| 124 | 4 | header CRC32C | Bytes `[0,124)` |
| 128 | variable | payload | Canonical nonempty Native `QUERY_RESULT`, or empty terminal |
| final - 4 | 4 | frame CRC32C | Every preceding frame byte |

## Authority and canonicality

The codec borrows one immutable grouped-shuffle authority, one raw grouped result schema, and the
expected coordinator node. Query, partition count, hash version, result source, and result target
must match. The source must be the authority destination for that exact partition. This format has
no source-tablet field because all tablet states have already closed and merged at the reducer.

Every nonempty payload must contain at least one row. Its column names, logical types, and
nullability must exactly match the raw grouped result schema, and every cell must pass the Native
codec's canonical typed validation. Zero-row payloads are rejected so an empty partition has one
wire representation. The schema checksum detects early descriptor-authority drift; full descriptor
comparison still occurs after payload integrity passes.

The header-first reader validates fixed integrity, version, authority, schema identity, and all
allocation-driving lengths before retaining one exact frame. It consumes only the current-frame
prefix from coalesced input, publishes no partial value, resets after success, and makes frame
failure sticky. The move-only write cursor owns the complete encoded bytes and all short-write
progress; its moved-from owner is complete and has no pending bytes.

CRC32C detects accidental damage; it does not authenticate a peer. An enclosing mutually
authenticated session must bind the source certificate principal to the result source node and the
target principal to the coordinator node. That session must also define request authority,
partition-stream closure, complete-response publication, deadlines, retry deduplication, and
cancellation.

## Compatibility

Version 1.0 is the only accepted layout. Writers emit exactly 1.0 and zero all unknown fields.
Readers reject any other major or minor version. Changing routing identity, terminal semantics,
payload type, or schema binding requires a new version or separately negotiated envelope.
