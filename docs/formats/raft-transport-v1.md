# Raft Transport Envelope v1

> **Status:** accepted with implemented exact codec.

This cluster envelope carries one deterministic Raft message for one logical group. All integers
are little-endian. CRC32C detects accidental damage; a mutually authenticated carrier and explicit
principal-to-node authorization establish peer identity.

## Envelope

The fixed header is 96 bytes:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Magic `CHRNRTW\0` |
| 8 | 2 | Major version `1` |
| 10 | 2 | Minor version `0` |
| 12 | 4 | Header length `96` |
| 16 | 8 | Exact complete frame length |
| 24 | 16 | Nonzero logical Raft group UUID |
| 40 | 8 | Nonzero source node |
| 48 | 8 | Nonzero, distinct destination node |
| 56 | 1 | Message kind, `1` through `8` below |
| 57 | 7 | Zero flags/reserved bytes |
| 64 | 8 | Exact payload length |
| 72 | 4 | CRC32C of the payload |
| 76 | 4 | CRC32C of the header with this field zero |
| 80 | 16 | Zero reserved bytes |
| 96 | variable | One exact message payload |
| final 4 | 4 | CRC32C of every preceding frame byte |

The default complete-frame limit is 64 MiB. Embeddings may select a smaller bound and must
configure Raft append batching so every emitted message fits. The codec additionally bounds append
entry count, each entry payload, and snapshot voter count.

## Message payloads

Every payload starts with a nonzero 64-bit term.

1. `REQUEST_VOTE_REQUEST` is 32 bytes: term, candidate node, last-log index, and last-log term. The
   candidate must equal the envelope source.
2. `REQUEST_VOTE_RESPONSE` is 16 bytes: term, one canonical Boolean byte, and seven zero bytes.
3. `APPEND_ENTRIES_REQUEST` starts with term, leader node, previous index, previous term, leader
   commit index, 32-bit entry count, and four zero bytes (48 bytes total). The leader equals the
   source. Each entry is index (8), term (8), nonzero type (1), three zero bytes, payload length (4),
   and exact payload. Entry indexes are consecutive after the previous index.
4. `APPEND_ENTRIES_RESPONSE` is 40 bytes: term, success Boolean, conflict-term-present Boolean, six
   zero bytes, match index, conflict term or zero, and conflict index. Success forbids conflict
   state. A failed response may retain the follower's last known match index for conflict repair.
5. `INSTALL_SNAPSHOT_REQUEST` starts with term and source-equal leader, followed by snapshot last
   index, last term, Manifest generation, 32-byte part-set checksum, configuration index, 32-bit
   voter count, four zero bytes, and ascending unique nonzero 64-bit voters. The snapshot metadata
   is 72 bytes before voters. Application snapshot bytes travel through their separate bounded
   transfer protocol; this message preserves the two-stage installation request only.
6. `INSTALL_SNAPSHOT_RESPONSE` is 24 bytes: term, success Boolean, seven zero bytes, and installed
   last-included index.
7. `READ_BARRIER_REQUEST` is 24 bytes: term, source-equal leader, and nonzero opaque context.
8. `READ_BARRIER_RESPONSE` is 24 bytes: term, nonzero context, accepted Boolean, and seven zero
   bytes.

Vote last-log and append previous-log index/term fields use exact zero-pair semantics, and their log
term cannot exceed the message term. A successful snapshot response names a nonzero installed index.

## Validation and compatibility

Decoding validates the fixed physical bound, magic, and header checksum before trusting declared
lengths. It then validates exact version, header and complete lengths, required-zero bytes, route,
payload and full-frame checksums, kind-specific bounds, canonical Booleans, exact payload exhaustion,
and message/source consistency. A checksum-valid unknown version or kind is `NOT_SUPPORTED`;
damage is `CORRUPTION`; configured bound violations are `RESOURCE_EXHAUSTED`.

The receiver rejects an unauthenticated peer before decoding, authorizes its stable principal for
the claimed source node, exact-matches the destination to its local node, and only then calls the
group-scoped durable runtime. The envelope does not replace the runtime's persist-before-send rule:
outbound bytes are released only after any associated persistent transition has synchronized.

Minor-version compatibility is exact in v1. Reserved fields must remain zero until a later accepted
version defines them.

## Stream ownership

`RaftTransportFrameReader` supports fragmented and coalesced byte streams without trusting a wire
length before validation. It buffers only the fixed 96-byte header first, validates its checksum,
route, version, reserved fields, and configured complete-frame bound, then allocates exactly the
declared frame. It returns at most one owned envelope and the exact consumed prefix per call; callers
retain and resubmit any suffix containing another frame. Corruption, unsupported versions, bounds,
and allocation failures are sticky for that reader instance.

`RaftTransportFrameWriteCursor` owns one complete canonically decoded frame and exposes only its
unwritten suffix. Checked advancement supports short writes, and moving the cursor leaves the source
complete. Descriptor, TLS, retry, and readiness ownership remain outside both types.

## Authenticated dispatch

`RaftTransportReceiver` requires a transport-authenticated nonzero stable principal before it
decodes a complete frame. It then authorizes that principal for the claimed source, exact-matches the
destination to the local node, and admits one owning receive operation to the asynchronous durable
runtime. Outbound frames become available only through that runtime's post-synchronization
completion. Outbound encoding borrows the completed transition so a size or encoding failure cannot
consume the only owned response.
