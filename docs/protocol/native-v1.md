# ChronosDB Native Protocol v1

> **Status: accepted specification; Protocol 1.0, feature-gated 1.1 subscriptions, and 1.2
> source-tagged subscription changes implemented.**
> This document controls the Protocol v1 byte layout and compatibility rules. ADR 0060 accepts the
> frame contract, ADR 0094 accepts the minor-1 subscription extension, and ADR 0409 accepts its
> minor-2 source-tagged change payload. Protocol v1 never accepts `QUORUM_SYNC`; the separately
> negotiated [Protocol v2](native-v2.md) owns that extension.

## Scope and normative language

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. Integers are unsigned
little-endian values of the stated width. Byte ranges are half-open. Encoders write fields
individually and never dump a native structure. CRC32C uses the Castagnoli parameters defined by
WAL v1 and is serialized as a little-endian u32.

Protocol v1 frames handshake, ingest, query, subscriptions, cancellation, error, and liveness
messages. It does not define distributed routing, compression, or TLS records. Resume tokens remain
opaque independently authenticated bytes carried by the subscription extension.

## Limits

| Name | Value |
| --- | ---: |
| Header size | 40 bytes |
| Protocol baseline/latest minor | 1.0 / 1.2 |
| Maximum payload | 16,777,216 bytes |
| Maximum complete frame | 16,777,256 bytes |

A server MAY configure a smaller nonzero maximum payload and MUST apply it before payload-sized
allocation. A peer cannot negotiate above the Protocol v1 ceiling.

## Frame header

| Offset | Width | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 4 | `magic` | bytes `43 44 42 31` (`CDB1`) |
| 4 | 2 | `protocol_major` | `1` |
| 6 | 2 | `protocol_minor` | selected session minor; hello frames use `0` |
| 8 | 2 | `header_size` | `40` |
| 10 | 2 | `message_type` | assigned below |
| 12 | 4 | `flags` | assigned below |
| 16 | 8 | `request_id` | connection-scoped identity |
| 24 | 4 | `payload_size` | exact following byte count |
| 28 | 4 | `payload_crc32c` | CRC32C of payload bytes |
| 32 | 4 | `reserved` | zero |
| 36 | 4 | `header_crc32c` | CRC32C over stored bytes `[0,36)` |

The complete frame is exactly `40 + payload_size` bytes. Exact decoders reject trailing bytes.
Streaming decoders may retain later bytes as the next frame only after consuming the exact current
length.

## Assigned message types

| Value | Name | Direction |
| ---: | --- | --- |
| 1 | `CLIENT_HELLO` | client to server |
| 2 | `SERVER_HELLO` | server to client |
| 10 | `INGEST_REQUEST` | client to server |
| 11 | `INGEST_ACKNOWLEDGEMENT` | server to client |
| 20 | `QUERY_REQUEST` | client to server |
| 21 | `QUERY_RESULT` | server to client |
| 22 | `QUERY_END` | server to client |
| 23 | `SUBSCRIBE_REQUEST` | client to server, negotiated 1.1 or newer |
| 24 | `SUBSCRIPTION_READY` | server to client, negotiated 1.1 or newer |
| 25 | `SUBSCRIPTION_CHANGE` | server to client, negotiated 1.1 or newer |
| 26 | `SUBSCRIPTION_ACKNOWLEDGE` | client to server, negotiated 1.1 or newer |
| 27 | `SUBSCRIPTION_CHECKPOINT` | server to client, negotiated 1.1 or newer |
| 28 | `SUBSCRIPTION_END` | server to client, negotiated 1.1 or newer |
| 30 | `CANCEL` | client to server |
| 31 | `ERROR` | server to client |
| 40 | `PING` | either direction after handshake |
| 41 | `PONG` | response in the opposite direction |

Unassigned values are invalid. Assignment is permanent within major version 1.

Bit 0 is `END_STREAM` and is valid only on `QUERY_RESULT`. All other flag bits are unassigned and
MUST be zero. Unknown required flags fail the connection.

## Validation order

An implementation MUST validate without payload-sized allocation in this order:

1. 40 header bytes are available;
2. magic, supported version, header size, type, flags, and reserved field;
3. header CRC32C;
4. payload length against the configured and protocol ceilings;
5. exact payload availability and payload CRC32C; and
6. message/state-specific payload rules.

Truncation, corruption, unsupported interpretation, or a noncanonical field returns an explicit
protocol error and never permits partial dispatch.

## Compatibility

The default frame encoder and every Protocol 1.0 session emit major 1/minor 0. Implementations also
accept minors 1 and 2; a session emits one only after the hello payload selects it. Hello frames
themselves use 1.0 framing so every version can parse negotiation. Every post-handshake frame uses
the selected minor exactly. Types 23–28 require selected minor 1 or newer and feature bit 0. Minor 2
changes only the required `SUBSCRIPTION_CHANGE` payload format described below. Unknown major
versions, newer minor versions, header extensions, types, flags, or feature bits fail closed.

## Security boundary

CRC32C is accidental-corruption coverage, not authentication. A peer able to modify bytes can
recompute it. The baseline permits plaintext only on IPv4 loopback and can attach an
authenticator-issued principal before the native handshake. In epoll `TLS_REQUIRED` mode, OpenSSL
must first verify the server-configured chain policy and client certificate; the application
authenticator then maps the verified certificate SHA-256 fingerprint to a nonzero principal before
any frame is decoded. The transport never downgrades to plaintext. io_uring TLS is explicitly
unsupported.

## Handshake and request lifecycle

The first client frame MUST be `CLIENT_HELLO` with request ID zero. Its fixed 24-byte payload is:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 2 | payload format `1` |
| 2 | 2 | minimum major |
| 4 | 2 | maximum major |
| 6 | 2 | maximum minor |
| 8 | 8 | requested feature bits; zero in 1.0, subscription bit 0 in 1.1/1.2 |
| 16 | 4 | requested maximum payload |
| 20 | 4 | reserved zero |

`SERVER_HELLO` uses the same size with selected major at offset 2, selected minor at 4, zero at 6,
accepted feature bits at 8, effective maximum payload at 16, and reserved zero at 20. Protocol 1.0
has zero feature bits. Protocol 1.1 and 1.2 assign bit 0 to subscriptions. The server selects the
highest common minor and the intersection of requested and supported features. No request is
admitted before a compatible handshake.

Ingest/query/subscription IDs are positive and strictly increase per connection. They cannot be
reused after completion or cancellation. The configured active-request limit fails immediately with
overload. `CANCEL` has an empty payload and names an already issued ID; repeating cancellation is a
successful no-op. Ingest/query ownership ends immediately; subscription ownership remains only for
its terminal token response. PING has request ID zero and an empty payload.

## Ingest and query payloads

`INGEST_REQUEST` begins with payload format u16 `1`, durability u8 (`1` ASYNC, `2` LOCAL_SYNC),
reserved u8 zero, canonical Columnar Append length u32, then exactly those bytes.

`INGEST_ACKNOWLEDGEMENT` is 32 bytes: format u16, requested/effective durability u8 each, outcome u8
(`1` applied, `2` matching retry), three zero bytes, then record sequence, segment number, and byte
offset as u64 values. Applied acknowledgements require nonzero record and segment identities.
Matching retries encode all three position fields as zero because no new WAL operation occurred.

`QUERY_REQUEST` begins with format u16, reserved u16 zero, SQL byte length u32, and exact nonempty
Unicode-scalar UTF-8 SQL.

`QUERY_RESULT` begins with format u16 `1`, flags u16 zero, row count u32, column count u32, and
descriptor-byte length u32. Column count is nonzero and at most 4096; row count may be zero. The
descriptor region contains exactly `column_count` entries, each: logical type u16, type parameters
u16/u16, nullable u8 (0/1), reserved u8 zero, nonempty UTF-8 name length u32, reserved u32 zero, and
exact name bytes. Types and parameters use the frozen Columnar Batch v1 registry.

Cells follow row-major. Each is a u32 length plus exact bytes. Length `0xffffffff` is NULL, carries
no bytes, and requires a nullable descriptor. Non-NULL fixed-width cells have the registry width;
Boolean is one byte 0/1. STRING/SYMBOL cells are valid Unicode-scalar UTF-8; BINARY is uninterpreted.
No trailing bytes are allowed. A zero-row batch carries descriptors and no cells. `END_STREAM`
forbids a later result batch, while an empty `QUERY_END` separately confirms successful execution.

`ERROR` begins with format u16, stable error code u16, message length u32, and an exact nonempty
Unicode-scalar UTF-8 diagnostic. Codes cover malformed frames, unsupported version, invalid state,
duplicate/unknown requests, overload, cancellation, invalid request, execution failure,
unauthorized access, and internal failure.

## Protocol 1.1 and 1.2 subscription payloads

All payloads below use format u16 `1`, except the Protocol 1.2 change payload explicitly described
as format 2. Variable lengths are u32 and must exactly consume the remaining payload within
negotiated limits.

`SUBSCRIBE_REQUEST` has a 28-byte envelope: format at 0, start mode u8 at 2 (`1` new query, `2`
resume), zero u8 at 3, subscription UUID at 4, body length at 20, zero u32 at 24, then the body. A
new body is nonempty Unicode-scalar UTF-8 SQL. A resume body is one nonempty opaque Resume Token.

The historical result uses ordinary `QUERY_RESULT` batches. Its final batch carries `END_STREAM`.
`SUBSCRIPTION_READY` then begins live state with format, zero u16, token length, and one nonempty
initial Resume Token in an 8-byte envelope.

Under Protocol 1.1, `SUBSCRIPTION_CHANGE` payload format 1 has an 84-byte envelope:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 2 | payload format `1` |
| 2 | 1 | operation (`1` UPSERT, `2` DELETE) |
| 3 | 1 | required zero |
| 4 | 8 | nonzero delivery sequence |
| 12 | 16 | tablet UUID |
| 28 | 16 | WAL ID |
| 44 | 8 | nonzero committed record sequence |
| 52 | 16 | schema UUID |
| 68 | 8 | schema version |
| 76 | 4 | nonzero result-key length |
| 80 | 4 | result payload length |
| 84 | variable | result key followed by payload |

DELETE requires an empty result payload. UPSERT payload interpretation belongs to the bound plan.
The full source position and schema identity make replay validation explicit; the delivery sequence
is the at-least-once deduplication order, not event time.

Protocol 1.2 requires payload format 2 for every `SUBSCRIPTION_CHANGE`. The envelope and all offsets
remain identical except:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 2 | payload format `2` |
| 3 | 1 | source kind (`1` WAL, `2` Raft) |
| 28 | 16 | WAL ID for kind `1`; Raft group UUID for kind `2` |
| 44 | 8 | nonzero committed WAL record sequence or Raft log index |

The source identity is nonzero and belongs only to the selected namespace. Equal WAL-ID and Raft
group-UUID bytes do not identify the same source. A 1.2 decoder rejects format 1, and 1.1 and 2.0
decoders reject format 2. Protocol 2.0 retains the frozen 1.1 WAL-only subscription payload.

`SUBSCRIPTION_ACKNOWLEDGE` is 12 bytes: format, zero u16, and nonzero delivery sequence. It uses the
active subscription request ID and does not consume a new request ID. The sequence cannot move
backward or exceed delivered state.

`SUBSCRIPTION_CHECKPOINT` has a 20-byte envelope: format, zero u16, acknowledged sequence u64,
token length u32, zero u32, and the exact Resume Token. It confirms logical acknowledgement; socket
write completion is not acknowledgement.

`SUBSCRIPTION_END` has a 24-byte envelope: format, reason u16 (`1` cancelled, `2` schema changed,
`3` state expired, `4` overflowed, `5` server shutdown), zero u32, safe sequence u64, token length
u32, zero u32, and the final Resume Token. Client cancellation keeps the request active until this
frame or `ERROR`, so the safe token is not lost. External delivery remains at least once.

## Compatibility fixtures and fuzzing

Source-controlled hexadecimal packet fixtures under `tests/fixtures/network` include the accepted
golden frame plus exact truncated-header and corrupted-header-checksum cases. The network protocol
fuzzer drives exact decoding/re-encoding, fragmented stream reassembly, every assigned message
decoder, server state, and client fail-closed input under finite limits.
