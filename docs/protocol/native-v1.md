# ChronosDB Native Protocol v1

> **Status: accepted specification; fixed framing, message payloads, and request state implemented.** This document controls the
> Protocol v1 byte layout and compatibility rules. Payload and connection-state sections identify
> their implementation status independently. ADR 0060 accepts the frame contract.

## Scope and normative language

The words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** are normative. Integers are unsigned
little-endian values of the stated width. Byte ranges are half-open. Encoders write fields
individually and never dump a native structure. CRC32C uses the Castagnoli parameters defined by
WAL v1 and is serialized as a little-endian u32.

Protocol v1 frames handshake, ingest, query, cancellation, error, and liveness messages. It does
not define subscriptions, resume tokens, distributed routing, compression, or TLS records.

## Limits

| Name | Value |
| --- | ---: |
| Header size | 40 bytes |
| Protocol major/minor | 1/0 |
| Maximum payload | 16,777,216 bytes |
| Maximum complete frame | 16,777,256 bytes |

A server MAY configure a smaller nonzero maximum payload and MUST apply it before payload-sized
allocation. A peer cannot negotiate above the Protocol v1 ceiling.

## Frame header

| Offset | Width | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 4 | `magic` | bytes `43 44 42 31` (`CDB1`) |
| 4 | 2 | `protocol_major` | `1` |
| 6 | 2 | `protocol_minor` | emitted as `0` |
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

An implementation emits major 1/minor 0. Before handshake negotiation exists, it accepts major 1
and a minor not greater than 0. Unknown major versions, newer minor versions, header extensions,
types, or flags fail closed. Later compatible minors require explicit handshake negotiation and
golden compatibility fixtures before acceptance.

## Security boundary

CRC32C is accidental-corruption coverage, not authentication. A peer able to modify bytes can
recompute it. The baseline permits plaintext only on IPv4 loopback and can attach an
authenticator-issued principal before handshake. `TLS_REQUIRED` fails closed until a maintained TLS
record backend is integrated; it never downgrades to plaintext.

## Handshake and request lifecycle

The first client frame MUST be `CLIENT_HELLO` with request ID zero. Its fixed 24-byte payload is:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 2 | payload format `1` |
| 2 | 2 | minimum major |
| 4 | 2 | maximum major |
| 6 | 2 | maximum minor |
| 8 | 8 | requested feature bits; zero in v1 |
| 16 | 4 | requested maximum payload |
| 20 | 4 | reserved zero |

`SERVER_HELLO` uses the same size with selected major at offset 2, selected minor at 4, zero at 6,
accepted feature bits at 8, effective maximum payload at 16, and reserved zero at 20. No request is
admitted before a compatible 1.0 handshake.

Ingest/query IDs are positive and strictly increase per connection. They cannot be reused after
completion or cancellation. The configured active-request limit fails immediately with overload.
`CANCEL` has an empty payload and names an already issued ID; repeating cancellation is a successful
no-op. PING has request ID zero and an empty payload.

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
