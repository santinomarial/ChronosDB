# ChronosDB Native Protocol v1

> **Status: accepted specification; fixed framing implemented.** This document controls the
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
recompute it. Authentication and confidentiality belong to the maintained TLS/authenticator
boundary later in Phase 10.
