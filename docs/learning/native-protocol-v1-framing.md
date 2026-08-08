# Native Protocol v1 Framing

## Purpose and interface

`chronos::network` provides a portable frame codec shared by clients, connection state machines,
fuzzers, and the Linux reactor. `encode_frame` produces canonical owned bytes;
`decode_frame_header` validates the fixed interpretation prefix without payload allocation; and
`decode_frame` requires one exact frame and returns owned payload bytes.

## Invariants and ownership

- Every frame inherits major/minor version 1/0 and has one 40-byte header.
- Header CRC32C is checked before a payload length can authorize allocation.
- Payload size is bounded by both the 16 MiB protocol ceiling and a smaller deployment limit.
- Decoded payload ownership is independent of the socket input buffer.
- Unknown types, flags, versions, reserved values, corruption, truncation, and trailing bytes fail
  closed.
- Allocation failure becomes `RESOURCE_EXHAUSTED`; it does not escape as `std::bad_alloc`.

Encoding and exact decoding are linear in payload bytes and retain one payload-sized output. Header
decoding is constant time and allocation-free on successful validation.

## Tradeoffs

The fixed header spends 40 bytes per message in exchange for early length validation, direct
request correlation, two independent integrity boundaries, and simple partial-I/O state. CRC32C is
deliberately not a security primitive; TLS/authentication remains a separate boundary.

## Failure testing and measurement

Golden bytes freeze field order and endianness. Unit tests enumerate every truncation boundary and
header byte corruption, payload corruption, exact-length behavior, invalid types/flags, configured
limits, and empty frames. A dedicated allocator executable sweeps every owned encode/decode
allocation. Protocol fuzzing and codec throughput profiles are added before the Phase 10 exit.

## Review questions

**Why checksum the header separately?** It prevents a corrupted peer-controlled length or type from
being trusted merely because the body has not arrived.

**Why is request ID not a retry identity?** It scopes multiplexing and cancellation to one
connection. Durable ingest retry semantics use the independently defined batch identity.

**Why reject trailing bytes in `decode_frame`?** Exact decoding is a safety boundary. A streaming
connection parser splits coalesced input into exact frames before calling it.
