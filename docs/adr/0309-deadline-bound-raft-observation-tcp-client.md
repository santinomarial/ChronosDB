# ADR 0309: Deadline-bound Raft observation TCP client

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0308](0308-outbound-raft-observation-mtls-acquisition.md),
  [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)

## Context

The outbound observation mTLS attempt accepted an already-connected borrowed descriptor. Embedding
glue still had to own a nonblocking connect, prove `SO_ERROR` completion, bind authentication
metadata to the actual route, apply a connect deadline, and destroy TLS before closing its socket.

## Decision

`RaftObservationTcpClient` is the move-only owner of one exact observation request, one nonblocking
`TcpSocket`, and its later mTLS attempt. Construction validates the request, every timeout,
configured transport limits, and equality between the authentication IPv4 address and actual remote
endpoint before opening a descriptor.

While connecting, the client requests write readiness and applies a separate positive deadline. It
creates TLS only after `finish_connect` proves completion, then delegates handshake, authorization,
request/response I/O, exchange deadlines, and exact correlation to `RaftObservationTlsClient`.
Failure destroys the TLS carrier before closing the descriptor and is sticky. The composite owns no
retry, address rotation, fan-out, or pair-selection policy.

## Consequences and validation

Each acquisition now has one explicit descriptor owner across connect and exchange. Memory is one
fixed request, bounded response state, TCP/TLS state, and constant metadata. A real loopback test
proves nonblocking connect, mutual TLS, request/response exchange, and exact observation result. A
second test proves route mismatch rejects before connection and the exact connect deadline closes
the descriptor permanently. The loopback tests require host execution where sandbox policy forbids
local bind.

Inbound serving, multi-address retry, leader/follower fan-out, pair validation, and packaged query
construction remain incomplete.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Outbound Raft observation mTLS acquisition](0308-outbound-raft-observation-mtls-acquisition.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Raft Observation Transport v1](../formats/raft-observation-transport-v1.md)
