# ADR 0336: Deadline-bound grouped-query TCP client

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md),
  [ADR 0335](0335-bounded-grouped-query-mutual-tls.md)

## Context

The grouped mutual-TLS client accepted an already-connected descriptor. Embeddings still had to
retain the immutable attempt while a nonblocking TCP connection was pending, prove `SO_ERROR`
completion, establish TLS only afterward, enforce a separate connect deadline, and destroy the TLS
carrier before its borrowed descriptor. Inconsistent glue could leak a descriptor, bind peer
authentication to a different address, or expose a response prefix from a failed stream.

## Decision

`DistributedGroupedQueryTcpClient` is a move-only, single-threaded composite for one immutable
grouped attempt. Before opening a descriptor, `begin` exact-decodes and validates the attempt,
requires its embedded target to match the attempt target, validates connect/handshake/exchange
timeouts and response-frame bounds, and requires the authentication peer IPv4 address to equal the
actual remote endpoint address.

The client starts one move-only nonblocking `TcpSocket`, requests write readiness while connecting,
and applies a separate positive monotonic connect deadline. It does not create TLS state until
`finish_connect` confirms `SO_ERROR` success. It then creates the maintained client `TlsSocket` and
delegates readiness to `DistributedGroupedQueryTlsClient`.

Completion exposes only that carrier's terminally closed value-owned response span. Any connect or
carrier failure is sticky and resets the TLS carrier before explicitly closing its descriptor. The
field order preserves the same carrier-before-descriptor destruction order on normal destruction.
The TLS context, authenticator, and node authorizer are borrowed and must outlive the client.

The composite owns no retry, backoff, address rotation, or coordinator policy. Those remain above
the one-attempt boundary. No durable or network bytes change.

## Consequences and validation

Memory is one bounded attempt plus TCP/TLS state and the grouped carrier's bounded response vector.
Work is linear in request and response bytes plus constant connect state. Invalid attempts and
inconsistent routes open no socket. One event-loop thread serializes calls, so no synchronization
or memory-ordering argument is required.

A focused real loopback test drives nonblocking TCP establishment into mutual TLS, authenticates
both certificate fingerprints and exact principal/node mappings, invokes the grouped receiver once,
and returns two ordered partials only after terminal closure. A second test proves an invalid frame
limit is rejected and the exact connect deadline creates a sticky `UNAVAILABLE` failure after the
descriptor is closed. The installed external-consumer gate references the public constructor.

Inbound TCP listener/server ownership, sender/coordinator integration, packaged multi-tablet
execution, multi-process failover, and broad fault/measurement evidence remain incomplete. No Phase
16 exit gate is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Bounded grouped-query mutual TLS](0335-bounded-grouped-query-mutual-tls.md)
- [Deadline-bound distributed-query TCP client](0177-deadline-bound-distributed-query-tcp-client.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Distributed Grouped FLOAT64 Query Transport v1](../formats/distributed-grouped-float64-query-transport-v1.md)
