# ADR 0371: Deadline-bound schema-bound vector query v2 TCP client

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md),
  [ADR 0370](0370-bounded-schema-bound-vector-query-v2-mutual-tls.md)

## Context

The v2 mutual-TLS client accepted an already-connected descriptor. Embeddings still had to retain
the immutable schema-bound attempt while a nonblocking TCP connection was pending, prove
`SO_ERROR` completion, create TLS only afterward, apply a separate connect deadline, and destroy
the TLS carrier before its borrowed descriptor. Duplicated glue could open a socket for an invalid
request, authenticate a different address, or leak a response prefix from a failed stream.

## Decision

`DistributedVectorQueryTcpClientV2` is a move-only, single-threaded composite for one immutable v2
attempt. Before opening a descriptor, `begin` exact-decodes the request, requires its embedded
target to equal the attempt target, validates connect/handshake/exchange timeouts and response
frame/byte bounds, and requires the authentication peer IPv4 address to equal the actual remote
endpoint address.

The client starts one move-only nonblocking `TcpSocket`, requests write readiness while connecting,
and applies a separate positive monotonic connect deadline. It creates a maintained client
`TlsSocket` only after `finish_connect` confirms `SO_ERROR` success, then transfers the exact attempt
to `DistributedVectorQueryTlsClientV2` and delegates readiness.

Completion exposes only the TLS carrier's terminally closed, schema-bound, value-owned response
span. Any connect or carrier failure is sticky and resets TLS before explicitly closing the TCP
descriptor. Declaration order enforces the same TLS-before-descriptor destruction order on normal
teardown. The TLS context, authenticator, and node authorizer are borrowed and must outlive the
client.

The composite owns no retry, backoff, address rotation, coordinator, listener, or worker policy.
No durable or network bytes change.

## Alternatives considered

- **Let each scheduler join TCP and TLS state:** rejected because route validation, deadlines, and
  destruction order are protocol safety properties, not scheduler preferences.
- **Create TLS before `SO_ERROR` success:** rejected because a writable nonblocking descriptor can
  represent a failed connect.
- **Retry inside this owner:** rejected because one immutable attempt must have one reportable
  transport outcome; whole-attempt retry belongs to a finite sender.

## Consequences

Memory is one bounded request attempt plus TCP/TLS state and the carrier's independently bounded
response values. Work is linear in request and response bytes plus constant connection state.
Invalid attempts and inconsistent routes open no socket. One event-loop thread serializes calls,
so no synchronization or memory-ordering argument is required.

Inbound listener/admission ownership, retry arbitration, production vector worker execution,
schema-bound coordination, and process integration remain incomplete.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the composite does not alter v2 application bytes.
- [Invariant 6](../architecture/invariants.md): all count, byte, and deadline bounds pass before
  acquisition and remain enforced by the TLS carrier.
- [Invariant 10](../architecture/invariants.md): the exact Fragment-v2 schema stays owned by the
  immutable attempt until transfer to TLS.
- [Invariant 14](../architecture/invariants.md): request target, attempted endpoint, and
  authenticated peer address remain exact.
- [Invariant 15](../architecture/invariants.md): mutual TLS and node authorization remain mandatory.
- [Invariant 18](../architecture/invariants.md): ownership, reverse-safe destruction, and
  single-thread affinity are explicit.

## Validation plan

A real loopback test drives nonblocking TCP establishment into mutual TLS, authenticates both
certificate fingerprints and exact principal/node mappings, invokes the v2 receiver once, and
publishes two schema-bound responses only after terminal closure. A focused case proves the exact
connect deadline closes the descriptor and creates a sticky `UNAVAILABLE` failure, while an invalid
response-byte limit is rejected before acquisition. Header self-containment, installed
consumption, ASan/UBSan, relevant static analysis, formatting, and the full serialized suite are
required before completion.

## Migration or rollback considerations

No durable or wire migration exists. Embeddings can replace ad hoc single-attempt acquisition with
this owner. Rollback restores caller-side TCP/TLS joining, which must preserve validation before
connect, `SO_ERROR` completion, exact route identity, terminal-only publication, sticky deadlines,
and TLS-before-descriptor teardown.

## References

- [Bounded schema-bound vector query v2 mutual TLS](0370-bounded-schema-bound-vector-query-v2-mutual-tls.md)
- [Deadline-bound grouped-query TCP client](0336-deadline-bound-grouped-query-tcp-client.md)
- [Nonblocking IPv4 TCP descriptor ownership](0175-nonblocking-ipv4-tcp-descriptor-ownership.md)
- [Distributed Vector Query Transport v2](../formats/distributed-vector-query-transport-v2.md)
