# ADR 0392: Deadline-bound definition-bound vector aggregate query v2 TCP client

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0371](0371-deadline-bound-schema-bound-vector-query-v2-tcp-client.md),
  [ADR 0391](0391-bounded-definition-bound-vector-aggregate-query-v2-mutual-tls.md)

## Context

The aggregate mutual-TLS client accepted one already-connected descriptor plus the exact aggregate
definition vector and query resource authority. Embeddings still had to retain that ownership while
a nonblocking TCP connection was pending, prove `SO_ERROR` completion, construct TLS afterward, and
destroy TLS state before its borrowed descriptor. Repeating that glue could detach variable
aggregate memory authority, authenticate a different endpoint, or start transport for an invalid
definition vector.

## Decision

`DistributedVectorAggregateQueryTcpClientV2` is a move-only, single-threaded owner for one
immutable aggregate attempt. Before acquisition, `begin` exact-decodes the Fragment-v2 request,
requires its embedded target to equal the attempt target, validates the complete ordered definition
vector and all outer/nested carrier limits, requires positive connect/handshake/exchange deadlines,
and exact-matches the authentication peer IPv4 address to the remote endpoint.

The client owns the move-only nonblocking `TcpSocket`, attempt bytes, definition vector, and query
resource context while connecting. Write readiness invokes `finish_connect`; only confirmed
`SO_ERROR` success permits maintained client-TLS construction and an atomic transfer of the attempt,
definitions, and resources into `DistributedVectorAggregateQueryTlsClientV2`. TLS readiness and
terminal response publication then delegate to that carrier.

Connect or carrier failure is sticky. Failure destroys the TLS carrier before explicitly closing
the TCP descriptor, and declaration order enforces that teardown order normally. The TLS context,
authenticator, and node authorizer are borrowed and outlive the client. This owner performs no
retry, backoff, endpoint rotation, listener admission, coordination, or process lifecycle.

## Alternatives considered

- **Copy definitions after connect:** rejected because allocation failure or caller mutation could
  detach the connection from the authority validated before acquisition.
- **Create TLS before checking `SO_ERROR`:** rejected because writable nonblocking sockets can
  represent failed connections.
- **Reuse the row TCP client:** rejected because it cannot own aggregate definition or query-memory
  authority.
- **Retry internally:** rejected because the finite aggregate sender owns whole-attempt retry and
  must observe every transport outcome.

## Consequences

Memory is one attempt, definition vector, shared query resource authority, TCP/TLS state, and the
carrier's independently bounded response vector. Work is linear in request and response bytes plus
constant connection state. Invalid authority opens no socket. One event-loop thread serializes
calls, so no synchronization or memory-ordering argument is required.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): acquisition does not alter Fragment-v2 or aggregate
  response bytes.
- [Invariant 6](../architecture/invariants.md): definition width, nested frames, bytes, and three
  deadlines are independently finite before acquisition.
- [Invariant 10](../architecture/invariants.md): definitions and query resources remain attached to
  the exact attempt through TLS transfer.
- [Invariant 14](../architecture/invariants.md): request target, remote endpoint, and authenticated
  peer address remain exact.
- [Invariant 15](../architecture/invariants.md): TCP success never bypasses mutual TLS or node
  authorization.
- [Invariant 18](../architecture/invariants.md): move-only ownership, reverse-safe teardown, and
  single-thread affinity are explicit.

## Validation plan

A real IPv4 loopback listener must progress nonblocking connect into mutual TLS, authenticate both
certificate fingerprints and exact principal/node mappings, invoke definition binding and execution
once, and publish the complete two-state vector only after terminal closure. Focused tests prove the
exact sticky connect deadline closes the descriptor, responses remain unavailable, and invalid
definition/count bounds reject before acquisition. Run header self-containment, installed
consumption, formatting, static analysis, ASan/UBSan, and the full serialized suite.

## Migration or rollback considerations

No durable or wire bytes change. Embeddings can replace ad hoc outbound acquisition with this owner.
Rollback restores caller-side TCP/TLS joining, which must preserve validation-before-connect,
definition/resource ownership, exact endpoint identity, `SO_ERROR` completion, sticky deadlines,
terminal-only publication, and TLS-before-descriptor teardown.

## References

- [Deadline-bound schema-bound vector query v2 TCP client](0371-deadline-bound-schema-bound-vector-query-v2-tcp-client.md)
- [Bounded definition-bound vector aggregate query v2 mutual TLS](0391-bounded-definition-bound-vector-aggregate-query-v2-mutual-tls.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
