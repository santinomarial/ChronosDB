# ADR 0480: Deadline-bound grouped sufficient-state TCP client

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster and networking maintainers
- **Extends:** [ADR 0392](0392-deadline-bound-definition-bound-vector-aggregate-query-v2-tcp-client.md),
  [ADR 0479](0479-bounded-grouped-sufficient-state-mutual-tls.md)

## Context

The grouped sufficient-state mutual-TLS client accepted one already-connected descriptor plus the
complete ordered group-key and aggregate authority and query resource context. Embeddings still had
to retain that authority while a nonblocking TCP connection was pending, prove `SO_ERROR`
completion, construct TLS afterward, and destroy the TLS carrier before its borrowed descriptor.
Repeating that glue could connect before authority validation, authenticate a different address, or
detach data-dependent grouped response decoding from its admitted limits and resources.

## Decision

`DistributedVectorGroupedAggregateQueryTcpClientV2` is a move-only, single-thread-affine owner for
one immutable grouped attempt. Before acquisition, `begin` exact-decodes the Fragment-v2 request,
requires its target to equal the attempt target, validates the complete ordered group-key and
aggregate authority against the grouped plan and result schema, validates every outer and nested
carrier limit, requires positive connect, handshake, and exchange deadlines, and exact-matches the
authentication peer IPv4 address to the remote endpoint.

The client owns the nonblocking `TcpSocket`, attempt bytes, both authority vectors, and query
resource context while connecting. Write readiness calls `finish_connect`; only confirmed
`SO_ERROR` success permits client-TLS construction and atomic transfer of the attempt, authority,
and resources into `DistributedVectorGroupedAggregateQueryTlsClientV2`. TLS readiness and
complete empty-or-contiguous response publication then delegate to that carrier.

Connect or carrier failure is sticky. Failure destroys the TLS carrier before explicitly closing
the TCP descriptor, and declaration order enforces the same teardown order normally. The TLS
context, authenticator, and node authorizer are borrowed and outlive the client. This owner performs
no retry, address rotation, listener admission, multi-tablet scheduling, cancellation, or process
lifecycle.

## Alternatives considered

- **Copy authority after connect:** rejected because allocation failure or caller mutation could
  detach the connection from the complete grouped authority validated before acquisition.
- **Create TLS before checking `SO_ERROR`:** rejected because writable nonblocking sockets can
  represent failed connections.
- **Reuse the ungrouped aggregate TCP client:** rejected because it cannot retain group-key
  authority or publish a data-dependent complete grouped stream.
- **Retry internally:** rejected because the finite sender must observe and bound every whole-
  attempt transport outcome.

## Consequences

Memory is one attempt, two authority vectors, shared query resource authority, TCP/TLS state, fixed
carrier scratch, and an unpublished independently bounded response prefix. Work is linear in the
request and complete response bytes plus constant connection state. Invalid authority opens no
socket. One event-loop thread serializes calls, so no synchronization or inter-thread memory-
ordering argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): acquisition changes neither Fragment-v2 nor
  `CHDVGRP2` bytes.
- [Invariant 6](../architecture/invariants.md): connect, handshake, exchange, frame, byte, group,
  key, aggregate, state, and query-memory bounds remain independently finite.
- [Invariant 10](../architecture/invariants.md): complete grouped authority and query resources stay
  attached to the exact attempt through TLS transfer.
- [Invariant 14](../architecture/invariants.md): request target, remote endpoint, and authenticated
  peer address remain exact.
- [Invariant 15](../architecture/invariants.md): TCP success never bypasses mutual TLS or target
  authorization.
- [Invariant 18](../architecture/invariants.md): move-only ownership, reverse-safe teardown, and
  single-thread affinity are explicit.

## Validation

A real IPv4 loopback listener progresses nonblocking connect into mutual TLS, authenticates both
certificate fingerprints and exact principal/node mappings, invokes fresh grouped authority binding
and execution once, and publishes the complete two-group stream only after terminal closure. A
focused case proves the exact sticky connect deadline closes the descriptor, leaves responses
unavailable, and rejects invalid limits and mismatched authority before acquisition. Both cases pass
normally and under ASan/UBSan. The complete cluster suite passes 240 of 240 and the allocation-
failure suite passes 31 of 31. Header self-containment, formatting, and whitespace checks pass. LLVM
18 static analysis remains blocked by the installed macOS 26 libc++ headers and reports no project-
local finding before those compiler errors.

## Migration and rollback

No wire or durable bytes change. Embeddings can replace caller-owned outbound connection joining
with this owner. Rollback restores that glue, which must preserve validation-before-connect,
authority/resource ownership, exact endpoint identity, `SO_ERROR` completion, sticky deadlines,
terminal-only publication, and TLS-before-descriptor teardown.

## Unresolved questions

- Bounded listener admission and accepted-connection ownership.
- Multi-address retries, whole-query cancellation, and all-tablet scheduling.
- Native SQL and multi-process compatibility qualification.

## References

- [Bounded grouped sufficient-state mutual TLS](0479-bounded-grouped-sufficient-state-mutual-tls.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
- [Network security boundary](../learning/network-security-boundary.md)
