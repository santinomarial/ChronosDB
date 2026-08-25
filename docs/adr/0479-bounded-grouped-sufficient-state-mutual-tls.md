# ADR 0479: Bounded grouped sufficient-state mutual TLS

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, query, networking, and security maintainers
- **Extends:** [ADR 0391](0391-bounded-definition-bound-vector-aggregate-query-v2-mutual-tls.md),
  [ADR 0477](0477-distinct-grouped-sufficient-state-response-v2.md), and
  [ADR 0478](0478-authenticated-complete-grouped-state-attempts.md)

## Context

Grouped Fragment-v2 requests, `CHDVGRP2` responses, authenticated receiver-side authority binding,
and finite sender attempts existed without an owner for one connected mutual-TLS session. The row
and ungrouped aggregate carriers cannot be reused: neither owns the complete ordered group-key plus
aggregate authority required to decode grouped frames, and the number of group responses is
data-dependent. Empty input also closes with one distinct terminal rather than zero frames.

## Decision

Add move-only, single-thread-affine grouped TLS client and server owners around one already connected
nonblocking `TlsSocket`. Each readiness call performs at most one TLS read, write, or handshake
operation. Positive handshake and exchange deadlines use caller-supplied monotonic time and every
terminal failure is sticky.

Both endpoints finish mutual TLS and authenticate the verified certificate fingerprint before any
application byte. The client additionally authorizes the server principal for the immutable attempt
target before writing the request. The server authenticates the client before reading the request;
the receiver subsequently authorizes the claimed source and local target before binding or execution.

Client construction exact-decodes the Fragment-v2 attempt, validates complete grouped authority
against its plan and result schema, and transfers that authority plus query resources into the
header-first response reader. A successful stream is either one empty terminal or a contiguous
nonempty sequence whose declared group count is stable, bounded, and closes exactly. Reverse route,
query, tablet, ordinal, sequence, and terminal correlation are exact. One failure response closes the
attempt. No decoded prefix is observable; TLS, integrity, sequence, count, byte, allocation, or
deadline failure clears all retained responses.

The server consumes `receive_bound`, revalidates the returned authority and response vector against
the decoded request and independent TLS limits, exact-decodes every response under temporary query
memory, and constructs every write cursor before exposing the first response byte. Both owners use
fixed 16-KiB scratch arrays. Frame count, total bytes, grouped nested limits, key payload, key and
aggregate widths, state limits, and query memory remain independently finite.

TLS contexts, descriptors, authenticators, authorizers, and the receiver remain embedding-owned and
outlive the carrier. Connection acquisition, listener admission, route rotation, retry decisions,
multi-tablet scheduling, cancellation, and process lifecycle remain separate.

## Consequences

The carrier preserves grouped authority across the authenticated session and cannot confuse grouped
states with row or ungrouped-state responses. Client memory is one request cursor, exact response
reader, fixed scratch, and an unpublished bounded response prefix. Server memory is one request
reader, fixed scratch, one complete receiver result, temporary exact decode, and complete write-
cursor vector. Work is linear in complete request and response bytes.

One event-loop thread serializes mutation, so no synchronization or inter-thread memory-ordering
argument applies.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): TLS changes neither application magic nor framing.
- [Invariant 6](../architecture/invariants.md): frame, byte, group, key, aggregate, state, and
  temporary query-memory bounds are independent.
- [Invariant 10](../architecture/invariants.md): complete grouped authority crosses and is reapplied
  at both TLS endpoints.
- [Invariant 14](../architecture/invariants.md): route, query, tablet, group count, ordinal,
  sequence, terminal, and attempt correlation are exact.
- [Invariant 15](../architecture/invariants.md): mutual authentication and target authorization
  precede application I/O.
- [Invariant 18](../architecture/invariants.md): socket/context borrowing, decoded credit, cursors,
  and unpublished-prefix lifetime are explicit.

## Validation

Real nonblocking socket pairs prove mutual TLS, both certificate fingerprints, exact target
authorization, one receiver invocation, complete two-group publication, and the distinct empty
terminal. Focused cases prove exact sticky deadline expiry, invalid grouped authority and bounds,
server-principal rejection before request write, and complete prefix clearing when the client byte
budget expires between valid frames. All 16 focused grouped transport, TLS, and execution cases pass
under ASan/UBSan. The complete cluster suite passes 238 of 238 and the complete cluster allocation-
failure suite passes 31 of 31. Header self-containment, formatting, and whitespace checks pass.
LLVM 18 static analysis remains blocked by the installed macOS 26 libc++ headers and reported no
project-local finding in the changed source or test before those compiler errors.

## Migration and rollback

No wire or durable bytes change. Rollback removes the grouped TLS carrier while retaining the codec,
receiver, sender, and in-process execution owner. Deployments must not route grouped frames through
the row or ungrouped aggregate TLS carrier.

## Unresolved questions

- Nonblocking TCP connection and listener ownership.
- Multi-address retries, deadline/cancellation, and all-tablet scheduling.
- Native SQL and multi-process compatibility qualification.

## References

- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
- [Distributed Vector Grouped Aggregate Exchange v1](../formats/distributed-vector-grouped-aggregate-exchange-v1.md)
- [Network security boundary](../learning/network-security-boundary.md)
