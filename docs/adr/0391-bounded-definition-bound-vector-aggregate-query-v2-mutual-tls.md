# ADR 0391: Bounded definition-bound vector aggregate query v2 mutual TLS

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster, query, and networking maintainers
- **Extends:** [ADR 0370](0370-bounded-schema-bound-vector-query-v2-mutual-tls.md),
  [ADR 0388](0388-authenticated-vector-aggregate-query-receiver-v2.md),
  [ADR 0390](0390-finite-definition-bound-vector-aggregate-query-sender-v2.md)

## Context

Aggregate Fragment-v2 requests and `CHDVARP2` responses had bounded partial-I/O owners, an
authenticated receiver, a production worker, and finite retry policy, but no connected-session
owner joined them to mutual TLS. The aggregate response decoder requires both the exact bound
definition vector and query resource authority, neither of which the row TLS carrier owns. The
server also cannot reconstruct definitions from final result columns because COUNT, AVG, and
variance outputs erase input type information.

## Decision

`DistributedVectorAggregateQueryTlsClientV2` and
`DistributedVectorAggregateQueryTlsServerV2` are move-only, single-threaded owners for one already
connected nonblocking `TlsSocket`. Every readiness call performs at most one TLS operation. Positive
handshake and exchange deadlines use caller-supplied monotonic time; all terminal failures are
sticky.

Both sides complete mutual TLS and authenticate the verified certificate fingerprint before any
application byte. The client authorizes the server principal for the exact immutable attempt
target. The server authenticates the client before request read; the receiver then authorizes its
claimed source and local target.

Client creation exact-decodes the Fragment-v2 attempt, validates the fixed ungrouped definition
vector against its plan and result schema, and transfers that vector plus the query resource
context into the header-first aggregate response reader. Success requires exactly one state per
definition in ordinal order with exact reverse route, identity, sequence, and terminal position.
One failure response terminates the attempt. No decoded prefix is exposed; any TLS, integrity,
sequence, count, byte, or deadline failure clears all retained states and their query credit.

The receiver adds `receive_bound`, which returns the freshly authority-bound definitions beside
the complete encoded response vector. Its existing `receive` remains a convenience wrapper that
returns only encoded frames. The TLS server consumes the bound form, revalidates the definitions
against the decoded request, exact-decodes every response under a temporary bounded query context,
checks complete success/failure shape and correlation, and only then constructs all typed write
cursors. This prevents the server from guessing or detaching response definitions.

Both sides use fixed 16-KiB scratch arrays. Header-first readers allocate only an integrity-checked
exact current frame. Frame count, total outer encoded bytes, nested frame length, aggregate count,
state frame length, and variable extremum bytes are independent limits. TLS contexts, descriptors,
authenticators, authorizers, and the receiver remain embedding-owned and outlive the carrier. Retry,
connection acquisition, listener admission, multi-tablet coordination, and process lifecycle are
separate.

## Alternatives considered

- **Reuse the row TLS carrier:** rejected because it owns a result schema, not aggregate definition
  and query-memory authority.
- **Derive definitions at the server from result columns:** rejected because several operations'
  output descriptors do not identify their input type.
- **Trust receiver-produced encoded bytes:** rejected because the TLS boundary independently owns
  its count, byte, route, and definition contract.
- **Expose client frames incrementally:** rejected because later failure would leak a partial global
  aggregate contribution.

## Consequences

Retained client memory is one request cursor, one exact reader frame, fixed scratch, the bounded
decoded prefix, and constant metadata. Server memory is one request frame, fixed scratch, bound
definitions, complete encoded frames returned by the synchronous receiver, typed cursors, and one
temporary decoded state. Work is linear in complete request and response bytes. One event-loop
thread serializes calls, so no synchronization or memory-ordering argument is required.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): TLS changes no application framing or versions.
- [Invariant 6](../architecture/invariants.md): all untrusted allocations and retained prefixes have
  independent finite limits.
- [Invariant 10](../architecture/invariants.md): definition authority crosses both TLS endpoints and
  is reapplied before decode/write.
- [Invariant 14](../architecture/invariants.md): node route, query, tablet, aggregate ordinal,
  sequence, and terminal position are exact.
- [Invariant 15](../architecture/invariants.md): mutual authentication and node authorization
  precede application I/O.
- [Invariant 18](../architecture/invariants.md): socket/context borrowing, resource credit, and
  complete-vector lifetime are explicit.

## Validation plan

A real nonblocking socket pair must complete mutual TLS, authenticate both certificate
fingerprints, invoke definition binding/execution once, and expose the exact two-state response only
after both endpoints complete. Focused cases prove sticky exact deadline expiry, invalid definition
and count bounds, and prefix clearing under a total-byte limit between the first and second valid
frames. Run header self-containment, changed-file formatting/static analysis, ASan/UBSan, installed
consumer, and the full serialized suite.

## Migration or rollback considerations

No durable or wire bytes change. The receiver's new bound-result API is additive. Rollback removes
the TLS carriers and leaves aggregate remote execution disabled; embeddings must not substitute the
row carrier or unbound response decode.

## References

- [Bounded schema-bound vector query v2 mutual TLS](0370-bounded-schema-bound-vector-query-v2-mutual-tls.md)
- [Authenticated vector aggregate query receiver v2](0388-authenticated-vector-aggregate-query-receiver-v2.md)
- [Finite definition-bound vector aggregate query sender v2](0390-finite-definition-bound-vector-aggregate-query-sender-v2.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
