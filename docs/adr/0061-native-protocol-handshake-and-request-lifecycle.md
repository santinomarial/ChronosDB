# ADR 0061: Native Protocol Handshake and Request Lifecycle

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB networking and protocol maintainers
- **Extended by:** [ADR 0094](0094-native-protocol-1-1-subscriptions.md) and
  [ADR 0409](0409-source-tagged-native-subscription-changes.md)

## Context

Protocol v1 framing needs canonical payloads and a deterministic server-side lifecycle before a
reactor may dispatch work. Ambiguous request reuse or durability acknowledgements would make
cancellation races and write guarantees impossible to audit.

## Accepted decision

The first client frame is `CLIENT_HELLO` with request ID zero. It negotiates Protocol 1.0, zero v1
feature bits, and the smaller of the client/server payload limits. Client request IDs for ingest
and query are positive and strictly increase for the life of a connection; they are never reused.
At most a configured finite number are active. `CANCEL` names an issued ID, is idempotent after the
first cancellation/completion, and never makes a higher unseen ID valid.

Ingest carries one canonical Columnar Append application payload plus an explicit `ASYNC` or
`LOCAL_SYNC` request. Its acknowledgement repeats requested and effective modes, distinguishes a
new apply from a matching retry, and includes a physical WAL position only for a new apply. Query
requests contain exact nonempty UTF-8 SQL. Errors have stable v1 codes and UTF-8 diagnostics.

The portable state machine validates direction, phase, payload, negotiated limits, monotonic IDs,
and in-flight admission before dispatch. Closing releases its active set and permanently rejects
new input.

## Detailed rationale

Strictly increasing IDs eliminate the late-completion ambiguity created by reusing an ID after
cancellation. A fixed-capacity active set bounds connection memory. Repeating both durability modes
on the wire prevents an implementation from silently acknowledging a weaker guarantee.

## Alternatives considered

- Reusable IDs require generations or unbounded retired-ID tracking.
- Implicit handshake from the frame version cannot negotiate resource limits.
- A generic opaque acknowledgement cannot prove the durability boundary.
- JSON payloads still need bounds/versioning and weaken canonical fixture coverage.

## Consequences

Clients must reconnect before the u64 request sequence is exhausted. Matching retries return their
logical prior outcome without inventing a new WAL position. Authentication remains a separate gate
inserted between hello and active request admission.

## Affected invariants

This decision supports invariants 1, 9, 14, and 17 through mode-explicit acknowledgement,
canonical retry transport, version negotiation, and bounded lifecycle state.

## Validation plan

Golden and hostile payload tests; phase/direction/duplicate/saturation/cancel/close tests;
allocation sweeps; fragmented connection parsing, fuzzing, reactor integration, and sanitizer runs.

## Deferred decisions

Streaming input/output buffering, shard routing, query result batch encoding, authentication/TLS,
timeouts, and socket overload actions remain subsequent Phase 10 increments.

## Migration or reversal implications

Changing payload fields, durability values, error codes, or request-ID reuse rules requires a new
protocol major version and superseding ADR.

## References

- [Native Protocol v1](../protocol/native-v1.md)
- [ADR 0060](0060-native-protocol-v1-framing.md)
- [WAL durability](0006-wal-durability-and-group-commit.md)
