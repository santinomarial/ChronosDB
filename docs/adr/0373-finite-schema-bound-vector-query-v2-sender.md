# ADR 0373: Finite schema-bound vector query v2 sender

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster, query, and networking maintainers
- **Extends:** [ADR 0169](0169-bounded-distributed-query-carrier-lifecycle.md),
  [ADR 0368](0368-schema-bound-distributed-vector-query-transport-v2.md),
  [ADR 0371](0371-deadline-bound-schema-bound-vector-query-v2-tcp-client.md)

## Context

The v2 TCP client owned one connection attempt and withheld incomplete response prefixes, but no
portable policy owner could construct immutable schema-bound attempts, exact-correlate a complete
response vector, independently bound retained count/bytes, apply finite retry/backoff, or expose an
advisory leader hint without mutating proof-bound authority. Letting socket code retry would
duplicate policy and risk carrying a response prefix into a later attempt.

## Decision

`DistributedVectorQuerySenderV2` is a move-only, single-threaded deterministic state machine for
one immutable Fragment-v2 dispatch. Creation validates the source, target, complete dispatch and
result schema, finite attempt and positive capped exponential-backoff limits, response-frame count,
and exact encoded response-byte bounds. `begin_attempt` permits only one outstanding attempt and
independently encodes one value-owned canonical request.

`accept_responses` accepts only a nonempty, terminally closed vector. Before state mutation, it
exact-matches every reverse route, query, tablet, payload query/tablet, one-based sequence, terminal
position, and Fragment-v2 result schema. Each response is canonically re-encoded against that
schema so constructed in-memory values cannot bypass descriptor validation. The sum of exact
encoded response bytes and response count have independent configured and hard ceilings. An empty
batch is valid only as the sole sequence-one terminal payload. A failure must be the sole response
and carry no payload; any hint must have nonzero node and placement epoch.

Only after the complete successful vector validates and copies does the sender publish its
value-owned result and enter `Succeeded`. Correlation, schema, sequence, count/byte, or allocation
failure publishes no prefix and leaves the attempt outstanding. Retryable `UNAVAILABLE`,
`RESOURCE_EXHAUSTED`, and `IO_ERROR` responses or transport failures schedule one whole new attempt
under finite capped backoff. Other statuses and attempt exhaustion are terminal.

Leader hints are exposed but never change the immutable target or dispatch. Following one requires
fresh external authority acquisition and a new execution owner. The sender owns no socket, clock,
coordinator, or durable state and changes no network bytes.

## Alternatives considered

- **Trust already-decoded response objects:** rejected because callers can construct those values
  directly and bypass the Fragment-v2 schema decoder.
- **Count only nested batch bytes:** rejected because headers and empty terminal/failure responses
  are real retained and transmitted bytes.
- **Retry inside the TCP client:** rejected because transport ownership must report one immutable
  attempt outcome without carrying policy or partial state.
- **Follow leader hints in place:** rejected because a hint is not committed placement authority.

## Consequences

Retained memory is one bounded dispatch, one optional bounded successful response vector, and
constant retry metadata. Each attempt allocates one bounded request. Validation transiently owns at
most one canonically encoded response while total retained publication stays under the exact byte
ceiling. Work is linear in complete response bytes. One owner thread serializes calls, so no
synchronization or memory-ordering argument is required.

ADRs 0375–0377 subsequently supply production row execution, inbound ownership, and pinned
coordinator delivery. Multi-tablet TCP scheduling, whole-query cancellation/deadlines, global
semantics, aggregate state, and process integration remain incomplete.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): retries reproduce the same v2 request bytes.
- [Invariant 6](../architecture/invariants.md): attempt, frame, byte, and backoff bounds are finite
  and independently enforced.
- [Invariant 10](../architecture/invariants.md): every accepted response is revalidated against the
  immutable Fragment-v2 result schema.
- [Invariant 14](../architecture/invariants.md): reverse route, query, tablet, sequence, and terminal
  correlation are exact across every whole attempt.
- [Invariant 15](../architecture/invariants.md): leader hints never rewrite committed authority.
- [Invariant 18](../architecture/invariants.md): single ownership and all-or-nothing result lifetime
  are explicit.

## Validation plan

Focused cases prove exact attempt bytes, two-frame and terminal-only success, schema/sequence/count/
byte rejection without state mutation, value-owned result retention, advisory hint capture, exact
capped backoff, immutable target selection, transport failure, terminal status, and attempt
exhaustion. Allocation injection covers canonical schema revalidation and result-copy publication.
Header self-containment, installed consumption, ASan/UBSan, relevant static analysis, formatting,
and the full serialized suite are required before completion.

## Migration or rollback considerations

No durable or wire migration exists. Schedulers can construct one sender per bound tablet and feed
it only complete TCP-client outcomes. Rollback returns finite retry, exact correlation, schema
revalidation, byte accounting, and advisory-hint handling to each embedding.

## References

- [Schema-bound distributed vector query transport v2](0368-schema-bound-distributed-vector-query-transport-v2.md)
- [Deadline-bound schema-bound vector query v2 TCP client](0371-deadline-bound-schema-bound-vector-query-v2-tcp-client.md)
- [Finite grouped-query sender](0339-finite-grouped-query-sender.md)
- [Bounded distributed vector coordinator](0363-bounded-distributed-vector-coordinator.md)
