# ADR 0390: Finite definition-bound vector aggregate query sender v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and query maintainers
- **Extends:** [ADR 0373](0373-finite-schema-bound-vector-query-v2-sender.md),
  [ADR 0383](0383-owned-cross-tablet-vector-aggregate-definitions.md),
  [ADR 0387](0387-definition-bound-vector-aggregate-query-response-v2.md)

## Context

The aggregate receiver and response carrier could return a complete definition-bound state vector,
but no portable policy owner preserved its exact request bytes, definition authority, query-memory
authority, retry budget, and accepted states together. Reusing the row sender would detach variable
aggregate extrema from query accounting and would not enforce the fixed one-state-per-definition
shape. Letting a socket owner retain decoded prefixes would also allow memory and state from one
failed attempt to cross a retry boundary.

## Decision

`DistributedVectorAggregateQuerySenderV2` is a move-only, single-threaded finite state machine for
one Fragment-v2 ungrouped aggregate dispatch. Creation exact-validates the dispatch, ordered
definition vector, result schema, source/target route, query decode limits, fixed response width,
exact encoded-byte ceiling, attempt count, and positive capped exponential backoff. It encodes and
retains the immutable `CHDVREQ2` request once. Each attempt receives a value-owned copy of those
exact bytes; allocation failure does not start an attempt.

The sender owns the complete definition vector and a copy of the query resource context. A success
must contain exactly one `CHDVARP2` response per definition in ordinal order, with exact reverse
route, query/tablet identity, sequence, terminal position, no leader hint, and aggregate operation,
input, and state definition. Each in-memory response is canonically re-encoded and decoded again
under the owned resources. This prevents constructed values from bypassing the wire codec and makes
retained variable extrema acquire query credit at the ownership boundary. Count and total outer
frame bytes are independently bounded.

Only after every state reconstructs does the sender atomically publish the vector. A validation,
limit, or allocation failure leaves the current attempt pending and destroys all temporary states,
returning their query credit. One failure response may carry an advisory leader hint. Retryable
`UNAVAILABLE`, `RESOURCE_EXHAUSTED`, and `IO_ERROR` outcomes schedule a whole new immutable attempt;
all other statuses and attempt exhaustion terminate. Hints never rewrite the admitted target.

## Alternatives considered

- **Reuse the row sender:** rejected because row batches are copyable bytes and have no merge-state
  definition or query-memory ownership.
- **Trust decoded response objects:** rejected because directly constructed values could bypass
  canonical definition/state validation.
- **Retain each state as it arrives:** rejected because a later invalid state would expose a prefix
  and retain memory from a failed attempt.
- **Follow a leader hint in place:** rejected because a hint is not committed placement authority.

## Consequences

The sender retains one bounded dispatch, request, definition vector, resource handle, and optional
complete result. Validation transiently holds one encoded outer frame and the reconstructed prefix;
failure destroys that prefix before returning. Work is linear in complete encoded response bytes.
One owner thread serializes calls, so no synchronization or memory-ordering argument is required.
TLS, TCP, multi-tablet scheduling, coordination, finalization, and process ownership remain separate.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): every retry reproduces the same canonical request.
- [Invariant 6](../architecture/invariants.md): attempts, backoff, frames, bytes, and nested state are
  independently finite.
- [Invariant 10](../architecture/invariants.md): accepted states are reconstructed against the exact
  Fragment-v2-derived definition vector.
- [Invariant 14](../architecture/invariants.md): route, query, tablet, ordinal, sequence, and terminal
  correlation are exact.
- [Invariant 15](../architecture/invariants.md): advisory hints do not mutate placement authority.
- [Invariant 18](../architecture/invariants.md): request, definitions, reservations, and result
  lifetime have one explicit owner.

## Validation plan

Prove exact immutable attempt bytes, complete success, sequence/width/count/byte rejection without
publication, finite capped backoff, advisory hint retention, immutable target selection, terminal
failure, and invalid construction. Allocation injection must cover canonical re-encoding, decoding,
result publication, and exact query-credit release on every failed path. Header self-containment,
formatting, static analysis, ASan/UBSan, installed consumption, and the full serialized suite are
required.

## Migration or rollback considerations

No durable or wire bytes change. Later schedulers may create one sender per definition-bound tablet
and pass only its complete terminal vector to the aggregate coordinator. Rollback removes that
policy owner; aggregate TCP execution must then remain disabled rather than retry in socket code.

## References

- [Finite schema-bound vector query v2 sender](0373-finite-schema-bound-vector-query-v2-sender.md)
- [Definition-bound vector aggregate query response v2](0387-definition-bound-vector-aggregate-query-response-v2.md)
- [Bounded vector aggregate coordinator v2](0385-bounded-vector-aggregate-coordinator-v2.md)
- [Distributed Vector Aggregate Query Transport v2](../formats/distributed-vector-aggregate-query-transport-v2.md)
