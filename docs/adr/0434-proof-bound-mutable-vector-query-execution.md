# ADR 0434: Proof-bound mutable vector query execution

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB query, cluster, and distributed-systems maintainers
- **Extends:** [ADR 0377](0377-pinned-schema-bound-vector-query-v2-execution-owner.md),
  [ADR 0430](0430-distinct-mutable-vector-query-transport.md)

## Context

The mutable transport supplied one finite sender per proof-bound fragment, but an embedding still
had to correlate several tablet senders, publish each complete response stream exactly once, retain
one shared result schema and plan, and poison the whole query on terminal fragment failure. Reusing
the durable Fragment-v2 execution owner would falsely require a Manifest pin and reinterpret its
authority boundary.

## Decision

`DistributedMutableVectorQueryExecution` is a move-only, single-threaded portable owner. Creation
accepts a value-owned vector of distinct mutable fragments, validates every fragment, and requires
one query, database, table, destination schema, read policy, vector plan, result schema, and unique
plan-ordered tablet set. It creates one finite mutable sender per fragment and one bounded
schema-owning result coordinator over the exact tablet order.

Attempt creation, complete response acceptance, transport failure, backoff, terminal state, and
advisory leader hints delegate to the tablet sender. A successful sender publishes its complete
retained terminal stream to the coordinator exactly once. Terminal sender or coordinator failure
poisons completion exactly once. `finish` is unavailable until every tablet succeeds and then
transfers the common plan and plan-ordered schema-bound messages in the existing vector-v2 result
value, which can be consumed directly by bounded global row finalization.

Leader hints remain requests for fresh authority. They never rewrite a fragment's serving node,
placement epoch, exact applied position, or barrier. A later network/service owner must reacquire
and rebind the affected immutable fragment before sending to a different leader.

## Consequences

The coordinator now has one portable all-tablet correctness boundary independent of sockets and
durable Manifest ownership. It retains bounded sender and coordinator state and performs
`O(tablets log tablets)` creation plus `O(log tablets)` event lookup. One owner thread serializes
calls, so no inter-thread memory-ordering argument applies. It introduces no network or durable
format.

TCP scheduling, fresh-authority rebinding, SQL lowering, native response publication, and daemon
integration remain later composition boundaries.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): every fragment names only committed/applied state.
- [Invariant 6](../architecture/invariants.md): mixed query, schema, plan, or policy authority fails
  before execution.
- [Invariant 11](../architecture/invariants.md): fragments own every value retained by senders.
- [Invariant 15](../architecture/invariants.md): sender and coordinator messages and bytes remain
  bounded.
- [Invariant 18](../architecture/invariants.md): no partial result is published after any terminal
  fragment failure.

## Validation

Focused tests complete two tablet streams out of order and prove plan-ordered all-tablet
publication, incomplete refusal, foreign-tablet rejection, fresh-authority hint retention, bounded
retry to terminal failure, and rejection of mixed or duplicate authority. An allocation sweep
requires construction failures to return `RESOURCE_EXHAUSTED`. Header self-containment and
installed external consumption protect the public API.

## Migration and rollback

This is additive and is not yet installed in the native daemon. Rollback removes the portable
owner without changing fragment, request, response, or Native result bytes.

## References

- [Distinct proof-bound mutable vector fragment](0429-distinct-proof-bound-mutable-vector-fragment.md)
- [Distinct mutable vector query transport](0430-distinct-mutable-vector-query-transport.md)
- [Bounded global vector row finalization v2](0379-bounded-global-vector-row-finalization-v2.md)
