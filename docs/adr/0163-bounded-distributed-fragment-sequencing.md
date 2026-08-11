# ADR 0163: Bounded distributed fragment sequencing

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0161](0161-canonical-distributed-aggregate-exchange-frame.md)

## Context

Exchange v1 carries a nonzero sequence and terminal bit, but the original coordinator accepted only
one terminal message per tablet and did not interpret the sequence. A retry could therefore be
mistaken for a conflicting result, a gap could be merged, and a worker disconnect after successful
terminal delivery could incorrectly fail the query.

## Decision

Each planned tablet owns one contiguous sequence starting at 1. The single-owner coordinator
accepts only the next sequence, merges its partial aggregate into tablet-local state, and marks the
tablet complete only after accepting a terminal message. A future sequence with a gap is
`UNAVAILABLE` and does not mutate state. A new sequence after terminal completion is invalid.

The coordinator retains every accepted message until query completion or destruction. A retry of
any retained sequence succeeds only when query/tablet identity, sequence, terminal flag, count,
optional-extrema presence, and every binary64 bit are identical. A different message at the same
sequence is `ALREADY_EXISTS` and leaves state unchanged. This bit-exact rule remains deterministic
for NaNs and signed zero.

Retention is bounded by both a per-fragment limit and a global limit, with a hard public ceiling of
65,536 messages. Configuration that cannot hold at least one message per planned fragment is
invalid. Exhaustion rejects before merge or retention. Allocation failure is reported as
`RESOURCE_EXHAUSTED` without partial state publication.

The first incomplete-worker failure wins and is never replaced by a later failure. Once a tablet's
terminal message is owned, its subsequent worker loss is harmless. `finish` returns no aggregate
until every tablet is terminal and fails with the retained first worker error when one exists.

## Consequences and validation

Exact retry history costs `O(accepted messages)` finite memory and permits deterministic duplicate
delivery across arbitrary prior sequences. Acceptance and retry lookup are constant time within a
tablet vector; final merging is linear in planned tablets. Calls are not internally synchronized:
one coordinator owner serializes admission, worker failure, and finish.

Tests cover gaps, exact nonterminal and terminal duplicates, conflicting same-sequence messages,
post-terminal output, multi-message merging, first-failure stability, completed-worker loss, finite
history exhaustion, and invalid ceilings. Full query, focused sanitizer, and installed-consumer
checks cover the public integration.

This does not define reconnection identity, durable query recovery, a resend request message,
connection authentication, or general physical fragment bytes.

Invariants 4–6, 8–10, 14, 15, and 18 apply.

## Migration and rollback

The Exchange v1 bytes do not change. Existing single-message workers remain compatible by sending
sequence 1 with `terminal` set. A rollback must not merge gaps or treat conflicting duplicates as
idempotent success.

## References

- [Distributed Aggregate Exchange v1](../formats/distributed-aggregate-exchange-v1.md)
- [Bounded distributed exchange partial I/O](0162-bounded-distributed-exchange-partial-io.md)
