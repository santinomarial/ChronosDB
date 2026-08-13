# ADR 0324: Bounded grouped FLOAT64 coordinator

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0163](0163-bounded-distributed-fragment-sequencing.md),
  [ADR 0320](0320-canonical-nullable-float64-grouped-exchange.md),
  [ADR 0322](0322-distinct-empty-grouped-stream-terminal.md)

## Context

The grouped partial and empty-stream terminal formats could carry bounded canonical values but no
coordinator could reject gaps/conflicting retries, distinguish an empty tablet from a NULL group,
or merge equal keys across tablets. Publishing groups before every tablet closed would expose
partial distributed success.

## Decision

`DistributedGroupedFloat64Coordinator` is a move-only, single-owner state machine over an explicit
query UUID and unique planned tablet set. Each tablet stream starts at sequence 1 and admits only
the next message. Every accepted grouped partial or terminal-only frame is retained under the same
per-fragment and global limits as the ungrouped coordinator. A prior sequence succeeds only when
its canonical value is exact; a conflicting grouped/terminal form or partial is `ALREADY_EXISTS`,
a gap is `UNAVAILABLE`, and output after closure is invalid.

Grouped keys are canonicalized before retry retention and grouping: NULL is distinct, signed zeros
share positive-zero bits, and every NaN shares the frozen quiet-NaN token. Equal keys merge through
the existing count/sum/minimum/maximum/Welford state. The terminal-only frame is accepted only as
sequence 1 of an otherwise empty tablet stream. A nonempty stream closes only through a terminal
grouped partial, so a terminal can never fabricate or erase a real group.

`finish()` returns no groups until every planned tablet closes and the first incomplete-worker
failure remains sticky. Completed-worker loss is harmless. Result iteration is deterministic by
the canonical key token, with NULL first; it is an internal reproducibility order, not SQL
`ORDER BY`. Merge overflow and allocation failure fail closed without publishing a partial result.

## Consequences and validation

The first nullable-FLOAT64 grouped exchange now has bounded sequencing, exact retry arbitration,
empty-tablet completion, cross-tablet merging, and all-fragment completion. Retained history is
`O(accepted messages)` under the 65,536 hard ceiling. Tablet-local and final group maps are
`O(groups)`; admission and merging are `O(log groups)`.

Four focused coordinator cases cover canonical signed-zero retries and NaN output, NULL merging,
sequence gaps/conflicts/post-terminal output, terminal-only empty results, cross-form conflicts,
invalid limits, history exhaustion, first-failure stability, completed-worker loss, and final count
overflow. The installed-consumer gate references construction, both admission forms, and finish.

This does not define authority/type binding, group-scoped executable dispatch, a unified stream
discriminator, authenticated grouped transport, multi-key/non-FLOAT64 state, SQL ordering, top-N,
LIMIT, durable query recovery, or broad multi-node failure evidence. The first structural grouped
fragment intent is the accepted follow-up in
[ADR 0325](0325-distinct-grouped-float64-fragment-intent.md). No Phase 16 exit gate is claimed.

Invariants 4–6, 8–11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Aggregate Exchange
  v1](../formats/distributed-grouped-float64-exchange-v1.md)
- [Bounded distributed fragment sequencing](0163-bounded-distributed-fragment-sequencing.md)
- [Distinct empty grouped-stream terminal](0322-distinct-empty-grouped-stream-terminal.md)
