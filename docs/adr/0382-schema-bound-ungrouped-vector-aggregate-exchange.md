# ADR 0382: Schema-bound ungrouped vector aggregate exchange

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-query maintainers
- **Extends:** [ADR 0381](0381-canonical-mergeable-vector-aggregate-state-bytes.md)

## Context

Mergeable Vector Aggregate State v1 preserves sufficient partial state, but intentionally has no
query, tablet, aggregate position, terminal, or fragment-schema authority. Sending those nested
bytes directly would permit a valid state to be replayed at the wrong query, tablet, or aggregate
definition. Reusing row Result Exchange v2 would prematurely finalize AVG, variance, and exact
sums. Adding an optional group key would also make one frame ambiguous across ungrouped and future
multi-key grouped execution.

## Decision

Distributed Vector Aggregate Exchange v1 is a distinct ungrouped envelope around exactly one
Mergeable Vector Aggregate State v1. Its fixed 96-byte header binds nonzero query and tablet IDs,
zero-based aggregate ordinal, exact aggregate count, canonical `ordinal + 1` sequence, terminal
position, nested length, and independent nested/header checksums. A final checksum covers the
complete frame.

Encode, exact decode, fragmented reader, and short-write cursor require the complete ordered
aggregate-definition vector derived from the admitted fragment. Count and position must match that
vector, and the decoded nested definition must equal the selected definition exactly. A query-layer
binder derives this vector only after validating the ungrouped plan, projected input shapes, and
Fragment-v2 result schema together.

Header integrity and hard/caller limits pass before frame allocation. Complete and nested integrity
pass before nested decode; variable extrema continue to use query-accounted memory. The reader
owns one frame and leaves coalesced successor bytes caller-owned. Failures are sticky.

Grouped plans are rejected. Group keys, group cardinality, and grouped stream termination require a
separate contract because they alter correlation and ordering semantics. This exchange also does
not arbitrate retries or merge a multi-frame stream; those are coordinator responsibilities.

## Consequences and validation

Workers can now transport every ungrouped partial without finalization while receivers prove the
exact fragment-authorized operation and input type. The maximum frame is 1,048,792 bytes and
deployment bounds may be lower. Encoding and decoding are O(frame bytes). Objects are
thread-affine; no inter-thread memory-ordering argument applies.

Focused tests freeze all outer fields and checksums, bind a mixed COUNT/SUM/MAX schema, exact-
round-trip states, reject wrong definitions, noncanonical positions, versions, reserved bytes,
truncation, trailing bytes, damage, and lower bounds, enumerate every two-part split with a
coalesced successor, and prove sticky failure and cursor ownership. Allocation-failure injection
classifies all owned allocation failures and verifies query-credit release. A deterministic
libFuzzer target covers arbitrary exact/fragmented input and canonical mutations.

Worker aggregate execution, cross-tablet coordination, global merge/order/finalization, grouped
exchange, authority rebinding, and process integration remain separate tasks. No Phase 16 exit gate
is claimed.

Invariants 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Vector Aggregate Exchange v1](../formats/distributed-vector-aggregate-exchange-v1.md)
- [Mergeable Vector Aggregate State v1](../formats/mergeable-vector-aggregate-state-v1.md)
- [Canonical aggregate-state bytes](0381-canonical-mergeable-vector-aggregate-state-bytes.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)

