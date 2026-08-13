# ADR 0374: Bounded schema-bound vector result coordinator v2

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB cluster and query maintainers
- **Extends:** [ADR 0363](0363-bounded-distributed-vector-coordinator.md),
  [ADR 0366](0366-schema-bound-distributed-vector-result-exchange.md),
  [ADR 0373](0373-finite-schema-bound-vector-query-v2-sender.md)

## Context

The v1 vector coordinator sequenced table-shaped Columnar Batch messages but deliberately treated
their schemas as opaque. V2 senders now return schema-bound result-exchange values, yet no
multi-tablet owner retained the admitted result schema, validated every direct in-memory message,
arbitrated exact retries, bounded complete retained encoded bytes, or transferred schema and output
together only after all planned tablets closed.

## Decision

`DistributedVectorResultCoordinatorV2` is a move-only, single-threaded state machine over one query
UUID, one unique nonempty plan-ordered tablet vector, and one value-owned admitted result schema.
Each tablet admits a contiguous sequence starting at one. Before retention, every message is
canonically encoded as Result Exchange v2 against the owned schema, so a constructed in-memory
message cannot bypass descriptor validation. A retained sequence is idempotent only when identity,
sequence, terminal state, and every batch byte match; conflict is `ALREADY_EXISTS`, a gap is
`UNAVAILABLE`, and output after terminal is invalid. Empty bytes are valid only for the sole
sequence-one terminal of an empty tablet stream.

Retention is independently bounded by per-tablet and total message counts plus the sum of exact
canonical Result Exchange v2 frame bytes, including headers and terminal-only frames. Creation
requires enough byte budget for one minimum terminal per planned tablet and applies the existing
65,536-message and 1-GiB hard ceilings. Exhaustion and allocation failure publish no message. The
first incomplete-worker failure remains authoritative; loss after that tablet's terminal is
harmless.

Rvalue `finish` withholds output until every tablet terminates, reserves the complete result vector
before moving retained values into plan-tablet and sequence order, then transfers the admitted
schema and ordered messages together as `DistributedVectorQueryResultV2`. A failed result-vector
allocation leaves the coordinator retryable and its schema/messages intact. No row merge, global
sort/limit, socket, retry, or worker policy is owned here.

## Alternatives considered

- **Reuse the v1 coordinator:** rejected because it validates a different table-shaped batch format
  and intentionally owns no result-schema authority.
- **Return messages without their schema:** rejected because downstream code could lose the exact
  descriptor contract that admitted the batches.
- **Bound only nested payload bytes:** rejected because terminal-only messages and framing consume
  real retained and transmitted bytes.
- **Trust the sender's prior checks:** rejected because the coordinator is a public in-memory
  boundary and must validate direct callers independently.

## Consequences

Retained memory is one schema, one planned-tablet map, and canonical messages under count and exact
encoded-byte ceilings. Each admission transiently owns at most one canonical encoded frame for
validation. Work is linear in accepted and retried message bytes; final ordering is linear in
retained message count. One owner thread serializes calls, so no synchronization or memory-ordering
argument is required.

ADRs 0377 and 0378 subsequently supply pinned exactly-once sender delivery and multi-tablet TCP
scheduling with whole-query deadlines/cancellation. Global row/aggregate semantics, authority
rebinding, and process integration remain incomplete.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): retry identity is based on the canonical v2 frame.
- [Invariant 6](../architecture/invariants.md): planned tablets, messages, exact frame bytes, and
  allocation failure are bounded before publication.
- [Invariant 10](../architecture/invariants.md): the admitted schema remains owned through final
  result transfer and validates every message.
- [Invariant 11](../architecture/invariants.md): only all-tablet terminal closure publishes a
  result; the first incomplete-worker failure wins.
- [Invariant 14](../architecture/invariants.md): query, tablet, sequence, retry, and terminal
  correlation are exact.
- [Invariant 18](../architecture/invariants.md): ownership and single-thread lifetime are explicit.

## Validation plan

Focused cases cover gaps, exact retries, conflicts, schema mismatch, post-terminal output,
terminal-only empty streams, deterministic plan order, schema transfer, byte/message exhaustion,
invalid identity/tablet/schema sets, first-failure stability, completed-worker loss, and one-shot
finish. Allocation injection covers construction, admission with rollback, and retryable final
publication. Header self-containment, installed consumption, ASan/UBSan, relevant static analysis,
formatting, and the full serialized suite are required before completion.

## Migration or rollback considerations

No durable or wire migration exists. V2 execution owners can replace ad hoc result accumulation
with this coordinator. Rollback must preserve the schema lifetime, independent validation, exact
retry arbitration, count/byte bounds, all-tablet terminal publication, and first-failure rule.

## References

- [Bounded distributed vector coordinator](0363-bounded-distributed-vector-coordinator.md)
- [Schema-bound distributed vector result exchange](0366-schema-bound-distributed-vector-result-exchange.md)
- [Finite schema-bound vector query v2 sender](0373-finite-schema-bound-vector-query-v2-sender.md)
- [Distributed aggregate exchange learning guide](../learning/distributed-aggregate-exchange.md)
