# ADR 0363: Bounded distributed vector coordinator

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0163](0163-bounded-distributed-fragment-sequencing.md),
  [ADR 0350](0350-canonical-distributed-vector-batch-exchange.md)

## Context

Canonical vector exchange frames carried positive sequence and terminal state, but no owner could
reject gaps or conflicting retries, withhold output until every planned tablet closed, or bound the
retained variable-size batch history. Worker execution remains separately blocked on an explicit
result-schema identity contract; sequencing opaque canonical batch bytes does not require inventing
that contract.

## Decision

`DistributedVectorCoordinator` is a move-only, single-owner state machine over one query UUID and a
unique nonempty plan-ordered tablet vector. Each tablet admits a contiguous sequence starting at 1.
A retained sequence is idempotent only when identity, sequence, terminal state, and every encoded
batch byte are exact. Conflict is `ALREADY_EXISTS`, a gap is `UNAVAILABLE`, and new output after a
terminal is invalid.

Retention is bounded by per-tablet and total message counts plus total nested-batch bytes. Message
history retains the existing 65,536 hard ceiling; batch bytes default to 64 MiB and have a 1 GiB
hard ceiling. Exhaustion and allocation failure publish no message. The first incomplete-worker
failure remains authoritative, while loss after that tablet's terminal is harmless.

Rvalue `finish()` withholds output until every tablet is terminal, reserves the complete result
vector before moving retained messages into plan-tablet and sequence order, and rejects reuse after
success. The coordinator validates each nonempty message as canonical Columnar Batch v1 but treats
its schema-shaped bytes as opaque. It does not infer cross-batch result column identity, execute a
plan, or merge/order/limit rows.

## Consequences and validation

Two focused cases cover gaps, exact retries, conflicts, post-terminal output, incomplete
withholding, terminal-only empty streams, deterministic plan order, byte and message exhaustion,
invalid identity/tablet sets, first-failure stability, completed-worker loss, and one-shot finish.
All five vector-exchange cases, header self-containment, and installed consumption cover the public
coordinator and existing frame owners.

The schema-light result identity contract is implemented separately. Its carriage, authenticated
request lifecycle, vector worker execution, and process integration remain incomplete. No Phase 16
exit gate is claimed.

Invariants 4–6, 8–11, 14, 15, and 18 apply.

## References

- [Distributed Vector Exchange v1](../formats/distributed-vector-exchange-v1.md)
- [Bounded distributed fragment sequencing](0163-bounded-distributed-fragment-sequencing.md)
- [Canonical distributed vector-batch exchange](0350-canonical-distributed-vector-batch-exchange.md)
