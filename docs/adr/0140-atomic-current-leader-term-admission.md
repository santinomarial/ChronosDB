# ADR 0140: Atomic current-leader term admission

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** Raft and distributed-runtime subsystems
- **Supersedes:** none

## Context

Prepared tablet-reconfiguration actions can be retried and routed to a node believed to lead their
destination group. An observation followed by a separate admission is insufficient: another
producer can enqueue a higher-term transition between those operations. Checking a cached role or
term outside the single-owner runtime would therefore permit stale routed work to mutate Raft.

## Decision

`DurableRaftRequest` may carry a nonzero `required_leader_term`. The durable Multi-Raft owner checks
the target group, local leader role, and exact current term immediately before dispatching that
operation. A missing group returns `NOT_FOUND`; a follower or term mismatch returns `UNAVAILABLE`;
zero is invalid. All rejections have no transition, persistence, outbound messages, or group
mutation and do not fail the runtime.

The check and operation dispatch occur on the same exclusive owner thread with no intervening queue
boundary. The asynchronous owner preserves the same guarantee because it moves accepted FIFO work
to that durable owner. The precondition is an admission fence, not a lease: success does not prove
that the node remains leader after the operation, nor that a resulting entry has committed.

## Alternatives considered

- **Check a prior group observation:** rejected because admission can race a later queued term
  change.
- **Let each transport operation implement its own term check:** rejected because it duplicates a
  subtle safety boundary and can omit new operation kinds.
- **Reject a mismatch as a terminal runtime failure:** rejected because stale routing is expected
  during leader changes and does not imply local corruption.

## Consequences

Current-leader transports can bind decoded work to the exact routing term without adding transport
semantics to the deterministic Raft core. Callers must retry `UNAVAILABLE` only after refreshing
routing; exact-action replay rules remain responsible for duplicate suppression.

## Affected invariants

This decision supports invariants 11, 12, and 16 by preserving Raft leadership and ordering at the
single-owner boundary while keeping overload and stale routing explicit.

## Validation

Focused durable-runtime tests prove that follower, stale-term, and zero-term requests append
nothing and do not advance the durable physical sequence, while an exact current-term request is
persisted normally. Existing asynchronous-owner tests exercise the same durable dispatch path.

## References

- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0135](0135-bounded-asynchronous-prepared-reconfiguration-admission.md)
- [ADR 0139](0139-observation-driven-tablet-reconfiguration-reconciliation.md)
- [Architecture invariants](../architecture/invariants.md)
