# ADR 0138: FIFO-ordered Raft group observation

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** ChronosDB distributed-systems maintainers
- **Extends:** [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md) and
  [ADR 0135](0135-bounded-asynchronous-prepared-reconfiguration-admission.md)
- **Extended by:** [ADR 0139](0139-observation-driven-tablet-reconfiguration-reconciliation.md)

## Context

The asynchronous durable Multi-Raft owner exclusively holds the mutable runtime on its worker
thread. Producers can submit operations but cannot safely borrow `RaftNode` pointers to select a
leader, observe commit/application progress, or reconcile membership after prepared execution.
Reading a separate unsynchronized copy would race or return a state unrelated to FIFO operation
order.

The existing durable operation/result batch already supplies bounded admission, deterministic
ordering, completion lifetime, shutdown, and failure fanout. Observation should compose with that
boundary instead of adding an unbounded side channel or publishing internal node ownership.

## Decision

`RaftGroupObservation` is a bounded owning value containing group and local-node identity, role,
term and leader, last/commit/applied indexes, stable and active voter sets, joint old/new sets, and
joint/finalization flags. It deliberately excludes retained log payloads, pending messages, and
borrowed spans. Its allocation is bounded by the configured voter limit.

`ObserveGroupOperation` is an in-process durable-runtime operation. It creates no Raft transition,
durable record, outbound message, or synchronization request. `DurableRaftResult` carries either a
transition or an observation; a successful observation has only the latter. An unknown group is a
per-operation `NOT_FOUND` result and does not fail the runtime closed.

`AsyncDurableMultiRaftRuntime::try_observe_group` admits a one-operation batch through the existing
bounded FIFO. The worker observes the group only after all earlier admitted tasks. If the same batch
contains earlier persistent transitions, its completion remains withheld until the established
end-of-batch synchronization succeeds. The existing queue and completion mutex release/acquire
edges publish the owning observation; no new atomic or detached lifetime is introduced.

## Detailed rationale

Routing and reconciliation need a small stable summary rather than the full retained log. Copying
the bounded voter arrays prevents producer access to worker-owned storage. Reusing the operation
FIFO gives the observation an exact happens-after relationship with submitted work and preserves
one accounting/backpressure model.

The observation reports facts, not proofs. A leader field can become stale immediately after
completion, a commit index is not application visibility, and an applied index does not by itself
prove a particular external metadata value. Callers must validate term/epoch identity and retry
through authoritative reconciliation.

## Alternatives considered

- Return a borrowed `RaftNode` pointer was rejected because its lifetime and thread affinity are
  owned by the worker.
- Maintain an atomic mirror was rejected because membership vectors and related fields must form
  one coherent snapshot and would require another publication protocol.
- Copy full `PersistentState` was rejected because retained payload bytes are not bounded by the
  voter limit and are unnecessary for routing or reconciliation.
- Give observations a separate queue was rejected because order, capacity, shutdown, and failure
  semantics would diverge from execution.

## Consequences

Control-plane producers can observe current local leader, commit/application, and membership facts
without violating single-owner runtime affinity. Each observation consumes one pending batch and
operation until completion. It may allocate four bounded voter arrays, and a busy FIFO may delay it
behind earlier durable work.

This decision does not authenticate a remote node, guarantee that an observed leader remains
current, transmit prepared actions, apply metadata, or reclaim action-ledger evidence.

## Affected invariants

Invariants 1, 4, 5, 8, 11, and 18 apply. Observation never advances persistence or visibility,
worker ownership remains exclusive, one mutex-published value contains coherent related fields, and
the existing bounded admission contract remains authoritative.

## Validation plan

Synchronous durable-runtime coverage observes an uncommitted joint configuration after earlier
same-batch transitions, verifies every owning membership field, proves no extra physical sequence,
and treats a missing group as nonterminal. Asynchronous coverage queues election, proposal, apply,
observation, and missing-group observation without intervening waits, then proves FIFO state,
single-consumer completion, accounting, shutdown, and durable reopen. Full Raft suites remain
required.

## Migration or rollback considerations

No durable or wire bytes change. The operation and observation exist only in process. Older binaries
can reopen every state produced while it is used. Rollback removes the observation API but may not
replace it with cross-thread borrowed node access.

## Unresolved questions

Authenticated current-leader routing, observation deadlines/coalescing, completion-to-reactor
continuations, full metadata application reconciliation, and safe ledger reclamation remain Phase
16 work.

## References

- [Asynchronous Multi-Raft owner learning guide](../learning/asynchronous-multi-raft-owner.md)
- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)

## Retrospective (2026-08-10)

[ADR 0139](0139-observation-driven-tablet-reconfiguration-reconciliation.md) makes this owning
value an accepted input to durable tablet reconfiguration reconciliation. It preserves this ADR's
point-in-order, non-lease semantics while validating group and membership coherence before any
phase checkpoint or action-ledger change.
