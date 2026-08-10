# ADR 0135: Bounded asynchronous prepared reconfiguration admission

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** ChronosDB distributed-systems maintainers
- **Extends:** [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md) and
  [ADR 0134](0134-sealed-local-tablet-reconfiguration-execution.md)
- **Extended by:** [ADR 0136](0136-idempotent-retained-reconfiguration-action-replay.md) and
  [ADR 0138](0138-fifo-ordered-raft-group-observation.md) and
  [ADR 0139](0139-observation-driven-tablet-reconfiguration-reconciliation.md)

## Context

The sealed prepared-dispatch capability can execute through the synchronous durable Multi-Raft
owner, but that owner may block while appending and synchronizing its physical log. Reactor and
control-plane producer threads need the established bounded asynchronous owner without extracting a
raw request or weakening prepare-before-execute.

## Decision

`try_submit_local_prepared_tablet_reconfiguration` accepts one valid sealed dispatch and one
`AsyncDurableMultiRaftRuntime`. It exact-copies the capability's request into a one-operation batch
and uses the existing nonblocking `try_submit` boundary. Successful admission returns the existing
single-consumer `AsyncDurableRaftCompletion`; the worker remains the sole owner of the synchronous
runtime and physical log.

The capability is borrowed, not consumed. Invalid/moved-from input fails before admission.
Allocation, closed admission, terminal failure, and capacity rejection return an error while leaving
the capability valid for an explicit later retry. Successful admission transfers only the request
copy; callers retain the capability as evidence and must not infer execution until consuming the
completion.

Completion retains ADR 0114 semantics: its wait/poll publication occurs after the worker's durable
batch finishes, so any returned outbound messages crossed the local persist-and-sync boundary.
Per-operation status remains distinct from top-level completion success, and neither is proof of
quorum commit or state-machine application. Reactor threads must poll or hand the completion to a
non-reactor continuation rather than block in `wait`.

## Detailed rationale

Adapting the sealed capability at the existing admission point preserves bounded FIFO ownership,
backpressure metrics, shutdown, and thread-affinity proofs. Copying before admission means rejection
does not consume the only retry handle. A dedicated one-operation helper prevents production code
from unpacking the prepared action merely to call the generic queue.

## Alternatives considered

- Call synchronous execution from a reactor was rejected because disk synchronization can block
  event progress.
- Move the capability into the queue was rejected because a capacity or shutdown rejection would
  complicate ownership recovery without improving execution safety.
- Add an unbounded retry side queue was rejected because it would convert overload into hidden
  memory and latency growth.
- Wait inside the helper was rejected because it would erase the nonblocking admission contract.

## Consequences

Control-plane producers can submit prepared local actions with explicit overload and retain an
owning completion whose lifetime may outlast the runtime. The action request is copied once before
queue ownership. Existing async metrics account for the batch and operation without a separate
reconfiguration queue.

This boundary does not expose an authoritative Raft snapshot from the worker, select or authenticate
a remote leader, transmit messages, suppress duplicate execution, apply committed metadata, or
reclaim ledger evidence.

## Affected invariants

Invariants 1, 4, 5, 8, 9, 10, 11, 14, and 18 apply. Bounded admission prevents hidden queues, mutex
publication preserves request/result ownership, the single worker retains physical ordering, and
prepared intent remains separate from execution/commit/application evidence.

## Validation plan

A real-filesystem test elects the async single-owner runtime, submits a sealed ledger-prepared
membership action, consumes its successful persistent transition, shuts down, proves a later
submission is rejected without invalidating the capability, and reopens the exact retained log
entry. Existing async-owner and full Raft suites remain required.

## Migration or rollback considerations

No durable, wire, or queue format changes. Existing generic async submissions remain valid. Rollback
can submit the same decoded request only if it preserves an equivalent sealed prepare-before-admit
boundary.

## Unresolved questions

Worker-state observation for reconciliation, authenticated current-leader routing, duplicate
delivery, remote retry/backoff, completion-to-reactor integration, and evidence reclamation remain
Phase 16 work.

## References

- [Asynchronous Multi-Raft Owner](../learning/asynchronous-multi-raft-owner.md)
- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)

## Retrospective (2026-08-10)

[ADR 0138](0138-fifo-ordered-raft-group-observation.md) supplies the worker-state observation left
unresolved here. A producer can now enqueue a bounded owning group observation behind admitted
execution without borrowing the worker-owned runtime. Authenticated routing and complete
reconfiguration application reconciliation remain separate work.

[ADR 0139](0139-observation-driven-tablet-reconfiguration-reconciliation.md) then connects that
owning observation to the existing durable reconciliation and next-action preparation path, closing
the local async application-observation loop without changing this admission contract.
