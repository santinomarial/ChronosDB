# ADR 0134: Sealed local tablet reconfiguration execution

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0071](0071-segmented-multi-raft-persistence.md) and
  [ADR 0133](0133-prepared-tablet-reconfiguration-dispatch.md)

## Context

ADR 0133 introduced a dispatch result intended to prove that an exact reconfiguration action was
durably prepared before release. Its initial aggregate representation remained publicly
constructible, however, so an in-process caller could label an arbitrary action as prepared without
using the ledger. There was also no narrow adapter from a genuinely prepared action to the existing
synchronous `DurableMultiRaftRuntime`; callers would have to extract and submit the raw request,
again weakening the boundary.

## Decision

`PreparedTabletReconfigurationDispatch` is a sealed, move-only capability. Only the internal factory
used after successful ledger `prepare` can construct it. Public access is read-only, and moving it
invalidates the source so execution rejects moved-from capabilities.

`execute_local_prepared_tablet_reconfiguration` accepts only a valid sealed capability and one
single-thread-affine synchronous `DurableMultiRaftRuntime`. It verifies that the preparation receipt
and action identity agree, copies the exact prepared request into a one-operation durable batch, and
returns the one `DurableRaftResult`. The runtime's established contract appends every persistent
transition and synchronizes the physical log before the result or any outbound messages become
observable.

The outer `Result` reports adapter or top-level durable-runtime failure. A successful outer result
still contains the operation's own `status`, which may reject the request without a transition.
Neither success means that an entry committed or applied. The caller must deliver returned outbound
messages, observe authoritative Raft/metadata state, and reconcile again.

## Detailed rationale

A sealed capability makes bypass harder to express and gives local execution a precise admissible
input. Reusing the existing durable runtime preserves one persistence implementation and its
persist-before-outbound proof. Copying rather than consuming the request leaves the capability
available when local submission is rejected and supports explicit reconciliation-driven retry.

## Alternatives considered

- Keep a public aggregate and rely on naming was rejected because it did not enforce the documented
  preparation boundary.
- Submit a raw `DurableRaftRequest` was rejected because it loses the connection to ledger
  preparation.
- Mark the ledger file complete after local submission was rejected because submission is not
  commit or application and the authoritative state machines already own that proof.
- Add remote or asynchronous execution here was deferred because routing, authentication,
  backpressure, completion lifetime, and duplicate delivery require separate contracts.

## Consequences

Local control-plane code can execute prepared actions without releasing outbound work before local
durability. Tests and lower-level recovery code may still use raw reconciliation, but it cannot
fabricate the sealed production dispatch through the public interface. The adapter allocates one
request copy and preserves the runtime's thread-affinity requirement.

This boundary does not suppress a second execution call, authenticate a source, select a remote
leader, deliver messages, prove quorum commit, apply metadata, or reclaim ledger evidence.

## Affected invariants

Invariants 1, 4, 5, 8, 9, 10, 11, 14, and 18 apply. Ledger preparation is required by construction,
the single durable owner retains persist-before-outbound ordering, and commit/application are not
inferred from local persistence.

## Validation plan

Compile-time checks prove that the capability is neither an aggregate nor publicly default/copy
constructible. A real-filesystem test prepares a single-voter membership action, rejects execution
through its moved-from source, executes the valid capability, observes the persistent transition
only after the durable sequence advances, closes the runtime, and reopens the exact retained entry.
Existing durable-runtime and full Raft suites remain required.

## Migration or rollback considerations

No durable or wire format changes. Source callers must replace aggregate field access with read-only
accessors and must obtain capabilities from the prepared reconciliation wrapper. Rollback may use
the same ledger bytes but must retain an equivalent explicit prepare-before-execute sequence.

## Unresolved questions

Asynchronous-owner admission, authenticated current-leader routing, duplicate-delivery behavior,
commit/application completion, and evidence reclamation remain Phase 16 work.

## References

- [ADR 0114: Bounded asynchronous Multi-Raft owner](0114-bounded-asynchronous-multi-raft-owner.md)
- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
- [Segmented Multi-Raft Persistent Log](../learning/raft-persistent-log.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
