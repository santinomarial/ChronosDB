# ADR 0238: Fail-Closed Subscription Continuity Loss

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB live-query, ingest, and recovery maintainers

## Context

A storage mutation is already committed and applied before its plan-bound live result is evaluated.
Evaluation can still fail because of resource, output-size, cancellation, or internal errors. The
write path cannot truthfully reject that mutation, but ignoring the missing result would leave
active and resumable subscriptions claiming a gap-free suffix.

Publishing a fabricated empty result is incorrect because the failed plan may have matched rows.
Keeping older retained changes is also insufficient: every token whose safe vector precedes the
missing position requires a suffix that can no longer be supplied.

## Decision

`MultiTabletSubscriptionManager::mark_continuity_lost` accepts only the exact next position of a
configured source. It advances that source, clears the complete cross-tablet retained admission
order, expires every source through its current latest position, and transitions every active
snapshot/live subscriber to `OVERFLOWED`. It publishes no logical change.

Old tokens consequently fail resume with `NOT_FOUND`; active clients receive the existing
overflow terminal contract. A new subscription may register at the resulting current position
vector and execute a fresh historical snapshot, after which normal consecutive publication can
continue. Schema-invalidated coordinators remain terminal and do not accept continuity loss as a
way to reactivate an old plan.

`DurableMultiTabletSubscription` exposes the same transition and marks its checkpoint state dirty.
The existing checkpoint source latest/expiry fields and empty retained suffix represent the state
without a durable format change.

## Consequences

Post-apply live failure cannot block or falsify ingestion, and it cannot silently create a
resumable gap. The blast radius is the complete plan coordinator because its cross-tablet delivery
order is one retained history; preserving unrelated entries while losing one position would not
make old delivery sequences replayable.

The transition does not choose logging, metrics, retry, or evaluator fan-out policy. The applied-
append observer must invoke it for each affected coordinator when evaluation/publication fails.

## Validation

Focused tests publish one source, lose the exact next position on another, verify active overflow,
empty retained state and fully advanced expiry frontiers, reject the old token, and register a new
snapshot at the current vector. Existing manager, durable-owner, checkpoint, and live suites remain
green.

## References

- [ADR 0095](0095-multi-tablet-subscription-delivery-order.md)
- [ADR 0237](0237-single-node-applied-append-observation.md)
- [Multi-tablet subscription order](../learning/multi-tablet-subscription-order.md)
