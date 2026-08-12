# ADR 0241: Single-Node Subscription Runtime Composition

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, live-query, runtime, and networking maintainers

## Context

The database requires its borrowed committed-append observer address at open time, while executable
subscription plans and their storage snapshot context are available only after database recovery.
The Protocol 1.1 lifecycle worker and applied-append fan-out also need the same plan, coordinator,
query resources, and thread-affine ownership. Ad hoc daemon wiring could bind dangling pointers or
let different threads mutate one coordinator.

## Decision

`SingleNodeCommittedAppendRouter` supplies one stable observer address before database open. On the
same owner thread, exactly one borrowed delegate may be attached after recovery and detached before
destruction. Appends received while unbound are counted rather than reported as forwarded; daemon
admission must not begin until binding succeeds.

`SingleNodeSubscriptionRuntime` composes one durable plan's append fan-out and
`SubscriptionService`. Its creation validates the fan-out and lifecycle configurations before
binding the router. It borrows the prepared plan, durable coordinator, catalog, query resources,
Manifest storage publication, schema lineage, and two internal SPSC queues. Every borrowed owner
and queue outlives the runtime. Its heap-owned implementation keeps the fan-out address stable
across runtime moves, and destruction unbinds before destroying the service or fan-out.

`SingleNodeDatabase::subscription_snapshot_context` returns a narrow borrowed view of its exact
Manifest storage, aggregate publisher, and table lineage for this composition. It rejects shutdown,
missing storage, and nonlocal tables. The database remains the lifetime owner; the view grants no
separate publication or shutdown authority.

The runtime remains single-plan. A daemon-level registry may own multiple instances and route by
durable plan fingerprint, but every coordinator must stay on the database worker thread. The
external reactor queues are not shared with `SubscriptionService`; the eventual daemon router uses
separate bounded internal queues so each SPSC ring still has one producer and one consumer.

## Consequences

One production-shaped component now owns the complete in-process historical/live/acknowledgement/
shutdown lifecycle and the exact post-apply feed. Binding cannot be accidentally replaced while
active. The runtime itself does not create plan definitions, MAC-key configuration, or coordinator
directories; those daemon registry responsibilities were composed later by
[ADR 0242](0242-configured-chronosd-subscription-lifecycle.md) and fail closed before socket
admission.

The observer router's unbound count is an operational invariant: any nonzero online value means an
applied append occurred outside configured live routing. Startup WAL replay intentionally does not
invoke the observer and is not counted.

## Validation

Focused service tests prove single-delegate binding and exact unbinding. A complete runtime test
executes a real aggregate storage snapshot, emits READY, forwards a committed append through the
router, installs coordinator generation 1, emits a live change, acknowledges it, and emits a
resumable server-shutdown termination. Database coverage verifies the narrow snapshot context.

## References

- [ADR 0105](0105-bounded-subscription-service-lifecycle.md)
- [ADR 0237](0237-single-node-applied-append-observation.md)
- [ADR 0239](0239-bounded-single-node-live-append-fanout.md)
- [ADR 0240](0240-write-synchronous-live-checkpoint-gate.md)
