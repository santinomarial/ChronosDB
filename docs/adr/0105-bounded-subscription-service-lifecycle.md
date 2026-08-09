# ADR 0105: Bounded reactor-facing subscription service lifecycle

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and networking maintainers
- **Extends:** [ADR 0063](0063-bounded-reactor-shard-spsc-routing.md),
  [ADR 0064](0064-bounded-linux-epoll-reactor.md),
  [ADR 0094](0094-native-protocol-1-1-subscriptions.md), and
  [ADR 0101](0101-durable-multi-tablet-subscription-owner.md)

## Context

The reactor already routes bounded request and response tasks, while the durable subscription owner
already composes plan-bound snapshot and live state. No service owner connected those lifecycles.
Hand-written callers could advance a snapshot after losing its response to a full ring, emit live
changes before READY, accept a resume token under a different outer subscription identity, or leak
manager state when a connection detached.

## Accepted decision

`SubscriptionService` is one thread-affine worker for one prepared durable coordinator. It borrows
the durable owner, catalog, query resources, storage publication, schema lineage, and one request and
response SPSC queue; all outlive it. It accepts only `SUBSCRIBE_REQUEST`,
`SUBSCRIPTION_ACKNOWLEDGE`, and `CANCEL` tasks. New SQL is reparsed and rebound, then must reproduce
the configured executable fingerprint before the exact global snapshot starts. Resume validates the
outer subscription UUID against the authenticated token before manager mutation.

Each `poll_once` performs finite nonblocking work. Active connection/request pairs are bounded and
advanced round-robin by at most one output. Snapshot batches, END_STREAM, READY, live changes,
acknowledgement checkpoints, cancellation, schema/overflow termination, and server shutdown all use
the existing canonical Protocol 1.1 payloads. Resumed streams emit an empty schema-bearing
END_STREAM before READY. Live delivery does not eagerly resend an already-enqueued unacknowledged
prefix; acknowledgement advances the manager before the next finite delivery window.

If the response ring is full, the service retains exactly one already-encoded `NetworkTask` and
does not consume another request or advance any subscription until that same task is published.
`SpscNetworkTaskQueue::try_push_preserving` checks the consumer frontier before moving the caller's
task. It uses the same producer-relaxed/consumer-acquire capacity check and cell-write/producer-
release publication as ordinary push; no second synchronization algorithm is introduced.

`begin_shutdown` stops admission. Continued polling terminally drains every active subscription
with `SERVER_SHUTDOWN`, except an already schema-changed or overflowed manager retains its more
precise reason. Destruction abandons any remaining manager states without token allocation. The
caller joins this sole response producer before reactor/queue destruction.

## Consequences and alternatives

This service is deliberately scoped to one fixed plan/coordinator. Database routing and topology
ownership select or replace services outside it. It owns no thread and performs no socket I/O; the
existing reactor remains the exclusive connection and partial-I/O owner. One retained response can
temporarily stall this service, but memory stays bounded and another service/shard can progress.

Dropping a response on ring saturation was rejected because a snapshot or delivery cursor may have
advanced. Copying the task on every retry was rejected because backpressure must not cause repeated
payload allocations. Mutating subscription state before validating the outer resume identity was
rejected because it could install an unreachable active session. Letting the reactor drive manager
calls directly was rejected because disconnect, shutdown, and exact plan/storage lifetimes would be
split across owners.

## Affected invariants and validation

Invariants 11, 12, 14, 15, and 17 apply. Focused tests execute a real global snapshot, force response
ring saturation and exact retry, deliver and acknowledge a live change, cancel with a safe token,
reject a mismatched resume UUID without mutation, resume through END_STREAM/READY, and drain with a
server-shutdown terminal frame. Queue tests prove a full preserving push leaves payload ownership
unchanged and retain the existing 100,000-task release/acquire run. Real-socket service/reactor
threading, disconnect races, sustained fan-out/backpressure, allocation sweeps, and TSan remain in
the Phase 18 ledger.
