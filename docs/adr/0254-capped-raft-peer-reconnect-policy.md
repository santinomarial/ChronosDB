# ADR 0254: Capped Raft Peer Reconnect Policy

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft transport and networking maintainers

## Context

A one-attempt TCP owner and failure-safe pool handoff still leave attempt timing and complete retry
frame custody to ad hoc event-loop code. Raft peers must retry indefinitely for liveness without a
tight failure loop or an unbounded delay.

## Decision

`RaftTransportPeerReconnect` owns one immutable node/address configuration, at most one TCP
connector, all complete frames between attempts, and a deterministic monotonic retry deadline.
Failures schedule exponential backoff from a positive initial delay to an inclusive configured cap.
Driving before the deadline is a no-op; driving at it starts exactly one attempt. There is no finite
attempt limit because a configured Raft peer may recover after an arbitrarily long outage.

Successful connector handoff resets the next delay to the initial value and marks the owner
connected. When the pool later returns the exact failed descriptor/carrier/frame bundle, the owner
verifies peer identity, takes the complete frames, destroys the old TLS-before-TCP pair, and resumes
backoff. New durable results are not absorbed while disconnected; their upstream owner must retain
or backpressure them rather than allowing an unbounded side queue.

## Consequences and validation

The per-peer retry lifecycle now has bounded memory, one in-flight descriptor, exact deadlines,
capped delay, and lossless duplicate-safe frame custody. A higher event-loop layer must still own a
bounded route catalog, poll descriptors, feed readiness, and retain unroutable fresh durable results.

**Retrospective note (2026-08-12):** [ADR 0255](0255-bounded-raft-outbound-peer-manager.md) now
provides the fixed route catalog, descriptor interests, pool installation, failure recycling, and
fresh-result backpressure boundary. Whole runtime poll composition remains separate.

Focused tests prove pre-deadline suppression, exact-deadline attempts, doubling to the configured
cap, successful descriptor/carrier handoff, TLS failure extraction from the production pool, and
retention of the complete queued frame for the next attempt.

## References

- [ADR 0248](0248-persistent-outbound-raft-mtls-carrier.md)
- [ADR 0251](0251-bounded-raft-peer-carrier-pool.md)
- [ADR 0253](0253-ownership-safe-raft-tcp-connect-attempt.md)
