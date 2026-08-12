# ADR 0257: Ordered Inbound Raft Observation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft runtime and transport maintainers

## Context

An authenticated inbound Raft message previously submitted only its receive operation. The durable
transition was safe to route after synchronization, but a production timer owner had no exact
post-message role and term with which to rearm election or heartbeat activity. A later independent
observation could be reordered behind other admitted work and describe a different state.

## Decision

`RaftTransportReceiver` submits every accepted message as one two-operation FIFO batch: the
`ReceiveOperation` immediately followed by `ObserveGroupOperation` for the same group. The
asynchronous durable runtime executes that batch on its exclusive owner and publishes both results
through one completion acquire edge.

The persistent inbound TLS carrier accepts only two structurally valid results. A successful
observation must contain one owning `RaftGroupObservation`, no transition, and the admitted group.
If observation fails, the receive must also have failed. The carrier retains the optional
observation beside the exact receive transition until embedding pickup. Unknown groups remain
ordinary nonterminal errors and expose no fabricated observation.

## Consequences and validation

The transport-to-timer composition can now rearm from the authoritative state directly following
the durable inbound operation, without another asynchronous race. Each admitted message consumes
two bounded runtime operations instead of one. Focused receiver and persistent TLS/TCP tests prove
successful term observations and unknown-group failure pairing. Unified event-loop routing remains
separate work.

## References

- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0138](0138-fifo-ordered-raft-group-observation.md)
- [ADR 0246](0246-authenticated-raft-transport-receiver.md)
- [ADR 0250](0250-async-durable-raft-timer-driver.md)
