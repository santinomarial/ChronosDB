# ADR 0265: Unified Raft Transport Runtime

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft runtime, transport, and networking maintainers

## Context

Production Raft pieces existed for durable execution, completion wakeups, timers, inbound TCP/TLS,
outbound peer reconnect, deadlines, and FIFO completion identity, but no owner composed them. An
embedding that polled them independently could sleep past a deadline, route results out of durable
order, drop work under output backpressure, or fail to rearm timers from exact inbound activity.

## Decision

`RaftTransportRuntime` is one single-thread-affine portable poll owner. It owns the timer driver,
inbound server, outbound peer manager, one preallocated descriptor/owner table, and one fixed result
ring while borrowing the asynchronous durable runtime. Each iteration:

1. drives reconnect, inbound durable progress, and due timer admission;
2. merges ready inbound and timer results by nonzero durable FIFO submission sequence;
3. rearms group activity from the inbound operation's immediately following observation;
4. routes results in FIFO order until the first disconnected/full peer boundary;
5. polls the durable completion descriptor, listener, stable inbound connection IDs, and exact
   outbound node IDs with a wait clamped to the earliest timer/transport deadline; and
6. drives all progress again at the post-poll monotonic time.

Routed results remain in the bounded ring for embedding-owned committed application, snapshot
installation, read-barrier work, and observation. The ring does not consume an unroutable result.
Later results never pass an earlier result still under routing backpressure, but already routed
results do not block routing of later FIFO entries merely because application pickup is delayed.

The runtime is the sole consumer of the borrowed durable completion descriptor. A wake returns
control from `poll_once` after internal progress, allowing the embedding to inspect any additional
completion owners that share the durable runtime. Those owners must not independently drain the
same descriptor.

## Consequences and validation

One bounded loop now composes production Raft network, storage wakeup, timers, deadlines, routing,
and result ownership without a periodic tick or hidden output queue. Component dependencies and the
durable runtime must outlive it; ordinary peer failures remain reconnect-local, while structural,
poll, listener, timer, or unexpected routing errors fail the owner closed.

Focused tests prove exact deadline wakeup into a synchronized single-voter election result and a
real two-node mutual-TLS request/response path through inbound admission, asynchronous durability,
ordered timer activity, outbound route establishment, and response receipt. Multi-node churn,
result-ring saturation, mixed producers, storage stalls, and long fault schedules remain Phase 18
work.

## References

- [ADR 0250](0250-async-durable-raft-timer-driver.md)
- [ADR 0255](0255-bounded-raft-outbound-peer-manager.md)
- [ADR 0258](0258-portable-durable-raft-completion-wakeup.md)
- [ADR 0259](0259-exact-raft-runtime-deadline-introspection.md)
- [ADR 0260](0260-embedding-owned-inbound-raft-readiness.md)
- [ADR 0261](0261-fifo-identified-raft-completions.md)
