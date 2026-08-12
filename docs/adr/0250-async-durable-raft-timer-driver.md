# ADR 0250: Asynchronous Durable Raft Timer Driver

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft runtime and transport maintainers

## Context

Generation-tagged deadlines alone do not submit work to the exclusive durable owner, obtain the
authoritative post-action role, or retain outbound and snapshot results for routing. Calling the
synchronous runtime from a timer/event loop would block on storage; submitting only an action would
leave rearming dependent on a racy separate observation.

## Decision

`RaftTimerDriver` is a single-event-loop owner that composes `RaftTimerRuntime` with
`AsyncDurableMultiRaftRuntime`. Every due action is submitted nonblockingly in one two-operation FIFO
batch: the election/heartbeat operation followed immediately by `ObserveGroupOperation`. The
completion's mutex acquire edge publishes both the post-sync action transition and the exact
post-action group observation.

The driver rearms the generation-tagged timer from that observation and retains the complete action
result in a preallocated bounded ring until the embedding takes it. Outbound messages, snapshot
install requests, and read-barrier results are therefore not discarded by timing code. A stale
generation completion is still published but cannot overwrite a deadline established by newer
activity.

An embedding-owned `RaftElectionDeadlineSource` supplies every strictly future nonleader deadline.
This preserves deterministic simulation and permits production randomization without putting an RNG
in the consensus core. Exceptions from that boundary are contained as statuses. Leaders use the
timer's heartbeat interval and do not consult the source.

Pending and completed actions have independent fixed bounds. Durable admission exhaustion releases
the timer action for immediate retry without changing its deadline. A full completed ring stops
completion consumption, retaining ownership in the async completion. Groups with in-flight timer
actions cannot be removed. All driver methods are single-thread-affine; cross-thread publication is
only through the asynchronous completion's documented mutex edge.

## Consequences

Bootstrapped term-0 groups now progress through real durable election and heartbeat operations while
the event loop remains nonblocking. Timer output is suitable for the same transport-routing path as
inbound results. The driver still requires an embedding to call `drive`, route taken results, feed
valid inbound activity observations back through `note_activity`, and select production deadlines.

## Validation

Focused tests use the real asynchronous persistent runtime to elect a term-0 single-voter group,
verify a synchronized transition plus leader observation, rearm and run a heartbeat, and prove a
one-slot completed ring retains/backpressures a second group result. Scheduler tests cover stale
generation and admission retry behavior. Runtime terminal failures, storage stalls, randomized
deadline distributions, event-loop wakeups, and large-group scheduling remain Phase 18 work.

## References

- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0138](0138-fifo-ordered-raft-group-observation.md)
- [ADR 0249](0249-generation-tagged-raft-runtime-timers.md)
