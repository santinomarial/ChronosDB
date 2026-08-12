# ADR 0249: Generation-Tagged Raft Runtime Timers

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft runtime maintainers

## Context

The deterministic Raft core deliberately owns no clock. A multi-group runtime still needs bounded
election and heartbeat scheduling without letting a stale timer completion overwrite a deadline
established by newer message activity. Runtime admission backpressure must not silently postpone a
due election, and test simulation must control every deadline exactly.

## Decision

`RaftTimerRuntime` is a single-thread-affine bounded scheduler over injected monotonic time. The
embedding selects and supplies every future nonleader election deadline, keeping randomness and
simulation policy outside the scheduler. Leaders use one configured heartbeat interval with
saturating deadline arithmetic.

Each group has one generation and at most one in-flight action. `poll` returns a bounded set of
generation-tagged `StartElectionOperation` or `HeartbeatOperation` requests and marks them in flight.
If asynchronous durable admission rejects an action, `reject_admission` makes the same due
generation immediately pollable again. `note_activity` or a valid completion rearms from an owning
group observation and increments the generation. A completion whose generation was superseded by
newer activity is rejected and cannot rewrite the current deadline.

All calls, observations, deadlines, and generation counters are owned by one scheduling thread; no
atomics or cross-thread memory ordering are involved. The embedding uses the asynchronous durable
completion's existing acquire edge before passing a resulting observation to `complete`.

## Consequences

Election and heartbeat intent is deterministic, bounded, backpressure-aware, and directly reusable
by production loops and simulators. The scheduler does not choose randomness, submit work, await
completions, reset deadlines from carrier results automatically, or own a timerfd/event loop. Those
composition responsibilities remain above this API.

## Validation

Focused tests cover exact deadline boundaries, one in-flight action, admission retry without time
shift, follower election, leader heartbeat cadence, generation advance, stale-completion rejection,
invalid deadlines, and group/action bounds. Async-runtime composition, clock jumps, thousands of
groups, long randomized schedules, and timing measurements remain in Phase 18.

## References

- [ADR 0069](0069-deterministic-raft-and-multiplexed-state-record.md)
- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0138](0138-fifo-ordered-raft-group-observation.md)
