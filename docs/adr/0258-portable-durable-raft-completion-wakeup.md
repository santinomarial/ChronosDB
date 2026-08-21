# ADR 0258: Portable Durable Raft Completion Wakeup

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft runtime and networking maintainers

## Context

The asynchronous durable Multi-Raft owner published completions through mutexes and condition
variables, but a nonblocking transport loop had no descriptor on which to wait. Periodic polling
would either add arbitrary consensus latency or consume CPU and could still sleep past completed
durable work.

## Decision

Each `AsyncDurableMultiRaftRuntime` owns one nonblocking, close-on-exec POSIX pipe. Its worker first
publishes a complete batch through the existing completion mutex release edge and then writes one
byte to the pipe. A full pipe is successful coalescing because its read end remains ready. Unexpected
notification failures stop admission and fail queued work closed.

Saturating metrics distinguish bytes written from writes coalesced after `EAGAIN`/`EWOULDBLOCK`.
They are observability counters, not publication primitives; the completion mutex and readable
descriptor remain the synchronization contract.

The runtime exposes the borrowed read descriptor and an explicit drain operation. One embedding
event loop owns draining and must inspect every completion owner it coordinates after a wakeup. The
pipe is runtime-lifetime state: shutdown joins the worker before destruction closes either end.
Completion results remain independently owning and may outlive the runtime as before.

## Consequences and validation

Portable `poll`/`epoll` integrations can now wait for durable results together with sockets without
timer polling. Every successful or terminal batch adds at most one byte and notifications may
coalesce without losing readiness. Focused tests prove a real durable operation wakes the
descriptor, draining clears readiness, and the owning completion is ready. A controlled full-bound
race also proves concurrent shutdown retains every accepted completion and leaves the descriptor
drainable after the worker joins. A bounded unread-completion test fills the real platform pipe,
observes coalescing without terminal failure or missing completions, drains readiness, and proves the
next completion writes a fresh signal. A controlled durable failure also publishes one wakeup for a
queued failure fanout and one for the current failed batch while shutdown joins the worker. Broader
syscall-level notification/storage fault timing matrices remain Phase 18 work.

## References

- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0250](0250-async-durable-raft-timer-driver.md)
