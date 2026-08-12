# ADR 0293: Ordered application Raft transport submissions

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster transport, Raft runtime, query, and service maintainers

## Context

The unified transport owned asynchronous completions submitted by inbound carriers and the timer
driver. An application could submit a Raft operation directly to the same durable worker, but its
initial transition would not enter the transport's routing FIFO. A read-barrier request would then
retain its outbound quorum probes in the caller's completion instead of sending them, while a
second completion-pipe consumer would violate the established ownership contract.

## Decision

`RaftTransportRuntime::try_submit_application` is a poll-owner-only, nonblocking admission seam for
application transitions that require normal outbound routing. It appends one `ObserveGroupOperation`
to the submitted operation in the same durable batch, registers the completion in a fixed bounded
slot before returning its nonzero global submission sequence, and exposes the eventual result with
`kApplication` origin.

Completion intake now compares the smallest ready submission sequence across inbound, timer, and
application owners. It moves only that owner into the existing result ring, routes its transition
through the same peer manager, and returns the ordered observation to the embedding. Result-ring
and outbound backpressure therefore preserve one global order across all three producers.

Timer-, receive-, and observation-owned operations are rejected at this seam. They already have
dedicated owners, and accepting them here could duplicate activity or desynchronize timer state.
Application observations do not rearm election deadlines: a failed follower query or proposal is
not Raft peer activity and must not postpone election.

## Consequences and validation

Application-initiated proposal, membership, snapshot-install completion, compaction,
applied-position, and read-barrier transitions can now use the authenticated transport without a
second completion-descriptor drainer or an unbounded side queue. The caller still owns request
correlation, deadlines, read-barrier completion, and applied-coverage policy. No durable or network
bytes change.

The constructive single-voter transport test elects a leader, fills the one-slot application bound,
routes a current-term proposal, then routes a read barrier and receives its exact application-origin
completion and ordered observation. It also rejects a timer-owned operation. The real listener/TLS
test requires socket permission and passed outside the restricted workspace sandbox. Mixed
three-producer stress, allocation injection, multi-node barrier loss, and saturation schedules
remain in the hardening ledger.

## Affected invariants

Invariants 4–6, 11, 14, 15, and 18 apply.

## References

- [ADR 0258](0258-portable-durable-raft-completion-wakeup.md)
- [ADR 0261](0261-fifo-identified-raft-completions.md)
- [ADR 0265](0265-unified-raft-transport-runtime.md)
- [ADR 0113](0113-linearizable-raft-read-barrier.md)
