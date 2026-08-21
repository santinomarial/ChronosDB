# ADR 0261: FIFO-Identified Raft Completions

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft runtime and transport maintainers

## Context

Inbound network work and timer work share one asynchronous durable FIFO but complete into separate
component-owned queues. Scanning those components in an arbitrary order could reverse transitions
from the same group. Physical persistence sequence cannot order every transition because valid
operations may emit responses without changing durable state.

## Decision

Every successfully admitted asynchronous batch receives a nonzero, strictly increasing,
runtime-lifetime submission sequence while holding the FIFO mutex. Rejected work consumes no
sequence. The move-only completion retains that identity before and after result consumption.
Admission fails closed before sequence wraparound.

Timer and inbound TLS completions propagate the sequence with their owning results. Timer collection
selects the smallest ready pending sequence rather than slot order. A multi-connection inbound
server likewise exposes and takes its smallest result-ready sequence rather than compacting table
order. A later composition owner can compare the two heads and preserve the durable owner's FIFO.

The sequence is ordering metadata, not a durable position, log index, term, or replay identity. It
must never be persisted or used to infer durability after restart.

## Consequences and validation

Cross-component routing and application can preserve authoritative in-process execution order even
when fixed slots are reused or connections are compacted. Focused runtime, timer, TLS, and TCP tests
prove nonzero consecutive identities and propagation. One real mutual-TLS aggregate retains an
application completion, timer completion, and inbound completion in their exact consecutive
submission order. Wraparound, high-contention mixed-producer ordering, and restart-boundary misuse
checks remain Phase 18 work.

## References

- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0138](0138-fifo-ordered-raft-group-observation.md)
- [ADR 0250](0250-async-durable-raft-timer-driver.md)
- [ADR 0260](0260-embedding-owned-inbound-raft-readiness.md)
