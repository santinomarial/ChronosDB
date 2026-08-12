# ADR 0277: Bounded durable Raft worker-extension composition

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft, ingestion, metadata, and runtime maintainers

## Context

The asynchronous durable Raft runtime deliberately accepts one worker extension so ownership and
failure semantics remain unambiguous. Production composition now needs both committed tablet
application and authoritative metadata application on that same worker. Giving either application
a separate thread or synchronous-runtime borrow would violate the single-owner contract, while
adding one optional field per application type would make the Raft layer depend on higher layers.

Higher-level operations also prove that the runtime hosts their exact extension before submitting
term-bound work. A top-level composite therefore has to preserve child identity without weakening
that check.

## Decision

`AsyncDurableRaftWorkerExtensionSet` composes between one and 64 unique, non-null direct children.
It is deliberately flat: nested sets are rejected, preventing recursive lifecycle graphs and
duplicate callbacks through overlapping composites.

Initialization, preparation, and completion call children in declaration order. One composite
batch context owns one non-null context per child, and all context destruction remains on the Raft
worker. Completion stops at the first failure because later application against an already failed
ordered boundary would be ambiguous. Shutdown calls every attempted child in reverse order,
including the child whose initialization failed. It continues after returned errors or exceptions
and reports the first failure in reverse invocation order. Shutdown is idempotent.

The base extension exposes an immutable membership query. Ordinary extensions match only
themselves; the set matches itself and each direct child. Runtime ownership checks delegate to that
query, so tablet and metadata owners can retain exact worker-hosting proof when the set is the
top-level extension.

Any child exception is converted to a terminal status at the composite boundary. No callback is
retried. The set adds no scheduler, synchronization primitive, durable bytes, or wire bytes.

## Consequences

- Tablet and metadata application can share the exact durable worker and persistence order.
- Declaration order is part of the in-process composition contract; embeddings must choose it
  intentionally.
- A completion failure can occur after earlier children applied the batch. The whole runtime fails
  closed and restart recovery must reconstruct every child from authoritative committed state.
- The fixed capacity makes lifecycle work and per-batch context ownership explicitly bounded.
- Flat composition is less general than a tree, but makes identity and exactly-once callback
  reasoning auditable for the current use.

## Affected invariants and validation

Invariants 1, 4, 5, 8, 11, 15, and 18 apply. Focused tests cover definition bounds, child identity,
ordered initialization/preparation/completion, reverse shutdown, partial-initialization cleanup,
continuation after a throwing shutdown, completion fail-stop behavior, and unchanged direct
extension behavior.

Allocation fault injection, TSan, shutdown under active production application, and long-running
hook watchdog measurements remain Phase 18 work.

## References

- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0272](0272-worker-affine-raft-application-extension.md)
- [ADR 0273](0273-bounded-term-bound-applied-quorum-completions.md)
