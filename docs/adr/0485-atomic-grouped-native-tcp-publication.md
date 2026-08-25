# ADR 0485: Atomic grouped Native TCP publication

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB query, cluster, networking, and protocol maintainers
- **Extends:** [ADR 0483](0483-pinned-grouped-sufficient-state-tcp-scheduling.md) and
  [ADR 0484](0484-bounded-grouped-sufficient-state-native-finalization.md)

## Context

The all-tablet grouped TCP scheduler and grouped Native finalizer were individually complete, but
their public boundary still exposed physical chunks. An embedding could forget final order/limit,
apply different output bounds after network work, or publish a physical prefix before Native
encoding failed.

## Decision

`DistributedVectorGroupedAggregateQueryTcpExecutionV2` now owns finalization limits and validates
them with routes, carrier limits, and grouped authority before opening a socket. After every finite
sender succeeds, it admits each canonical stream once, closes the global coordinator, drains its
physical output through the pinned order/limit plan, and Native-encodes the complete result. Only
then does the scheduler enter `kComplete` and expose `result()`.

`result()` is absent while running and after deadline expiry, cancellation, transport failure,
coordinator failure, or finalization failure. The finalizer borrows and synchronously drains the
portable execution rather than moving it, so the scheduler retains the Manifest snapshot and
diagnostic authority after completion. Finalization failure is whole-query terminal because the
physical stream may already be consumed; no Native prefix is retained.

## Consequences

One owner now covers immutable route preflight, retries, authentication, all-tablet closure,
deterministic merge, global order/limit, and bounded Native publication. This direct grouped
sufficient-state path has the same all-or-nothing public result shape as row and ungrouped aggregate
TCP execution. Replicated Native request preparation and daemon routing remain separate.

One thread serializes the lifecycle, so no inter-thread memory-ordering argument applies. No durable
or wire format changes.

## Validation

Two mutual-TLS servers, one refused first address, and two exact grouped workers produce one merged
FLOAT64/COUNT row. The scheduler proves three attempts, one retry, no surviving clients, no result
before completion, and one decoded Native row only after completion. Invalid finalization bounds
fail before I/O; deadline and explicit cancellation retain no result. Four focused cases pass
normally and under ASan/UBSan. The complete cluster suite passes 246 of 246 and allocation-failure
neighbors pass 31 of 31. Formatting, header self-containment, and whitespace checks pass. LLVM 18
static analysis remains blocked by the installed macOS 26 libc++ after no new local finding.

## Unresolved questions

- Replicated Native SQL preparation and packaged daemon routing into this scheduler.
- Computed pre-group and final projection splitting.
- Partitioned shuffle routing and multi-process split-leader qualification.

## References

- [Pinned grouped sufficient-state TCP scheduling](0483-pinned-grouped-sufficient-state-tcp-scheduling.md)
- [Bounded grouped sufficient-state Native finalization](0484-bounded-grouped-sufficient-state-native-finalization.md)
