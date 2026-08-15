# ADR 0403: Injectable Common Time Source

- **Status:** accepted
- **Date:** 2026-08-15
- **Owners:** ChronosDB common-runtime and correctness maintainers

## Context

Phase 1 requires a common time abstraction, while later subsystems need deterministic clocks for
failure and scheduling tests. The sealed-head flush queue already had a private function-pointer
and `void*` seam for monotonic age accounting, logging directly consulted the system wall clock,
and other runtime boundaries pass explicit time points. Without a typed common contract, each new
owner can invent different lifetime, clock-domain, and thread-safety rules.

Wall time and elapsed time are not interchangeable. The civil clock can move because of operator or
time-synchronization adjustments and is suitable for diagnostics, not timeouts or commit order.
The steady clock is monotonic within a process and suitable for durations and deadlines, but has no
portable civil or durable meaning.

## Decision

`chronos::common::TimeSource` is a noncopyable abstract source with separate `wall_now()` and
`monotonic_now()` operations. Both are `noexcept` and callable concurrently; implementations must
provide their own synchronization when state is mutable. `SystemTimeSource` delegates to the C++
system and steady clocks, and `system_time_source()` returns one stateless process-lifetime
instance.

Consumers that retain an injected source borrow it. The source must outlive the consumer's entire
shared state, including move-only reservations or work that can keep that state alive after an
outer wrapper is destroyed. This first integration replaces the sealed-head queue's untyped
callback/context pair with the typed source. Default structured-log timestamps use the system
source's wall domain; callers can still provide an exact timestamp in each `LogRecord`.

No clock reading becomes durable authority. WAL/Raft positions remain the system-order source,
elapsed-time decisions use only the monotonic domain, and converting between wall and monotonic
points is unsupported.

## Alternatives considered

- **Keep per-subsystem callbacks:** avoids a common API but repeats unsafe context casts and leaves
  lifetime and clock-domain rules inconsistent.
- **One wall-clock API:** is insufficient for deadlines because civil time may jump.
- **One monotonic API:** cannot produce operator-readable civil diagnostics.
- **Own clocks with `shared_ptr`:** simplifies some lifetimes but forces allocation/shared ownership
  on every consumer. The explicit borrow matches existing injected I/O and identity boundaries.
- **Return a correlated wall/monotonic pair:** suggests a stable conversion that clock adjustment
  invalidates and is not required by a current subsystem.

## Consequences

Tests can provide deterministic typed sources without `void*`, while production owners share one
thread-safe adapter. Virtual dispatch is accepted at control/diagnostic boundaries and is not a
per-row data-path operation. Borrowed injection requires explicit lifetime discipline. The API does
not virtualize sleeps, timers, scheduling, time zones, parsing, or durable timestamps; those need a
current use and separate contracts.

## Affected invariants

This decision supports invariants 1, 11, 14, 16, and 18 by keeping commit authority separate from
wall time, making injected lifetime explicit, preventing clock-domain ambiguity, and enabling
deterministic time-dependent tests.

## Validation

- Common tests verify exact injected values, distinct domains, the stable system source, and
  concurrent monotonic reads.
- The sealed-head queue's existing deterministic age and TSan tests run through the typed source.
- Structured-log timestamp tests retain exact explicit timestamp coverage.
- Public-header and installed external-consumer checks compile and execute the new interface.

## Migration or rollback considerations

There are no durable bytes or deployed APIs to migrate. Rolling back requires restoring every
consumer's clock seam while preserving separate domains, deterministic tests, thread safety, and
the same borrow lifetime.

## References

- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [Architecture invariants](../architecture/invariants.md)
- [Phase 1 roadmap](../roadmap.md#phase-1--build-and-common-foundations)
- [Sealed-head flush scheduling](../learning/sealed-head-flush-scheduling.md)
