# ADR 0179: Whole-query TCP cancellation and deadline

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0178](0178-pinned-multi-tablet-tcp-query-scheduling.md)

## Context

The multi-tablet TCP owner had bounded per-attempt connect, TLS-handshake, and exchange deadlines,
but no whole-query deadline or explicit cancellation entry point. A query could therefore continue
through its finite retry budgets after its caller no longer needed the result, retaining sockets,
TLS state, and its Manifest generation. Closing only one attempt could also leave peer tablets
running and create ambiguity about whether a partial coordinator result was usable.

## Decision

`DistributedQueryTcpExecutionConfig` may carry one absolute monotonic execution deadline. Every
poll checks it before opening due attempts and immediately after the bounded kernel wait. The wait
itself is capped by the earlier of that deadline, the next sender retry deadline, and the caller's
maximum wait. Expiry changes the owner to `kCancelled` with a sticky `CANCELLED` status before any
new attempt or response can be accepted at or after the boundary.

`cancel` supplies the same terminal transition explicitly. It destroys every active TCP client,
which destroys TLS before closing each descriptor, sets active-attempt metrics to zero, and retains
no partial result. Repeated cancellation and polling return the identical sticky cancellation.
Cancellation after a completed all-tablet result is a no-op and cannot erase that result. A
previous non-cancellation failure also remains the first terminal outcome.

The scheduler retains its compatible snapshot until the owner itself is destroyed. Cancellation
releases network resources immediately but does not create a background callback, detached cleanup,
or server-side success. Closing the one-request connection is the current best-effort remote stop
signal; a future multiplexed exchange requires an explicit cancellation frame and worker interrupt
contract.

## Consequences and validation

Deadline comparison and cancellation are `O(active tablets)` only for deterministic client
teardown; ordinary deadline checks are constant time in addition to the scheduler's existing scan.
No new allocation is required on the cancellation path beyond construction of the returned status.
The owner remains single-threaded, so callers serialize `poll_once` and `cancel`.

A focused test supplies an already-expired monotonic deadline and proves that no attempt or socket
starts, result access fails with the same cancellation, and later polling is sticky. It then starts
two real loopback connections, observes both active attempts, cancels, proves both clients are
released, and verifies repeated cancellation, polling, and result access preserve the first exact
status.

Invariants 6, 11, 15, and 18 apply.

## Alternatives considered

- **Rely only on per-attempt deadlines:** rejected because retries can remain valid after the
  caller's query budget expires.
- **Cancel one tablet and wait for peers:** rejected because no partial aggregate can be returned as
  success and retained peers waste bounded but unnecessary resources.
- **Destroy the Manifest pin during cancellation:** rejected because references remain governed by
  owner lifetime; an accessor must never observe a released snapshot.
- **Add a cancellation wire frame to the one-request connection:** deferred because closing that
  dedicated connection already gives unambiguous carrier ownership, while asynchronous worker
  interruption needs a separate service contract.

## Migration and rollback

This changes no durable or network bytes. Callers that need a query budget should supply one
absolute steady-clock deadline and may invoke `cancel` from the same serialized owner context.
Removing this feature returns whole-query cleanup to scheduler destruction, without weakening the
existing per-attempt deadlines.

## References

- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
- [Bounded distributed-query TCP server](0176-bounded-distributed-query-tcp-server.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Architecture invariants](../architecture/invariants.md)
