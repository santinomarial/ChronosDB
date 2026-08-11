# ADR 0142: Bounded remote reconfiguration response and retry

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** cluster integration and rebalancing subsystems
- **Supersedes:** none

## Context

The authenticated receiver accepts exact action-request bytes, but a carrier also needs canonical
response correlation and finite sender behavior under stale leaders, overload, I/O failure, delayed
responses, and duplicate attempts. Treating receiver-local persistence as commit would violate the
reconfiguration reconciliation model, while an unbounded automatic retry loop would hide overload
and retain resources indefinitely.

## Decision

Remote Tablet Reconfiguration Response v1 is a fixed, versioned, doubly checksummed envelope. It
binds receiver/sender nodes, the attempted required leader term, and tablet/epoch/action kind to one
stable status code. It can report exact-ledger duplicate preparation and an optional advisory
leader node/term.

`RemoteTabletReconfigurationSender` uniquely owns one sealed, ledger-prepared dispatch. It owns no
thread, I/O, timer, or wall clock. A carrier explicitly supplies monotonic time and a fresh leader
route to `begin_attempt`, transmits the returned canonical request, then supplies exact response
bytes or a transport failure.

Only `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, and `IO_ERROR` schedule retry. Retry count is bounded, and
positive exponential backoff is capped and overflow-safe. A retry cannot start before its deadline
and always requires a newly supplied nonlocal node and nonzero term. Correlation mismatch or damaged
bytes leave the actual response pending. Nonretryable status or budget exhaustion terminates.

`OK` transitions to `LOCALLY_ACCEPTED`, deliberately not `COMMITTED`. Movement reconciliation must
still observe authoritative Raft membership and applied metadata before advancing phase.

## Detailed rationale

Explicit carrier-driven attempts compose with future reactor/TLS implementations without embedding
a second scheduler or clock. Exact route/action correlation rejects delayed responses from another
attempt. Deterministic backoff makes tests and operations predictable; jitter belongs in a future
carrier-wide policy only if measurements justify it.

## Alternatives considered

- **Retry forever:** rejected because it creates unbounded retention and masks persistent failure.
- **Accept any response with the action ID:** rejected because delayed leaders and same-tablet phase
  transitions require exact route/term correlation.
- **Treat local `OK` as commit:** rejected because physical sync on one node is not Raft quorum or
  application evidence.
- **Put sleeps and socket I/O in the sender:** rejected because it would block owners and duplicate
  reactor scheduling/backpressure.

## Consequences

The future carrier has exact bytes and a finite state machine for response, timeout, leader refresh,
and retry decisions. The sealed preparation capability remains owned until terminal state. The
carrier must still enforce one pending attempt, arrange deadlines, and map completed receiver work
into response bytes. Automatic metadata application and authoritative reconciliation remain above
this local response.

## Affected invariants

Invariants 1, 4, 5, 8, 9, 10, 14, and 18 apply. Explicit local-versus-commit semantics preserve
visibility and ordering; exact identity plus bounded retries preserves idempotency; checksums and
versions protect parsing; finite retry state cannot silently weaken overload behavior.

## Validation plan

Focused tests round-trip response bytes and reject damage. Deterministic sender tests reject a
foreign response without consuming the attempt, enforce backoff deadlines, require fresh routes,
carry advisory hints, handle transport failure, cap exponential backoff, stop on terminal status or
attempt exhaustion, and distinguish local acceptance. TLS/socket partial I/O, timeouts, reconnects,
multi-process leader churn, and sustained overload remain carrier validation.

## Migration or rollback considerations

There is no deployed cluster protocol. Version 1.0 is exact. A sender must negotiate compatible
request and response support before attempting a mixed-version peer and retain its prepared action
when compatibility is unavailable.

## Unresolved questions

The maintained TLS provider, transport feature negotiation, response service adapter, timeout
policy, jitter evidence, automatic metadata application, and durable retry-evidence reclamation
remain Phase 16 work.

## References

- [Remote request v1](../protocol/remote-tablet-reconfiguration-v1.md)
- [Remote response v1](../protocol/remote-tablet-reconfiguration-response-v1.md)
- [ADR 0140](0140-atomic-current-leader-term-admission.md)
- [ADR 0141](0141-authenticated-remote-reconfiguration-receiver.md)
- [Architecture invariants](../architecture/invariants.md)

## Retrospective (2026-08-11)

[ADR 0143](0143-nonblocking-reconfiguration-response-publication.md) connects an authenticated
receiver admission to these exact response bytes after the asynchronous local durability boundary.
TLS/socket delivery and pre-admission error policy remain carrier work.
