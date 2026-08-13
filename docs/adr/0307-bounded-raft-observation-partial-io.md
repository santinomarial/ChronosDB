# ADR 0307: Bounded Raft observation partial-I/O ownership

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, networking, and Raft maintainers
- **Extends:** [ADR 0306](0306-authenticated-raft-observation-transport.md)

## Context

Raft Observation Transport v1 codecs and the authenticated receiver accept only complete frames.
Nonblocking stream carriers can fragment or coalesce those frames and can complete only a prefix of
an outbound write. A carrier must reject damaged or hostile declared lengths before allocating,
retain no caller-owned bytes across readiness callbacks, and never duplicate progress when state or
an outbound frame changes owner.

## Decision

Expose exact request and response header-validation gates. Each verifies magic, header CRC,
version, route/correlation semantics, presence/status consistency, and declared lengths before
returning a frame size. The response gate additionally derives a maximum from the configured voter
bound, so a peer-controlled length cannot cause an unchecked allocation.

`RaftObservationRequestReader` retains the fixed 84-byte request storage. The response reader
retains only its fixed 96-byte header until that gate succeeds, then allocates exactly one bounded
frame. Both consume at most one frame per call, report the exact consumed prefix, reset after a
successful decode, and make any error sticky. Response-reader moves explicitly transfer partial
state and reset the source; they do not duplicate progress.

`RaftObservationFrameWriteCursor` accepts only a complete canonical request or response under the
same configured limits. It owns the encoded vector, exposes only the unwritten suffix, rejects
over-advancement without changing state, and leaves its source complete after a move.

These types own bytes only. Descriptor ownership, TLS authentication, readiness, deadlines,
retries, cancellation, and leader/follower acquisition remain carrier responsibilities.

## Consequences

Request reads are allocation-free. Each response reader retains a fixed 96-byte header plus at most
one validated frame; the default maximum is 1,164 bytes and the hard configured maximum is 131,244
bytes. Processing and retained response memory are linear in the bounded frame size. Coalesced
suffixes remain caller-owned, preserving explicit dispatch and backpressure.

## Validation

Focused tests enumerate every split for request, success-response, and failure-response frames;
consume coalesced responses by exact reported prefixes; reject a damaged request header and an
over-limit response at the header boundary; prove sticky failure; transfer a partially read
response without duplicating progress; and cover request/response cursor validation, short writes,
over-advance rollback, and moved-from completion. The public header and installed consumer cover
the new interfaces.

Allocation-failure injection, TLS sockets, disconnect schedules, and hostile long-running stream
tests remain deferred to the applicable carrier work and Phase 18.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## Migration and rollback

This adds no wire bytes and changes no durable state. An embedding may use equivalent stream owners
only if it preserves header-first bounded allocation, exact prefix accounting, sticky failure, and
single write ownership.

## References

- [Raft Observation Transport v1](../formats/raft-observation-transport-v1.md)
- [Authenticated Raft observation transport](0306-authenticated-raft-observation-transport.md)
- [Bounded Raft transport partial-I/O ownership](0245-bounded-raft-transport-partial-io.md)
