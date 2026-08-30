# ADR 0554: Authenticated partial-frame TCP reset qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, networking, reducer service, and test maintainers
- **Extends:** [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md),
  [ADR 0551](0551-authenticated-partial-job-control-frame-qualification.md), and
  [ADR 0553](0553-established-job-control-black-hole-qualification.md)

## Context

The authenticated partial-frame gate proved deadline-bounded ownership while a peer stayed silent.
The established-session netfilter gate proved whole-request and whole-response black holes. Neither
proved how the production TLS session owners classify an abrupt TCP reset after accepting a valid
application-frame prefix. A reset must not dispatch an incomplete request, publish an incomplete
response, wait for the exchange deadline, or poison later connections on the same listener.

This boundary must be distinguished from a partial TLS record. The existing public TLS API can
submit an exact plaintext prefix while OpenSSL owns record formation; it does not expose
encrypted-record bytes or internal record segmentation. Tearing an encrypted record therefore
remains separate.

## Decision

Add a real-loopback-TCP test to the production Job Control mutual-TLS suite. A test helper completes
nonblocking connect/accept ownership and another helper sets `SO_LINGER` to zero, destroys the
borrowing TLS object, and closes the owning TCP socket. That teardown order emits an abortive close
without violating the production descriptor-lifetime contract.

For the production server session, repeat fresh mutually authenticated connections through one
retained listener and reducer service at three exact PREPARE plaintext prefixes:

1. 3 bytes, inside the eight-byte protocol magic;
2. 25 bytes, after magic and inside the fixed header; and
3. 129 bytes, the complete checksummed 128-byte header plus one nested-payload byte.

The scripted TLS client writes exactly one prefix, the production server consumes it while staying
in `ReadingRequest`, and the client then resets TCP before the suffix exists. The server must enter
`Failed` on the immediate transport event, with zero PREPARE requests and zero active jobs after
every boundary.

For the production client session, complete mutual TLS and the full PREPARE write, then have a
scripted authenticated server send the first 37 plaintext bytes of an otherwise valid, exactly
correlated 100-byte response. The client must remain in `ReadingResponse` with no result. Resetting
the server TCP connection must immediately enter `Failed` and retain no result.

After all four resets, use the same listener, TLS contexts, authenticators, authorizer, and reducer
service for complete production client/server PREPARE and CANCEL exchanges. The service must admit
one job, acknowledge authenticated cancellation, retain the terminal identity only through its
original execution deadline, and reclaim it afterward.

## Detailed rationale

Reusing the exact prefix boundaries from ADR 0551 makes silence and reset behavior directly
comparable. The 129-byte case crosses header validation and payload-allocation admission without
providing a complete integrity-protected request. The correlated response prefix ensures the
client's failure is not accidentally satisfied by malformed identity or action.

`SO_LINGER(0)` is applied only to a connected test socket and the socket is closed only after its
TLS borrower is destroyed. Unlike a graceful close, this exercises the kernel reset path. The
asserted contract permits the TLS implementation to report either an I/O error or an unavailable
closed transport; both are immediate terminal transport outcomes and neither is a timeout or
application success.

## Alternatives considered

- **Close normally without TLS `close_notify`:** this can become FIN/EOF and does not prove reset
  handling.
- **Inject RST with a raw packet tool:** forged sequence correctness and platform privileges add
  complexity without improving this connected-socket application-prefix boundary.
- **Claim partial TLS-record coverage:** each prefix is written through `TlsSocket` as accepted
  plaintext. Encrypted-record tearing requires a lower-level proxy or BIO fault seam and remains
  explicitly open.

## Consequences

ChronosDB now has real-TCP evidence that authenticated partial Job Control requests and responses
fail closed on abortive connection loss. Incomplete requests create no reducer state, incomplete
responses create no client result, failure occurs before the configured deadline, and the retained
listener and service remain usable for authenticated cleanup.

The change adds only test and documentation code. It changes no production behavior, dependency,
wire byte, durable format, or retry policy. It does not qualify TCP reset during the TLS handshake,
an encrypted TLS record torn at controlled wire-byte offsets, RST after a complete admitted request
but before any response byte, reset races with kernel-buffered suffix bytes, or packet
delay/duplication/reordering/probabilistic loss.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): no incomplete request or response can publish a job
  or correlated query result.
- [Invariant 10](../architecture/invariants.md): the header-plus-one-byte reset cannot bypass
  complete-frame integrity and nested-payload validation.
- [Invariant 11](../architecture/invariants.md): TLS borrowers are destroyed before abortively
  closing their owned TCP descriptors, and retained dependencies outlive all attempts.
- [Invariant 14](../architecture/invariants.md): unchanged canonical Job Control v1 bytes are used at
  every prefix boundary.
- [Invariant 15](../architecture/invariants.md): reset is an immediate terminal transport event and
  terminal service identity is reclaimed at its original execution deadline.
- [Invariant 18](../architecture/invariants.md): the test composes production TCP, mutual TLS,
  authentication, parsing, service, correlation, cancellation, and reclamation contracts.

## Validation plan

The five focused Job Control TLS tests pass in 25 consecutive Apple arm64 repetitions. The complete
cluster suite passes 365 tests, the allocation-failure suite passes 80, and the grouped-result
process suite passes eight. The reset case passes with GCC 13 Ubuntu 24.04 normally and under
ASan/UBSan and TSan. The separate privileged Linux netfilter target passes all four cases and leaves
the OUTPUT chain at its original accept-only policy. Formatting, workflow pinning, and whitespace
checks pass. Pinned clang-tidy 18 reaches only the known macOS 26 libc++ unsupported-builtin errors
and reports no project-source diagnostic. The final diff is reviewed for accidental scope
expansion.

## Migration or rollback considerations

No migration exists. Rolling back removes this test-only abortive-close helper, one qualification
case, and its evidence. No deployed state or compatibility boundary changes.

## Unresolved questions

- Qualify partial encrypted TLS-record interruption at controlled wire-byte boundaries.
- Qualify reset after complete reducer admission and before any response byte independently of the
  whole-response black-hole gate.
- Add deterministic delay, duplication, reordering, probabilistic loss, and bandwidth campaigns.
- Qualify larger reducer sets, skew, CPU stalls, and durable query recovery/fencing.

## References

- [Mutually authenticated grouped reducer Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Authenticated partial Job Control frame qualification](0551-authenticated-partial-job-control-frame-qualification.md)
- [Established Job Control black-hole qualification](0553-established-job-control-black-hole-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
