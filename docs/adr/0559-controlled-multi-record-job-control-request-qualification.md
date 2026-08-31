# ADR 0559: Controlled multi-record Job Control request qualification

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB networking, cluster-security, distributed-query, and test maintainers
- **Extends:** [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md),
  [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md), and
  [ADR 0556](0556-controlled-partial-tls-record-qualification.md)

## Context

ADR 0556 qualified partial encrypted request and response records when each complete Job Control
frame fit in one TLS application record. It did not prove that a reducer which has already decrypted
and buffered a complete application record will withhold dispatch when a later record belonging to
the same PREPARE is torn. That boundary matters because application framing and TLS record framing
are independent: a valid large request may span several authenticated records while remaining one
atomic Job Control request.

The missing evidence must use a valid production request, production TLS owners, and exact raw
record cuts. An oversized or semantically invalid frame could fail for unrelated decode limits and
would not qualify partial multi-record buffering.

## Decision

Add a three-cut real-loopback campaign using the existing raw two-leg TCP proxy and retained backend
listener. Each attempt constructs a valid PREPARE authority with 1,024 distinct source tablets. Its
encoded Job Control frame is greater than the TLS 16 KiB maximum plaintext-record size while staying
within the authority's normal retained-configuration and wire limits.

After mutual authentication, the production client writes the complete request into at least two
outer application-data records. The proxy captures and forwards the first record intact, waits for
the production reducer endpoint to become readable, and advances the reducer once. The reducer must
remain in `ReadingRequest`, with zero PREPARE dispatches and zero active jobs. The proxy then captures
the next application-data record and forwards exactly:

1. 2 bytes, inside the five-byte TLS record header;
2. 6 bytes, the complete header plus one encrypted payload byte; and
3. all record bytes except the final byte.

The proxy resets both independently owned forwarding legs after each prefix. Both production session
owners must fail, the client must publish no result, and the service must retain zero jobs. After all
three failures, a direct connection through the same backend listener must complete PREPARE, CANCEL,
and execution-deadline reclamation.

## Detailed rationale

The 1,024-source authority is not padding. Every source is a distinct valid tablet placement and the
canonical encoder accepts the complete request. The separately encoded size assertion proves before
transport that the application frame cannot fit in one maximum-size TLS plaintext record; observing
two outer application-data records proves the production OpenSSL carrier performed the split.

Forwarding and consuming the first record before selecting the second distinguishes this test from a
single-record truncation. Zero service dispatch at that point proves the Job Control reader does not
confuse a completely authenticated TLS-record prefix with a complete application frame. Cutting the
later record at header, first-ciphertext-byte, and final-byte boundaries covers framing acquisition,
payload acquisition, and nearly complete authenticated record delivery.

## Alternatives considered

- **Send arbitrary padding:** rejected because padding is not part of the versioned PREPARE format.
- **Lower the TLS fragment size through a test-only socket option:** rejected because the test must
  exercise the production TLS context and record policy.
- **Use an over-limit authority:** rejected because early resource-limit rejection would not prove
  atomic buffering of a valid multi-record request.
- **Forward both records before resetting:** rejected because that admits PREPARE and tests response
  loss instead of incomplete request isolation.

## Consequences

ChronosDB now has exact real-TCP evidence that one completely decrypted application record cannot
prematurely dispatch a larger Job Control PREPARE. A torn following record terminates both session
owners without a client result or reducer state, and the retained listener/service remains reusable.

The change adds only test and documentation code. It adds no dependency, production API, network
byte, durable format, or TLS configuration. Multi-record responses, later handshake records under
forced unusual segmentation, resets racing data already buffered beyond a complete record, general
delay/duplication/reordering/probabilistic-loss/bandwidth campaigns, and durable query recovery and
fencing remain separate.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): an incomplete distributed request cannot enter query
  visibility machinery.
- [Invariant 6](../architecture/invariants.md): reducer state is published only from one complete
  stable request.
- [Invariant 11](../architecture/invariants.md): proxy resets do not violate production TLS borrower
  ownership, and the retained service owns no partial job.
- [Invariant 14](../architecture/invariants.md): the canonical versioned PREPARE bytes cross unchanged
  production TLS framing.
- [Invariant 15](../architecture/invariants.md): an immediate reset terminates both owners without
  waiting for the exchange deadline.
- [Invariant 18](../architecture/invariants.md): every completely or partially forwarded record is
  still subject to OpenSSL integrity and peer authentication.

## Validation plan

All nine focused Job Control TLS tests pass, and the new three-cut case passes 25 consecutive Apple
arm64 repetitions. The complete cluster suite passes 369 tests, the allocation-failure suite passes
80, and the grouped-result process suite passes eight. The focused suite passes with GCC 13 on Ubuntu
24.04, under ASan/UBSan, and under TSan. The privileged Linux packet suite passes all four cases and
leaves the OUTPUT chain at its original accept-only policy. Formatting and whitespace checks pass.

The Ubuntu GCC build reaches the focused suite with warnings non-fatal and passes it. Enabling
repository-wide warnings-as-errors stops earlier in unchanged `src/cseg/metadata_codec.cpp` on GCC's
`-Wduplicated-branches`, before this test compiles. Pinned clang-tidy 18 reaches only the known macOS
26 libc++ unsupported-builtin compiler errors after reporting no project-source diagnostic.

## Migration or rollback considerations

No migration exists. Rolling back removes only the legal large-request fixture, the three later-record
cuts, and their documentation. No deployed identity, stored state, or compatibility boundary changes.

## Unresolved questions

- Qualify multi-record responses on a protocol with a naturally large bounded response.
- Qualify every later encrypted client-handshake record under forced unusual segmentation.
- Qualify reset timing when more suffix bytes have already reached kernel or TLS buffers.
- Add deterministic delay, duplication, reordering, probabilistic loss, and bandwidth campaigns.
- Qualify larger reducer sets, skew, CPU stalls, and durable query recovery and fencing.

## References

- [Maintained mutual-TLS socket carrier](0144-maintained-mutual-tls-socket-carrier.md)
- [Mutually authenticated grouped reducer Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Controlled partial TLS-record qualification](0556-controlled-partial-tls-record-qualification.md)
- [Controlled partial client-authentication record qualification](0558-controlled-partial-client-authentication-record-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
