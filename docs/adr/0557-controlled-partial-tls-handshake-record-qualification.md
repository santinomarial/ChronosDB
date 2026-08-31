# ADR 0557: Controlled partial TLS handshake-record qualification

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB networking, cluster-security, distributed-query, and test maintainers
- **Extends:** [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md),
  [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md), and
  [ADR 0556](0556-controlled-partial-tls-record-qualification.md)

## Context

ADR 0556 qualified controlled encrypted application-record cuts only after mutual TLS completed.
It did not prove that partial records during authentication remain below the certificate-to-principal
boundary. An incomplete initial ClientHello must not create a server identity, while an incomplete
encrypted server flight must not create a client identity. Neither direction may reach Job Control
parsing or reducer admission, and failure on one proxy leg must not leave the opposite production
session waiting until its handshake deadline.

The raw two-leg proxy already controls exact bytes without replacing either production TLS endpoint.
The missing evidence is a deterministic handshake schedule that distinguishes complete prerequisite
records from the record intentionally torn.

## Decision

Extend the real-loopback Job Control mutual-TLS test with two three-cut handshake campaigns. Every
attempt uses fresh production client/server sessions behind the raw proxy while retaining one
backend listener, TLS contexts, authenticators, authorizer, and reducer service.

For the client campaign, invoke the production client handshake once, capture its complete initial
outer handshake record, and verify the observed handshake content type and TLS-compatible legacy
record version. Forward exactly:

1. 2 bytes, inside the five-byte TLS record header;
2. 6 bytes, the complete header plus one ClientHello byte; and
3. all record bytes except the final byte.

For the server campaign, forward the complete captured ClientHello and wait until the production
server descriptor reports it readable. Drive the production server, capture its emitted record
flight, and forward complete prerequisite handshake or compatibility-change records to the
production client. Advance both owners after each prerequisite. Select the first encrypted outer
application-data record in the server handshake flight and apply the same 2-byte, 6-byte, and
all-but-final-byte cuts.

After each cut, apply `SO_LINGER(0)` only to both proxy-owned leg descriptors. Present the resulting
kernel failure to both endpoint owners. Both production sessions must enter `Failed` before either
application authenticator observes a verified certificate fingerprint. The reducer service must
retain zero PREPARE requests and zero jobs.

After all six failures, connect directly through the retained backend listener with the same
contexts and dependencies. A complete production PREPARE must authenticate and admit one job,
followed by authenticated CANCEL and reclamation at the original execution deadline.

## Detailed rationale

The ClientHello record covers the earliest peer-controlled TLS framing boundary. Selecting the first
encrypted server-flight record proves that complete unencrypted prerequisites do not weaken the
integrity requirement for the subsequent authenticated handshake flight. The test does not decrypt,
forge, or reinterpret that record; content type 23 only identifies the outer TLS 1.3 encrypted record
selected for truncation.

Waiting for descriptor readability before advancing an owner is part of the deterministic seam.
OpenSSL may defer later flight records until the peer consumes an earlier ServerHello or
compatibility record. Explicitly forwarding and consuming those records avoids confusing event-loop
scheduling with a record cut.

Resetting both proxy legs after forwarding the selected prefix gives each production endpoint an
immediate terminal transport event. It does not assert that one endpoint's local failure is itself
forwarded through a proxy that has already been intentionally destroyed.

## Alternatives considered

- **Cut only ClientHello:** rejected because it leaves the authenticated server-to-client flight
  unqualified.
- **Cut the first server record regardless of type:** rejected because a complete ServerHello is a
  weaker boundary than a partial encrypted handshake record.
- **Invoke authenticators with scripted fingerprints:** rejected because that bypasses OpenSSL
  certificate verification and cannot prove the security boundary.
- **Wait for synthetic handshake deadlines:** rejected because an observed reset is an immediate
  terminal event and should not consume the configured deadline.

## Consequences

ChronosDB now has controlled real-TCP evidence that partial TLS handshake records in both directions
remain below certificate authentication, protocol parsing, and reducer admission. Both production
session owners fail immediately, and the retained endpoint subsequently performs a complete
mutually authenticated lifecycle.

The change adds only test and documentation code. It adds no production dependency, TLS option,
authentication shortcut, network byte, or durable format. It does not qualify later encrypted
client-certificate/Finished records independently, handshakes spanning unusually fragmented record
flights, multi-record application frames, resets racing buffered suffixes, or general packet delay,
duplication, reordering, probabilistic loss, and bandwidth pressure.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): no unauthenticated transport prefix can reach
  committed-query visibility machinery.
- [Invariant 6](../architecture/invariants.md): no query or reducer state exists before the complete
  authenticated session and request boundary.
- [Invariant 11](../architecture/invariants.md): production TLS borrowers outlive their endpoint
  descriptors; only opposite-leg proxy descriptors are reset by the seam.
- [Invariant 14](../architecture/invariants.md): unchanged production TLS negotiation and Job Control
  bytes are used before and after every cut.
- [Invariant 15](../architecture/invariants.md): handshake reset is terminal immediately rather than
  waiting for the configured handshake deadline.
- [Invariant 18](../architecture/invariants.md): fault control does not weaken production TLS,
  certificate verification, application authentication, or later protocol guarantees.

## Validation plan

All eight focused Job Control TLS tests pass, and the new six-cut handshake case passes 25
consecutive Apple arm64 repetitions. The complete cluster suite passes 368 tests, the
allocation-failure suite passes 80, and the grouped-result process suite passes eight. The focused
suite passes with GCC 13 on Ubuntu 24.04 normally and under ASan/UBSan and TSan. The privileged Linux
netfilter target passes all four cases and leaves the OUTPUT chain at its original accept-only
policy. Formatting, workflow-pinning, and whitespace checks pass. Pinned clang-tidy 18 reaches only
the known macOS 26 libc++ unsupported-builtin errors after reporting no project-source diagnostic.
The final diff is reviewed for accidental scope expansion.

## Migration or rollback considerations

No migration exists. Rolling back removes only the six handshake-record cuts and their evidence.
No deployed session, identity, stored state, or compatibility boundary changes.

## Unresolved questions

- Qualify later encrypted client-certificate and Finished record cuts independently.
- Qualify multi-record application frames and resets racing buffered record suffixes.
- Add deterministic delay, duplication, reordering, probabilistic loss, and bandwidth campaigns.
- Qualify larger reducer sets, skew, CPU stalls, and durable query recovery/fencing.

## References

- [Maintained mutual-TLS socket carrier](0144-maintained-mutual-tls-socket-carrier.md)
- [Mutually authenticated grouped reducer Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Controlled partial TLS-record qualification](0556-controlled-partial-tls-record-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
