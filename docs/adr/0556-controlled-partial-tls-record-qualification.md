# ADR 0556: Controlled partial TLS-record qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, networking, reducer service, and test maintainers
- **Extends:** [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md),
  [ADR 0554](0554-authenticated-partial-frame-tcp-reset-qualification.md), and
  [ADR 0555](0555-admitted-prepare-pre-response-tcp-reset-qualification.md)

## Context

Application-prefix and abortive-close gates previously submitted plaintext through `TlsSocket` and
left OpenSSL in control of encrypted record formation. They therefore could not prove behavior when
TCP carries only a prefix of one authenticated TLS record. A record may be cut before its header is
complete, shortly after its declared encrypted payload begins, or after every byte except the final
ciphertext/authentication byte. None of those prefixes may become an application request or result.

The production TLS API intentionally does not expose OpenSSL's record bytes. Qualification needs a
wire-level seam that preserves the real endpoint implementations instead of adding a test-only TLS
backend or changing production framing.

## Decision

Add a bounded real-loopback TCP proxy to the production Job Control mutual-TLS test. The proxy owns
two ordinary TCP legs while the production client and server retain their normal `TlsSocket`,
authenticator, authorizer, protocol reader/writer, and reducer service. During mutual authentication,
the proxy transparently forwards raw bytes in both directions. It performs no TLS operation and
cannot create authenticated plaintext.

After both endpoints enter their application states, the proxy captures exactly one complete outer
TLS record. It validates the observed application-data content type, legacy record version, bounded
two-byte payload length, and complete declared record before choosing a cut. For both the encrypted
PREPARE request and its encrypted correlated response, use fresh mutually authenticated sessions
and forward exactly:

1. 2 bytes, inside the five-byte TLS record header;
2. 6 bytes, the complete header plus one encrypted payload byte; and
3. all record bytes except the final byte.

The unforwarded suffix remains owned by the proxy. It then applies `SO_LINGER(0)` and closes only the
proxy's server-facing or client-facing socket, leaving production TLS/socket destruction order
unchanged.

For request cuts, the production server must fail immediately with zero PREPARE dispatches and zero
active jobs. For response cuts, the complete request must be admitted, the production server may
finish its local response write, but the client must fail with no result. Repeating the exact
PREPARE across the three response attempts must retain one job and count only canonical duplicates.
The retained backend listener and service must then accept one complete duplicate PREPARE,
authenticated CANCEL, and original-execution-deadline reclamation.

## Detailed rationale

Capturing a complete record before forwarding a prefix makes each wire boundary exact and prevents
proxy scheduling from being mistaken for record segmentation. The near-complete cut is especially
important: even a complete record header and almost all ciphertext cannot pass OpenSSL integrity or
reach the application decoder. The header and header-plus-one cuts separately cover record framing
without assuming how application bytes align inside ciphertext.

The proxy is below both production TLS endpoints but above the kernel packet layer. It controls TCP
stream bytes, not packets, sequence numbers, TLS keys, or plaintext. Reusing one backend listener and
service across all failures proves that a torn record poisons only its connection. The response
campaign deliberately reuses one query identity so ambiguous delivery exercises the service's
existing idempotent admission contract rather than relying on cleanup between cuts.

## Alternatives considered

- **Expose OpenSSL BIO internals through the production API:** rejected because test control is not a
  production capability and would widen the security-sensitive interface.
- **Write plaintext prefixes through `TlsSocket`:** rejected because OpenSSL still emits complete,
  authenticated records for those prefixes.
- **Forge raw TLS records:** rejected because the resulting ciphertext would not authenticate and
  would not prove interruption of bytes emitted by the production peer.
- **Use packet filtering alone:** a whole-direction drop cannot select offsets within an already
  generated TLS record.

## Consequences

ChronosDB now has deterministic real-TCP evidence for partial encrypted request and response records
at early framing, early ciphertext, and final-byte boundaries. No encrypted request prefix dispatches
a job, no encrypted response prefix publishes success, all affected sessions fail immediately, and
the retained listener/service heals with exactly-once job ownership and bounded cleanup.

The change adds only test and documentation code. It adds no dependency, production API, TLS mode,
wire byte, or durable format. It does not qualify cuts during the TLS handshake, application frames
large enough to span multiple TLS records, resets racing a separately buffered suffix, or general
packet delay, duplication, reordering, probabilistic loss, and bandwidth pressure.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): incomplete encrypted responses cannot publish a query
  result and ambiguous response delivery retains one stable reducer identity.
- [Invariant 10](../architecture/invariants.md): a record missing its final authentication byte never
  reaches integrity-protected application framing.
- [Invariant 11](../architecture/invariants.md): endpoint TLS borrowers remain alive only while their
  owned endpoint descriptors are alive; the proxy resets only its own opposite-leg descriptor.
- [Invariant 14](../architecture/invariants.md): unchanged production TLS and Job Control v1 bytes
  cross every qualified boundary.
- [Invariant 15](../architecture/invariants.md): interrupted sessions terminate immediately and
  retained reducer identity is reclaimed at its original deadline.
- [Invariant 18](../architecture/invariants.md): the seam preserves production TLS, authentication,
  parsing, admission, idempotency, cancellation, and reclamation semantics.

## Validation plan

All seven focused Job Control TLS tests pass, and the new six-cut case passes 25 consecutive Apple
arm64 repetitions. The complete cluster suite passes 367 tests, the allocation-failure suite passes
80, and the grouped-result process suite passes eight. The focused suite passes with GCC 13 on
Ubuntu 24.04 normally and under ASan/UBSan and TSan. The privileged Linux netfilter target passes all
four cases and leaves the OUTPUT chain at its original accept-only policy. Formatting,
workflow-pinning, and whitespace checks pass. Pinned clang-tidy 18 reaches only the known macOS 26
libc++ unsupported-builtin errors after reporting no project-source diagnostic. The final diff is
reviewed for accidental scope expansion.

## Migration or rollback considerations

No migration exists. Rolling back removes only the raw test proxy, its six controlled record cuts,
and the resulting qualification evidence. No deployed state or compatibility boundary changes.

## Unresolved questions

- Qualify TLS handshake-record interruption at controlled wire-byte boundaries.
- Qualify multi-record application frames and resets racing buffered record suffixes.
- Add deterministic delay, duplication, reordering, probabilistic loss, and bandwidth campaigns.
- Qualify larger reducer sets, skew, CPU stalls, and durable query recovery/fencing.

## References

- [Maintained mutual-TLS socket carrier](0144-maintained-mutual-tls-socket-carrier.md)
- [Authenticated partial-frame TCP reset qualification](0554-authenticated-partial-frame-tcp-reset-qualification.md)
- [Admitted PREPARE pre-response TCP reset qualification](0555-admitted-prepare-pre-response-tcp-reset-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
