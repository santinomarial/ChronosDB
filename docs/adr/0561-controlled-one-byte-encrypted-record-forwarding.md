# ADR 0561: Controlled one-byte encrypted-record forwarding

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB networking, cluster-security, distributed-query, and test maintainers
- **Extends:** [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md),
  [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md), and
  [ADR 0556](0556-controlled-partial-tls-record-qualification.md)

## Context

The controlled TLS-record campaigns qualified resets at exact prefixes, a torn later request record,
and deterministic ciphertext corruption. They did not prove the corresponding success boundary:
valid encrypted request and response bytes may arrive in arbitrarily small stream fragments, and the
application must neither act on an incomplete record nor require coalesced delivery to finish.

TCP exposes a byte stream rather than packet boundaries. This qualification therefore controls bytes
at the raw proxy and scheduler boundary. It is not a claim about kernel packetization, elapsed-time
bandwidth, or adverse network delay.

## Decision

Add one real-loopback campaign using the retained backend listener and raw two-leg proxy. After the
production client and server complete mutual TLS, the client emits a valid PREPARE. The proxy captures
each complete encrypted application record, sends exactly one record byte at a time, waits for the
destination descriptor to become readable, and gives the production server one read-ready step at the
same logical instant after every byte.

While the server remains in `ReadingRequest`, the service must report zero PREPARE requests and zero
active jobs. Only processing the final required encrypted request byte may move the server to
`WritingResponse` and atomically admit the job.

The server then emits its correlated response. The proxy applies the same one-byte schedule in the
opposite direction. While the client remains in `ReadingResponse`, it must publish no result. Only
processing the final required encrypted response byte may make the client complete with `kOk`.
Authenticated CANCEL and original-execution-deadline reclamation through the same listener complete
the ownership check.

All forwarding steps use one fixed logical time below the existing exchange deadline. The test
qualifies fragmentation tolerance independently of deadline expiration.

## Detailed rationale

Waiting for raw readability after each send prevents the test driver from racing the kernel and
mistaking an early `WANT_READ` for consumption of the newly scheduled byte. Production TLS still owns
record authentication and plaintext release; the proxy never holds keys or calls an application
decoder.

Checking application state after every byte establishes both halves of the boundary. A prefix cannot
dispatch or publish, while the complete authenticated record cannot remain stuck merely because it
arrived as one-byte stream fragments.

## Alternatives considered

- **Send a record in one write:** rejected because it does not qualify maximal scheduler-controlled
  stream fragmentation.
- **Sleep between bytes:** rejected because wall-clock scheduling is nondeterministic and would
  conflate fragmentation with the exchange deadline.
- **Call each byte a TCP packet:** rejected because the kernel may segment or coalesce writes and TCP
  does not expose packet boundaries to the application.
- **Inspect OpenSSL internals:** rejected because externally visible state, service metrics, and the
  final result are the maintained ownership and publication contracts.

## Consequences

ChronosDB now has exact real-socket evidence that a valid Job Control request and response remain
invisible to the application until their authenticated records are complete and then finish under a
one-byte scheduler-controlled forwarding pattern. The test also preserves normal cancellation and
bounded reclamation afterward.

The change adds only test and documentation code. It adds no dependency, production API, TLS option,
network byte, or durable format. It does not qualify wall-clock bandwidth limits, packet-level delay,
duplication, reordering or probabilistic loss, naturally multi-record responses, or durable query
recovery and fencing.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): no incomplete request or response becomes visible.
- [Invariant 6](../architecture/invariants.md): reducer admission occurs only after a complete
  authenticated request.
- [Invariant 11](../architecture/invariants.md): TLS socket ownership and listener reuse remain
  unchanged through fragmented delivery and cleanup.
- [Invariant 14](../architecture/invariants.md): only the complete canonical protocol frame crosses
  the application boundary.
- [Invariant 15](../architecture/invariants.md): every step remains within the existing bounded
  exchange lifetime.
- [Invariant 18](../architecture/invariants.md): mutual TLS authenticates complete records before
  application dispatch or result publication.

## Validation plan

All eleven focused Job Control TLS tests pass, and the new one-byte forwarding case passes 25
consecutive Apple arm64 repetitions. The complete cluster suite passes 371 tests, the
allocation-failure suite passes 80, and the grouped-result process suite passes eight. The focused
suite passes with GCC 13 on Ubuntu 24.04, under ASan/UBSan, and under TSan. The privileged Linux
packet suite passes all four cases and leaves the OUTPUT chain at its original accept-only policy.
Formatting, workflow-pinning, whitespace, and final-diff checks pass.

The Ubuntu GCC build reaches the focused suite with warnings non-fatal and passes it. Enabling
repository-wide warnings-as-errors stops earlier in unchanged `src/cseg/metadata_codec.cpp` on GCC's
`-Wduplicated-branches`, before this test compiles. Pinned clang-tidy 18 reaches only macOS 26 libc++
unsupported-builtin and related compiler errors after reporting no project-source diagnostic.

## Migration or rollback considerations

No migration exists. Rolling back removes only the one-byte forwarding campaign and its documentation.
No deployed identity, stored state, or compatibility boundary changes.

## Unresolved questions

- Add kernel- or emulator-level deterministic delay, duplication, reordering, probabilistic loss,
  and bandwidth campaigns without misrepresenting TCP guarantees.
- Qualify naturally multi-record responses when a bounded response protocol can produce one.
- Qualify later encrypted client-handshake records under forced unusual segmentation.
- Qualify resets racing more already-buffered suffix bytes.
- Qualify larger reducer sets, skew, CPU stalls, and durable query recovery and fencing.

## References

- [Maintained mutual-TLS socket carrier](0144-maintained-mutual-tls-socket-carrier.md)
- [Mutually authenticated grouped reducer Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Controlled partial TLS-record qualification](0556-controlled-partial-tls-record-qualification.md)
- [Controlled encrypted-record byte-corruption qualification](0560-controlled-encrypted-record-byte-corruption-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
