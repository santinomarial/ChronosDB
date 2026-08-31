# ADR 0560: Controlled encrypted-record byte-corruption qualification

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB networking, cluster-security, distributed-query, and test maintainers
- **Extends:** [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md),
  [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md), and
  [ADR 0556](0556-controlled-partial-tls-record-qualification.md)

## Context

ADR 0556 qualified encrypted application records truncated at three prefix boundaries, and ADR 0559
qualified a torn later record after one complete record of a larger request had been consumed. Those
tests did not alter bytes inside a record while preserving later bytes. A forwarding defect that
duplicates, reorders, or loses an interior ciphertext byte must remain below both Job Control parsing
and reducer admission.

This is a TLS-record byte-transformation gate, not a claim about kernel packet scheduling. TCP hides
ordinary packet duplication and reordering from the application; the raw proxy intentionally models
what the TLS owner would observe if a faulty intermediary changed its encrypted byte stream.

## Decision

Add three exact real-loopback campaigns using the retained backend listener and raw two-leg proxy.
Each campaign completes production mutual TLS, lets the production client emit one complete encrypted
PREPARE record, and selects an interior ciphertext position beyond the five-byte outer header.

The proxy performs exactly one deterministic mutation while leaving the original TLS record length
field unchanged:

1. insert a duplicate of the selected ciphertext byte;
2. swap the selected byte with its successor; or
3. erase the selected ciphertext byte.

It forwards the resulting byte sequence and resets both proxy-owned legs. Duplication makes OpenSSL
authenticate the declared-length prefix with shifted ciphertext and leaves one trailing byte;
reordering preserves the length but invalidates record authentication; deletion leaves the declared
record one byte incomplete until reset. In every case both production session owners must fail, the
client must publish no result, and the service must report zero PREPARE requests and zero active jobs.

After all three failures, a direct connection through the same listener must complete PREPARE,
CANCEL, and execution-deadline reclamation.

## Detailed rationale

Mutating encrypted bytes rather than decoded Job Control bytes keeps the test at the intended trust
boundary. The proxy never owns TLS keys and cannot synthesize an authenticated alternative request.
The unchanged outer length also distinguishes the cases: two reach an integrity check with a complete
declared record, while interior loss reaches the reset path with a missing declared byte.

Using the middle of the encrypted payload avoids conflating this evidence with the previously
qualified header, first-payload-byte, or final-byte prefix cuts. Zero service dispatch proves both TLS
integrity and complete application framing remain prerequisites for reducer state.

## Alternatives considered

- **Mutate plaintext before encryption:** rejected because production OpenSSL would authenticate the
  changed request normally and the test would exercise only application validation.
- **Change the outer length with the payload:** rejected because a self-consistent shorter or longer
  record is a different framing test, not an interior byte-stream fault.
- **Call these packet duplication and reordering tests:** rejected because TCP packet recovery occurs
  below the socket byte stream and requires a separate kernel/network-emulation campaign.
- **Accept either failure or timeout:** rejected because the proxy makes reset visible immediately and
  both owners must terminate on that event.

## Consequences

ChronosDB now has exact evidence that duplicated, reordered, and lost interior ciphertext cannot
authenticate or dispatch one PREPARE. Both mutual-TLS owners terminate, no partial result or job is
published, and the retained listener/service heals normally.

The change adds only test and documentation code. It adds no dependency, production API, TLS option,
network byte, or durable format. Packet-level delay, duplication, reordering, probabilistic loss,
bandwidth pressure, multi-record responses, later unusually segmented handshake records, and durable
query recovery and fencing remain separate.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): unauthenticated altered bytes cannot enter query
  visibility machinery.
- [Invariant 6](../architecture/invariants.md): no reducer state is published from a corrupt request.
- [Invariant 11](../architecture/invariants.md): proxy resets preserve production descriptor borrowing
  and leave no partial job owner.
- [Invariant 14](../architecture/invariants.md): only the canonical versioned request is acceptable;
  altered encrypted bytes do not become another protocol frame.
- [Invariant 15](../architecture/invariants.md): the reset path terminates both bounded sessions.
- [Invariant 18](../architecture/invariants.md): OpenSSL record integrity and mutual authentication
  remain mandatory before application dispatch.

## Validation plan

All ten focused Job Control TLS tests pass, and the new three-mutation case passes 25 consecutive
Apple arm64 repetitions. The complete cluster suite passes 370 tests, the allocation-failure suite
passes 80, and the grouped-result process suite passes eight. The focused suite passes with GCC 13
on Ubuntu 24.04, under ASan/UBSan, and under TSan. The privileged Linux packet suite passes all four
cases and leaves the OUTPUT chain at its original accept-only policy. Formatting, workflow-pinning,
and whitespace checks pass.

The Ubuntu GCC build reaches the focused suite with warnings non-fatal and passes it. Enabling
repository-wide warnings-as-errors stops earlier in unchanged `src/cseg/metadata_codec.cpp` on GCC's
`-Wduplicated-branches`, before this test compiles. Pinned clang-tidy 18 reaches only the known macOS
26 libc++ unsupported-builtin compiler errors after reporting no project-source diagnostic.

## Migration or rollback considerations

No migration exists. Rolling back removes only the mutation enum, three corruption campaigns, and
their documentation. No deployed identity, stored state, or compatibility boundary changes.

## Unresolved questions

- Add kernel- or emulator-level deterministic packet delay, duplication, reordering, loss, and
  bandwidth campaigns without misrepresenting TCP guarantees.
- Qualify multi-record responses on a naturally large bounded response protocol.
- Qualify later encrypted client-handshake records under forced unusual segmentation.
- Qualify reset timing with more already-buffered suffix bytes.
- Qualify larger reducer sets, skew, CPU stalls, and durable query recovery and fencing.

## References

- [Maintained mutual-TLS socket carrier](0144-maintained-mutual-tls-socket-carrier.md)
- [Mutually authenticated grouped reducer Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Controlled partial TLS-record qualification](0556-controlled-partial-tls-record-qualification.md)
- [Controlled multi-record Job Control request qualification](0559-controlled-multi-record-job-control-request-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
