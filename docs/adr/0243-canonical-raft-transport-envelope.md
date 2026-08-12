# ADR 0243: Canonical Group-Scoped Raft Transport Envelope

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Raft, cluster transport, security, and recovery maintainers

## Context

The deterministic Raft core and durable Multi-Raft owner intentionally expose in-memory messages.
ChronosDB already has maintained mutually authenticated cluster socket carriers, but no accepted
Raft wire bytes. Serializing variants ad hoc would make group routing ambiguous, permit a claimed
node identity to diverge from an embedded candidate or leader, and bypass bounded parsing.

## Decision

Adopt [Raft Transport Envelope v1](../formats/raft-transport-v1.md) as a portable, checksummed,
group/source/destination-bound envelope for every current Raft message: vote request/response,
append request/response, two-stage snapshot request/response, and read-barrier request/response.
The codec lives in `chronos_raft` but owns no socket, clock, authentication, retry, or persistence.
No network implementation type enters `RaftNode` or `MultiRaftRuntime`.

The complete frame, append count, entry payloads, and snapshot membership are independently
bounded. Decoding checks header integrity before declared lengths and exact-exhausts every message.
Candidate and leader request identities must match the envelope source. The receiver must separately
authenticate its carrier, authorize the principal for the claimed source, and exact-match the local
destination before runtime dispatch. CRC32C is not authentication.

An embedding releases an encoded outbound message only after the existing durable runtime has
synchronized any persistent state returned by the same transition. Snapshot payload transfer
remains separate; the envelope carries only the existing two-stage installation metadata.

## Consequences

Raft transitions now have stable bounded bytes suitable for the maintained cluster carrier and
deterministic fault simulation. The 64 MiB default envelope can be lower than the core's theoretical
maximum aggregate AppendEntries suffix, so transport owners must configure finite append batching
that fits their selected bound and fail explicitly otherwise.

This decision does not yet provide connection pooling, election timers, retransmission, production
Raft socket scheduling, or automatic metadata routing. Those owners compose above the codec without
changing deterministic state-machine semantics.

## Validation

Focused tests round-trip all eight message kinds with exact route identity, carry a conflict-repair
response produced by `RaftNode`, reject damaged and checksum-valid unknown-kind frames, reject
source/embedded-identity mismatch, and enforce frame, entry, and voter bounds. Full golden fixtures,
fuzzing, authenticated carrier integration, mixed-version processes, and network fault simulation
remain in the Phase 18 ledger.

**Retrospective note (2026-08-12):** [ADR 0245](0245-bounded-raft-transport-partial-io.md) adds the
header-first reader and owning short-write cursor without changing the v1 bytes. [ADR 0246](0246-authenticated-raft-transport-receiver.md)
adds authenticated source authorization, exact local routing, and asynchronous durable admission.

## References

- [ADR 0069](0069-deterministic-raft-and-multiplexed-state-record.md)
- [ADR 0066](0066-authentication-and-tls-integration-boundary.md)
- [ADR 0076](0076-joint-consensus-raft-membership.md)
- [ADR 0078](0078-two-stage-raft-snapshot-installation.md)
- [ADR 0113](0113-linearizable-raft-read-barrier.md)
