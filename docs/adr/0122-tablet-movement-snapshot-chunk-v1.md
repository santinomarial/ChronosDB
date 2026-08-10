# ADR 0122: Tablet Movement Snapshot Chunk v1

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0117](0117-tablet-movement-checkpoint-v1.md)
- **Extended by:** [ADR 0123](0123-durable-tablet-movement-snapshot-chunks.md)

## Context

Self-contained movement checkpoints make restart exact but rewrite the entire received prefix as a
snapshot grows. Resumable transfer needs independently verifiable pieces that cannot be spliced
between tablets, movement epochs, source/target pairs, or snapshot boundaries.

## Decision

Tablet Movement Snapshot Chunk v1 is a bounded canonical value with a 128-byte little-endian header,
nonempty payload, and whole-record CRC32C trailer. The header binds tablet, placement epoch,
source/target nodes, snapshot manifest generation, applied Raft index/term, total snapshot size and
content CRC, chunk offset and length, and payload CRC. A separate header CRC protects all framing and
interpretation fields before payload use.

Chunks must fit wholly within the declared nonempty snapshot and configured snapshot/chunk/encoded
limits. Identities and snapshot boundary fields are nonzero, source and target differ, reserved
bytes are zero, and unknown versions fail closed. This format does not yet define filenames,
installation ordering, or when a complete set becomes application-snapshot authority.

## Rationale and alternatives

Repeating the full session identity in every piece makes each installed file independently
auditable and rejects cross-session substitution without trusting directory metadata. Offset and
length remain explicit so the future owner can require a gap-free exact prefix.

Raw payload files plus a side manifest were rejected because either side can be copied or advanced
independently. A mutable append file was rejected for this first owner because torn-tail repair and
in-place progress metadata add a second recovery protocol. Native structure serialization was
rejected as ABI- and endianness-dependent.

## Consequences and validation

Transfer pieces can be independently checksummed and later installed immutably. Per-chunk framing
adds 132 bytes and repeats session metadata, trading modest space for simple recovery. Focused tests
exact-round-trip session/offset/payload and reject damage, out-of-range bytes, and invalid identity.

Invariants 2, 8, 10, 14, and 18 apply. ADR 0123 implements filesystem installation,
contiguous-prefix reconstruction, exact retry, and final whole-snapshot validation. Checkpoint
integration, final RTAS/Manifest/CSEG installation, crash points, fuzzing, and large-transfer
testing remain follow-up work.

## References

- [Tablet Movement Snapshot Chunk v1 format](../formats/tablet-movement-snapshot-chunk-v1.md)
- [ADR 0117](0117-tablet-movement-checkpoint-v1.md)
- [ADR 0086](0086-durable-raft-tablet-snapshot-installation.md)
