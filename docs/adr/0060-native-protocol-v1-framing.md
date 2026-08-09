# ADR 0060: Native Protocol v1 Framing

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB networking and protocol maintainers
- **Extended by:** [ADR 0094](0094-native-protocol-1-1-subscriptions.md)

## Context

ADR 0009 requires a portable, versioned, bounded frame codec before a Linux reactor can allocate
or dispatch peer-controlled bytes. The deferred byte layout must distinguish compatibility,
request identity, message lifecycle, and corruption without serializing native structures.

## Accepted decision

Native Protocol v1 uses one fixed 40-byte little-endian header followed by an exact payload. The
header carries magic, major/minor version, header size, assigned message type, flags, request ID,
payload length, payload CRC32C, a zero reserved field, and a header CRC32C. The header checksum
covers bytes `[0,36)` and the payload checksum covers exactly the payload. The accepted Protocol v1
ceiling is 16 MiB; deployments may configure a smaller nonzero limit.

Major version 1 and minor version 0 are the only emitted version. A decoder accepts the same major
and a minor no newer than its own. Unknown types, unknown flags, nonzero reserved fields, header
extensions, oversized payloads, checksum mismatches, truncation, or trailing bytes fail closed.
`END_STREAM` is the only v1 flag and is valid only on `QUERY_RESULT`.

The portable codec owns decoded payload bytes. Header validation completes before any payload-sized
allocation. Allocation failure is classified as resource exhaustion.

## Detailed rationale

A fixed header makes minimum-byte admission and partial-read state explicit. Separate header and
payload checksums prevent a corrupt length from controlling allocation and allow the header to be
validated before the body arrives. A u64 request ID supports multiplexed request lifecycles while
remaining connection-scoped; it is not a durable database identity.

## Alternatives considered

- **Length prefix only:** cannot protect interpretation fields before dependent allocation.
- **Native C++ structure:** has ABI, padding, alignment, and byte-order instability.
- **Variable header in v1:** adds parser and allocation complexity without an accepted extension.
- **Checksum only over the complete frame:** delays safe header validation until the body arrives.
- **Cryptographic MAC:** belongs to the later authentication/TLS boundary; CRC32C detects accidental
  corruption and does not claim peer authenticity.

## Consequences

- The byte layout and assigned type numbers are public compatibility commitments.
- Reactors can read 40 bytes, validate a finite body requirement, and only then admit storage.
- Payload semantics remain independently versioned and state-machine constrained.
- Implementations must not treat a valid CRC as authentication.

## Affected invariants

This decision directly supports invariants 10, 14, and 17 by covering interpretation fields,
versioning the first network format, and bounding peer-controlled allocation.

## Validation plan

- Golden exact-byte fixtures and independent CRC checks.
- Every truncation boundary, trailing bytes, bit corruption, unknown version/type/flag, and limit.
- Deterministic round trips and allocation-failure sweeps.
- Portable codec fuzzing before reactor integration.
- Installed external-consumer and public-header coverage.

## Deferred decisions

Handshake payloads, error payloads, ingest/query payload schemas, state transitions, authentication,
TLS, reactor ownership, queue memory ordering, and overload policy remain later Phase 10 increments.

## Migration or reversal implications

Changing a field offset, checksum range, assigned type, or existing flag meaning requires a new
protocol major version and a superseding ADR. Compatible optional semantics may use a later minor
version only with explicit negotiation and golden mixed-version fixtures before acceptance.

## References

- [Native Protocol v1](../protocol/native-v1.md)
- [ADR 0009](0009-network-reactor-strategy.md)
- [Architecture invariants](../architecture/invariants.md)
- [Phase 10 roadmap](../roadmap.md)
