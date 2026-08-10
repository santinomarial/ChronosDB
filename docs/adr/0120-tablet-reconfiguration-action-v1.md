# ADR 0120: Tablet Reconfiguration Action v1

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extends:** [ADR 0119](0119-deterministic-tablet-reconfiguration-action-identities.md)

## Context

A stable in-memory action ID is insufficient for a durable retry ledger or leader-routed request.
Those owners need canonical bytes binding the identity, destination Raft group, supported operation,
and exact request payload without serializing native variants.

## Decision

Tablet Reconfiguration Action v1 uses a 96-byte checksummed header containing the tablet, movement
epoch, action kind, destination group, payload length, and payload checksum. A trailer checksum
covers the complete header and payload. All integers are little-endian and all reserved bytes are
zero.

Only three exact operations are representable:

- begin-joint carries a bounded canonical voter vector;
- finalize-joint carries no payload; and
- publish-placement carries entry type 2 plus a complete exact-decoded Tablet Placement Metadata
  command.

The identity kind, outer kind, and operation variant must agree. Tablet/epoch/group identities are
nonzero. Decoded nested placement bytes must remain a valid canonical metadata command for the same
tablet and exactly the next placement epoch. Other Raft operations fail closed rather than gaining
accidental retry semantics.

## Consequences and alternatives

The durable ledger can exact-compare same-ID bytes, diagnose destinations, and replay only supported
reconfiguration requests. The envelope duplicates integrity over an already checksummed metadata
command; this deliberately protects outer interpretation and catches splicing or destination
changes.

Serializing `std::variant` or native structs was rejected as ABI-dependent. Storing only a digest
was rejected because restart needs the exact request. Making the format generic to every Raft
operation was rejected because receive, heartbeat, reads, and snapshot control have different retry
contracts and no current use in this ledger.

## Validation and follow-up

Focused tests exact-round-trip all three supported actions and reject damage, identity/kind
mismatch, and unsupported operations. Invariants 8, 10, 14, and 18 apply. Golden fixtures, fuzzing,
hostile length matrices, future-version compatibility, filesystem installation, transport framing,
and authenticated routing remain follow-up work.

## References

- [Tablet Reconfiguration Action v1 format](../formats/tablet-reconfiguration-action-v1.md)
- [ADR 0119](0119-deterministic-tablet-reconfiguration-action-identities.md)
