# ADR 0266: Metadata Application Snapshot v1

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB metadata, Raft, and recoverability maintainers
- **Extends:** [ADR 0075](0075-durable-metadata-raft-commands.md) and
  [ADR 0214](0214-durable-complete-schema-definitions.md)

## Context

The dedicated metadata Raft group can reconstruct nodes, complete schemas, table policies, and
tablet placements only while its entire committed log remains physically retained. Raft already
supports safe prefix compaction, but `DurableMetadataStateMachine` rejects a nonzero snapshot
boundary because no versioned application bytes can reconstruct the omitted catalog.

Encoding only the latest maps would require a second catalog grammar and would lose the exact
command-order evidence used to validate schema succession and placement epochs. Metadata traffic is
small enough that an exact retained application stream is the safer first snapshot representation.

## Decision

Metadata Application Snapshot v1 binds one nonzero metadata group and complete Raft
`SnapshotMetadata` to the original application-bearing entries in its compacted prefix. Each stored
entry retains its logical index, term, permanent entry type, and exact Metadata Command v1 or Schema
Definition v1 payload. Internal Raft entries are represented by gaps because the snapshot metadata
already carries their resulting stable membership checkpoint.

The codec requires strictly increasing indexes, nondecreasing terms, only entry types 2 and 3,
nonempty bounded payloads, canonical voters, explicit zero padding, per-entry CRC32C, header CRC32C,
and whole-file CRC32C. It owns all decoded bytes and rejects unknown versions, reserved fields,
damage, trailing bytes, and caller-limit violations before unbounded allocation.

This checkpoint freezes the portable bytes and structural codec. The durable owner must separately
decode every nested command, replay gaps as internal no-ops, install the immutable snapshot before
compacting Raft, and exact-match both owners during recovery. Until that composition exists, the
metadata physical log prefix remains retained.

## Consequences and validation

The format duplicates logical metadata command bytes, trading space for exact semantic replay and a
small audit surface. It enables later physical-log reclamation without creating a last-writer-wins
catalog or reinterpreting existing command formats. Future compact state images require a new major
version and equivalent schema/placement succession validation.

Focused tests cover canonical round trip with internal gaps, an internal-only prefix, damage,
unknown types, ordering, and resource bounds. Durable installation, crash points, recovery,
compaction, prefix reclamation, fuzzing, and large-catalog qualification remain follow-up work.

Invariants 1, 4–6, 8, 10, 11, 14, and 18 apply.

## References

- [Metadata Application Snapshot v1](../formats/metadata-application-snapshot-v1.md)
- [Metadata Command v1](../formats/metadata-command-v1.md)
- [Schema Definition v1](../formats/schema-definition-v1.md)
- [Multiplexed Raft Persistent-State Record v1](../formats/multiplexed-raft-record-v1.md)
