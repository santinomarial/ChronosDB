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

This checkpoint freezes the portable bytes and structural codec. The durable composition in
[ADR 0268](0268-owned-metadata-snapshot-compaction.md) separately decodes every nested command,
replays gaps as internal no-ops, installs the immutable snapshot before compacting Raft, and
exact-matches both owners during recovery.

## Consequences and validation

The format duplicates logical metadata command bytes, trading space for exact semantic replay and a
small audit surface. It enables later physical-log reclamation without creating a last-writer-wins
catalog or reinterpreting existing command formats. Future compact state images require a new major
version and equivalent schema/placement succession validation.

Focused tests cover canonical round trip with internal gaps, an internal-only prefix, damage,
unknown types, ordering, and resource bounds. Independently packed golden fixtures freeze the
complete minor-0 and minor-1 bytes, including membership, application SHA-256 identity, entry
alignment, nested-payload CRC32C, header CRC32C, and whole-file CRC32C; production encoding and
decoding must match both. Exhaustive test-only allocator sweeps fail every observed minor-0 and
minor-1 encode/decode allocation, require `RESOURCE_EXHAUSTED`, and reach an exact successful retry.
A structure-aware ASan/UBSan libFuzzer target covers raw hostile bytes, generated canonical minors,
valid nested commands/bindings, caller-limit rejection, checksum-repaired mutation, truncation, and
stable semantic re-encoding; the deterministic bounded smoke runs in CI. ADRs 0267–0270 now cover
durable installation, process crash points, owned recovery/compaction, physical-log reclamation, and
obsolete snapshot reclamation. An exact structural scale test round-trips the declared 65,536-entry
maximum with nine voters and valid nested command payloads, then rejects a lowered entry limit and
entry 65,537. Local-only encode/decode benchmarks publish entry, payload, and complete snapshot-byte
counters at 1,024, 16,384, and 65,536 entries. ADR 0267 additionally qualifies the same shapes
through the unchanged durable installation and restart-recovery protocol. Sustained fuzz campaigns
remain follow-up work.

Invariants 1, 4–6, 8, 10, 11, 14, and 18 apply.

## References

- [Metadata Application Snapshot v1](../formats/metadata-application-snapshot-v1.md)
- [Metadata Command v1](../formats/metadata-command-v1.md)
- [Schema Definition v1](../formats/schema-definition-v1.md)
- [Multiplexed Raft Persistent-State Record v1](../formats/multiplexed-raft-record-v1.md)
