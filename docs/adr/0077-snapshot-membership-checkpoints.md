# ADR 0077: Raft snapshot membership checkpoints

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB distributed-systems and storage maintainers

## Context

Joint-consensus membership is reconstructed from retained log commands. A Raft snapshot that
compacts those commands would otherwise leave restart unable to identify the configuration that
governs the suffix. The existing physical state record already stored the snapshot index, term,
manifest generation, and part-set checksum, but not its membership checkpoint.

## Accepted decision

Multiplexed Raft Persistent-State Record v1.1 extends snapshot metadata with the last included
configuration index and its canonical stable voter set. The extension is protected by the existing
payload and record checksums. Encoders write minor version 1; decoders continue to accept minor 0,
whose absent checkpoint is upgraded from the configured group voters when `RaftNode` validates the
recovered state.

For a nonempty snapshot, the checkpoint voter set is the membership base for every retained suffix
command and supersedes the process bootstrap list. It must be nonempty, sorted, unique, nonzero,
bounded, and its configuration index cannot exceed the snapshot index. An empty snapshot cannot
claim a checkpoint or any external snapshot identity: its term, manifest generation, part-set
checksum, and configuration index are zero, and its voter set is empty. This decision supplies only
the durable prerequisite for snapshot compaction; snapshot transfer, application installation, and
log-prefix reclamation require separate state transitions and tests.

Local compaction derives the checkpoint by replaying membership from the existing snapshot base
through exactly the requested prefix. It does not copy the node's potentially later live voter set.
The prefix must end in stable state: a boundary after a joint entry but before its final entry is
invalid because this format cannot encode a joint checkpoint. A final entry at or before the
boundary advances the checkpoint voter set and configuration index. Application snapshot owners use
the core's read-only preparation result before installing bytes.

## Consequences and alternatives

Recovery can validate membership after compacting old joint/final entries without querying mutable
placement metadata. Minor-0 records remain readable and are rewritten as minor 1 on their next
persistent transition. Snapshot records grow by 16 bytes plus eight bytes per voter.

Relying only on current placement metadata was rejected because placement intent may be ahead of or
behind the installed Raft snapshot. Retaining membership entries below the snapshot index was
rejected because it violates the contiguous retained-suffix model and prevents complete prefix
reclamation.

## Affected invariants and validation

Invariants 1, 4, 8, 10, 14, and 18 apply. Focused tests cover v1.1 checkpoint round trip, legacy
minor-0 decode, snapshot checkpoint precedence over bootstrap configuration, and rejection of a
noncanonical checkpoint. Recovery coverage also rejects nonzero identity on an index-zero snapshot.
Compaction coverage rejects a joint-state boundary, accepts a stable boundary before later retained
joint/final entries, reopens that suffix from the older checkpoint, and allocation-sweeps compaction
through the final entry with a retained application suffix. Metadata and tablet owners both preserve
the boundary-time voters when later reconfiguration entries remain live.
Independent minor-0/minor-1 golden fixtures now fix the historical and current bytes for one
nonempty snapshot with a retained suffix. Focused disk recovery loads that minor-0 record through
the segmented log, canonicalizes it through `RaftNode`, writes minor 1, reopens the mixed-format
history, and reclaims the legacy segment behind a current checkpoint. Separate old/new process
interoperability, snapshot installation crash points, and broader reclamation matrices remain
deferred. The physical codec now applies the membership-command `u16` voter ceiling before encoding
or allocation-driving decode. Boundary coverage accepts 65,535 voters, rejects encoder input at
65,536, rejects checksummed count fields at 65,536, 65,537, and `UINT32_MAX`, and proves repeated
strict disk recovery releases the lock without changing the hostile image.
