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
Golden fixtures, corruption campaigns, mixed-version process tests,
snapshot installation crash points, and reclamation remain deferred.
