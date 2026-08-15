# ADR 0405: Verified logical-to-physical WAL prefix resolution

- **Status:** accepted
- **Date:** 2026-08-15
- **Owners:** ChronosDB WAL and live-query maintainers
- **Extends:** [ADR 0106](0106-topology-bound-subscription-retention.md)

## Context

Subscription retention authorizes a logical `(wal_id, record_sequence)` prefix. Physical WAL
reclamation instead requires an exact `WalReplayCheckpoint` containing the record-end segment and
byte offset. Casting a sequence to an offset, trusting an append-time coordinate retained only in
memory, or scanning only until the requested record would allow corrupted later history to
authorize deletion.

The WAL may also have already removed whole covered prefix segments. Resolution therefore cannot
assume segment 1 remains, and an older idempotent request may have no physical file left to delete.

## Accepted decision

`WalWriter::resolve_replay_checkpoint` is the serialized, read-only mapping operation. It accepts
only a sequence covered by the writer's durable frontier. Under the writer's existing directory
lock it discovers the current namespace, validates the first retained header, derives the exact
predecessor boundary admitted by WAL suffix recovery, and scans the complete retained history.
The scan must agree with the live writer's WAL identity, physical end, and final record sequence.

When the requested record remains, the resolver returns its decoded record-end segment and byte
offset. When it equals the retained predecessor it returns that exact suffix checkpoint. When it is
older than the first retained record, it returns no checkpoint: every whole segment it could have
authorized is already absent, so a caller can treat the request as an idempotent no-op. A request
beyond the durable record sequence fails before namespace inspection.

Corruption, unsupported bytes, and I/O failures poison the live writer because its authoritative
namespace can no longer be trusted. Invalid or not-yet-durable requests and bounded allocation
failure do not poison it. Resolution performs no unlink and therefore can prevalidate every source
in a higher-level batch before the first deletion.

## Consequences and alternatives

Resolution is `O(retained WAL bytes)` and allocates at most one format-bounded record plus directory
discovery state. This is intentionally a reclamation-path operation, not an ingest-path index.
Building and durably maintaining a second sequence-to-offset index was rejected without evidence
that the reclamation scan is a bottleneck. Stopping the scan at the target was rejected because a
damaged required suffix must fail before deletion authorization.

The returned coordinate is not itself deletion permission. The caller still needs an independently
durable storage/subscription frontier and must invoke `reclaim_checkpointed_segments`, which
revalidates the namespace immediately before mutation.

## Affected invariants and validation

Invariants 8, 10, 11, 12, 14, and 17 apply. Focused tests resolve an exact cross-segment record end,
recognize an older already-absent prefix after reclamation, and reject a written but unsynchronized
record without poisoning the writer. Higher-level WAL batch composition, injected deletion faults,
and Raft prefix mapping remain separate work.
