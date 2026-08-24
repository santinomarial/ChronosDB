# ADR 0269: Node-Wide Checkpointed Raft Log Reclamation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB distributed-systems and recoverability maintainers
- **Extends:** [ADR 0071](0071-segmented-multi-raft-persistence.md)

## Context

Raft application snapshots compact each group's logical retained log, but the multiplexed physical
log remained append-only. Removing a segment after only one group advanced is unsafe because its
latest record may be the only durable state for another group. Requiring retained segment numbers
to start at one also made a missing segment indistinguishable from deliberate prefix reclamation.

## Decision

`DurableMultiRaftRuntime::checkpoint_and_reclaim` is the node-wide reclamation boundary. The
single-thread-affine Multi-Raft owner copies the current persistent state of every resident group in
canonical group order and assigns consecutive next physical sequences. The physical-log owner
starts a fresh segment, appends the complete set, and synchronizes it before publishing a
checksummed [Raft Recovery Anchor v1](../formats/raft-recovery-anchor-v1.md).

The immutable, no-replace-installed anchor names the first retained segment, exact checkpoint
sequence interval, and logical-group count. Its directory entry is synchronized before any older
segment is removed. Recovery selects the highest anchor, validates the exact contiguous checkpoint
and distinct group set, then processes later records normally. Obsolete lower segments and anchors
are cleanup residue only after that validation succeeds.

The operation admits no empty checkpoint and requires every currently known group exactly once.
The existing record and segment bounds also cover the transitional old-plus-checkpoint history. Any
I/O failure poisons the owner; restart either sees the complete old history or a complete anchored
checkpoint. Physical sequences and segment numbers remain monotonic and are never renumbered.

## Consequences and alternatives

Reclamation is coarse and writes one full state per resident group, but it proves that no group's
only recovery record is removed. Per-group segment deletion was rejected because records are
interleaved. In-place segment renumbering was rejected because it mutates durable identity and adds
ambiguous crash states. A replaceable singleton pointer was rejected in favor of immutable
generational anchors compatible with the existing no-replace filesystem primitive.

The first implementation is synchronous and caller-triggered. Scheduling policy, high-cardinality
incremental checkpointing, process-kill fault points, metrics, and physical-device qualification
remain hardening work.

## Validation and invariants

Invariants 1, 4, 5, 8, 10, 11, 14, and 18 apply. Real-filesystem tests cover multi-group shared
prefix reclamation, exact reopen and sequence continuation, stale old-segment cleanup after an
interrupted deletion, damaged-anchor rejection, and durable-runtime composition.
Linux packaged-daemon qualification checkpoints the recovered one-group metadata state through the
public log owner and first proves a clean process reopen. It then damages one checksum-covered anchor
field, requires the exact checksum failure before socket admission, and preserves the anchor and
retained checkpoint segment byte-for-byte.
Portable qualification truncates the authoritative anchor by one byte. Two owner reopens return the
exact invalid-size corruption, release ownership, preserve the truncated anchor and retained
segment, and never fall back to reclaimed segment 1.
A semantic-field companion sets one required-zero anchor byte and recomputes CRC32C. Two owner
reopens return the exact invalid-fields corruption and preserve both files, proving reclamation
authority requires the complete anchor contract rather than checksum validity alone.
Portable aggregate-owner qualification separately removes the synchronized authoritative anchor
after segment 1 is reclaimed. Two database reopens require the exact absent-base corruption, release
ownership, and leave segment 2 unchanged, proving a retained segment number cannot substitute for
anchor authority.
The complementary portable case leaves the anchor intact and damages a checksum-covered field in
segment 2's header. Two owner reopens return the exact header-checksum corruption, release ownership,
and preserve the anchor and damaged segment, proving the anchor never replaces retained-byte
validation.
A retained-record companion damages the first complete checkpoint payload in segment 2. Two owner
reopens return the exact payload-checksum corruption, release ownership, preserve both authority
files, and never reconstruct reclaimed segment 1.
An incomplete-checkpoint companion truncates that record by one byte and authorizes ordinary tail
repair. Two owner reopens still fail closed, preserve the anchor and truncated segment, and never
reconstruct segment 1; an authoritative checkpoint is not a discardable final suffix.
