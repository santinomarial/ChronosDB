# ADR 0083: Manifest v2 temporal WAL recovery composition

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB temporal recovery and storage maintainers
- **Extended by:** [ADR 0084](0084-verified-temporal-checkpoint-overlap.md)
- **Further extended by:** [ADR 0110](0110-multi-tablet-temporal-wal-recovery.md)

## Context

Manifest v2 can select and validate durable CSEG v2 history, and the temporal WAL recovery owner can
replay a complete command-specific log, but neither boundary alone reconstructs one recoverable
query state. Replaying covered WAL commands into an already restored provider would duplicate
versions. Silently skipping a suffix between a global checkpoint and a later tablet boundary would
also be unsafe without comparing each covered command with retained physical history.

## Accepted decision

The first composed startup boundary supports exactly one WAL tablet. The selected Manifest must
carry a global WAL checkpoint whose record sequence exactly equals that tablet's durable position.
The checkpoint's WAL identity is the tablet source identity through Manifest validation. Startup
holds the Manifest lock, loads and revalidates the complete tablet part set, reconstructs a fresh
provider under an explicit caller-proven retained-system-time boundary, and then opens the WAL from
the exact physical checkpoint. Whole-suffix verification and semantic preflight complete before
records strictly after that checkpoint are applied.

The returned move-only owner retains Manifest storage, the selected generation, the provider, and
the reopened WAL writer. Destruction releases provider/generation state and the WAL lock before the
Manifest lock. Recognized temporary files are cleaned only after recovery succeeds; optional WAL
reclamation uses the same already durable checkpoint. Nothing is published outside the owner until
all steps succeed.

Multiple tablets, Raft sources, an absent global checkpoint, or a checkpoint behind the tablet
durable position return `NOT_SUPPORTED`. These shapes require a durable routing/application-snapshot
contract and an exact covered-command verifier; they are not approximated by skipping records.

## Consequences and alternatives

The exact-boundary restriction is intentionally narrower than Manifest v2's general registry, but
it provides a real end-to-end restart path without weakening recovery. It also keeps WAL v1,
Temporal Mutation Command v1, CSEG v2, and Manifest v2 bytes unchanged.

Replaying the whole WAL into empty state and ignoring the selected CSEG was rejected because it
defeats reclaimed prefixes. Applying the whole WAL after CSEG restore was rejected because it
duplicates covered versions. Skipping through each tablet boundary without content verification
was rejected because a mismatched Manifest/WAL pair could become query-visible.

## Affected invariants and validation

Invariants 1–8, 11–14, and 18 apply. A real-filesystem focused test creates deterministic CSEG
history through record 9, installs an exact Manifest/checkpoint at that physical boundary, appends a
record-10 correction, and proves recovery returns historical state plus that suffix and a writer at
sequence 11. It also proves an absent retained-history boundary fails before publication. Covered
command verification and multiple distinct-tablet routing are implemented. Exhaustive allocation
sweeps cover empty, one-CSEG, two-CSEG, and one-CSEG-plus-empty-two-tablet startup shapes, including
complete rollback and reacquisition of both locks. Raft snapshot composition, crash injection,
larger combined part/tablet allocation matrices, reclamation faults, and broader restart matrices
remain Phase 18 follow-up work.
