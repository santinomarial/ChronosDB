# ADR 0093: Durable windowed materialized-view owner

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and durability maintainers
- **Extends:** [ADR 0089](0089-exact-logical-materialized-view-checkpoints.md),
  [ADR 0091](0091-durable-materialized-view-checkpoint-storage.md), and
  [ADR 0092](0092-materialized-view-checkpoint-generations.md)

## Context

The logical view and checkpoint storage were independently correct, but leaving them as separate
callable components did not define the production recovery sequence. A caller could restore a
checkpoint under the wrong source or window configuration, reuse a generation incorrectly, or
release source retention before the new durable boundary had actually been installed.

## Accepted decision

`DurableWindowedMaterializedView` owns one logical view and one locked checkpoint directory. Creation
requires an empty checkpoint history. Reopen loads the latest generated checkpoint, or the latest
legacy checkpoint when no generated file exists, and exact-matches source tablet, source WAL, and
window definition before restoring any state. A restored legacy checkpoint is dirty so its next
successful checkpoint migrates it to generation 1.

Committed inputs remain consecutive and committed-only. Successful application and watermark
changes mark the owner dirty. Checkpointing captures the exact logical state, binds the configured
database/view/table/schema/version/plan identity, advances the monotonic generation, and installs it
through the locked storage owner. Only after the installation has file- and directory-synchronized
does the owner advance its exposed durable record sequence. A clean checkpoint call revalidates the
same immutable generation and is idempotent.

The durable record sequence is the safe source-retention frontier for this view. The owner does not
delete source WAL itself: a higher-level retention coordinator must combine all pins and may release
only through the minimum proven frontier.

## Consequences and alternatives

Recovery is now exactly checkpoint plus the consecutive committed suffix. Watermark-only progress
can be installed without falsely advancing source position. Generation exhaustion fails closed.
Opening an empty existing directory creates a fresh logical view without inventing a durable source
position.

The owner intentionally does not read WAL records or execute a query plan. Converting committed
source mutations into `MaterializedViewInput` depends on service-owned schema and plan semantics;
placing that interpretation in the durability component would conflate recovery ordering with query
execution.

Allowing callers to manipulate storage generation and logical source progress separately was
rejected because there would be no single authority for the retention frontier. Automatically
accepting a configured definition different from the checkpoint was rejected because it would
reinterpret durable state.

## Affected invariants and validation

Invariants 1, 4, 8, 10–15, and 17 apply. A real-filesystem test checkpoints source sequence 1,
installs a watermark-only second generation, reopens exact state, rejects a gapped suffix, applies
sequence 2, installs generation 3, and reopens with the advanced durable frontier. It also covers
clean idempotent checkpointing and refusal to create over existing history. Process-crash points,
syscall faults, source-retention deletion, service plan-to-input replay, schema migration, and
obsolete-generation reclamation remain in the Phase 18 ledger.
