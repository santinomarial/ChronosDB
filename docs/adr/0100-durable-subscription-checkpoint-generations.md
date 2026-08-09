# ADR 0100: Durable multi-tablet subscription checkpoint generations

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query and storage maintainers
- **Extends:** [ADR 0099](0099-multi-tablet-subscription-checkpoint-v1.md)

## Context

The logical coordinator checkpoint and its bound generation envelope are not restart state until an
owner installs the bytes atomically and can select them without accepting a renamed, partial,
foreign, or noncontiguous generation.

## Accepted decision

One checkpoint directory belongs to one exact database, table, plan fingerprint, schema/version,
and canonical tablet/WAL source set. A process must hold the directory's advisory `LOCK` for the
owner lifetime. Immutable files are named `generation-%020u.subc`, starting at one with no gaps.

Installation accepts only the next generation, except that retrying an already installed generation
with byte-identical content succeeds idempotently. The owner writes an exclusive canonical
temporary, reads and decodes the complete bytes back, synchronizes the file, closes it, renames it
without replacement, and synchronizes the directory. A directory-sync failure after the rename
poisons the owner because crash durability is then uncertain.

Reopen removes only canonical generation temporaries and synchronizes that removal. Selection
rejects malformed recognized entries and generation gaps, then decodes the latest file and checks
that its embedded generation, bound identity, and source set agree with the filename and owner.
Unknown unrelated names do not become checkpoint authority.

## Consequences and alternatives

Old generations remain immutable and are not reclaimed by this owner yet. This costs space but
keeps recovery selection independent from a mutable pointer file and preserves prior evidence for
repair. Retention and garbage collection require a later policy that never removes the latest
durable recovery point prematurely.

Overwriting a fixed checkpoint filename was rejected because a crash can destroy the only recovery
point. Rename-over-existing was rejected because it makes collisions and stale retries ambiguous.
Treating the largest filename as authoritative without checking continuity or the embedded envelope
was rejected because directory manipulation could silently advance recovery state.

## Affected invariants and validation

Invariants 10, 12, 14, 15, and 17 apply. Focused tests cover exclusive ownership, exact next-
generation admission, byte-identical retry, conflicting retry rejection, latest selection, reopen,
interrupted-temporary cleanup, and installed-byte corruption. Filesystem fault injection, crash
cut-point testing, generation reclamation, and cross-platform durability validation remain Phase 18
work.
