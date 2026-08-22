# ADR 0267: Durable Metadata Snapshot Installation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB metadata, Raft, and storage maintainers
- **Extends:** [ADR 0266](0266-metadata-application-snapshot-v1.md)

## Context

Metadata Application Snapshot v1 defines independently verifiable bytes, but a successful encoder
does not make those bytes durable or uniquely owned. Raft prefix compaction may happen only after an
exact application snapshot is safely installed; otherwise restart can retain compacted Raft state
without the catalog prefix needed to reconstruct it.

## Decision

`MetadataSnapshotStorage` exclusively locks one configured metadata-group directory. Canonical
final names are `metadata-snapshot-<20-digit-index>.rmas`; recognized temporaries append `.tmp`.
Opening ownership removes only canonical interrupted temporaries and synchronizes that cleanup.

Installation encodes and validates the exact group-owned snapshot before filesystem mutation,
writes one exclusive temporary, exact-rereads and decodes it, synchronizes the file, closes it,
renames without replacement, and synchronizes the directory. An existing byte-identical final is an
idempotent retry; different bytes for the same included index are corruption. Failure after rename
poisons the live owner because durability is uncertain. Loading validates size, framing, group, and
index/name binding. Highest-index selection does not fall back past a damaged selected final.

This owner proves immutable local application-snapshot durability. ADR 0268 composes nested-command
recovery and owned Raft compaction; ADR 0270 adds explicit cleanup only after exact durable Raft
authority is supplied. The storage owner alone does not select which applied boundary is safe or
publish recovered metadata state.

## Consequences and validation

Snapshot bytes can now cross the durable boundary before a later coordinated Raft compaction. The
implementation intentionally mirrors the established tablet application-snapshot installation
protocol, with a separate directory and filename namespace so the two authorities cannot collide.

Real-filesystem tests cover exclusive ownership, first install, exact retry, same-index conflict,
highest selection after reopen, temporary cleanup, and damaged-final rejection. Deterministic
one-shot POSIX injection covers every installation syscall boundary, including partial temporary
writes and the post-rename directory-sync ambiguity. Each failure withholds success; only the
post-rename sync failure poisons the live owner, and a clean reopen removes any temporary, discovers
the exact final authority, and converges under an exact retry. Separate reopen tests fail cleanup
before unlink and after unlink at directory sync, then prove that the next open converges. Process
crash injection, directory/device qualification, reclamation fault injection, and wider runtime
recovery composition remain deferred.

Invariants 1, 2, 4–6, 8, 10, 11, 14, and 18 apply.

## References

- [Metadata Application Snapshot v1](../formats/metadata-application-snapshot-v1.md)
- [ADR 0086](0086-durable-raft-tablet-snapshot-installation.md)
