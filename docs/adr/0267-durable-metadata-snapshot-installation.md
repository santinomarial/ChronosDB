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
before unlink and after unlink at directory sync, then prove that the next open converges. An
eight-cut real-process `SIGKILL` matrix covers temporary creation, write, readback, file sync,
temporary close, final rename, directory sync, and post-success release. Every cut reopens to the
exact absent or installed authority, removes interrupted temporaries, converges through an
idempotent retry, and survives a second reopen. Directory/device qualification and wider runtime
recovery schedules remain deferred. The declared 65,536-entry catalog ceiling is also exercised
through exact install, owned load, idempotent retry, owner teardown, reopen, and highest-snapshot
recovery. Local-only real-time benchmark shapes measure fresh durable installation and checked
restart recovery at 1,024, 16,384, and 65,536 entries without weakening synchronization; they are
not device or production-throughput claims. ADR 0268 now composes application partial-write and
post-rename directory-sync faults, plus every other post-ownership installation failure stage, with
every later Raft record-write and data-sync failure shape. It also composes interrupted-temporary
cleanup failure before unlink or after unlink at directory sync with all five later Raft persistence
outcomes. Metadata snapshot reclamation fault injection is covered separately by ADR 0270.

Invariants 1, 2, 4–6, 8, 10, 11, 14, and 18 apply.

## References

- [Metadata Application Snapshot v1](../formats/metadata-application-snapshot-v1.md)
- [ADR 0086](0086-durable-raft-tablet-snapshot-installation.md)
