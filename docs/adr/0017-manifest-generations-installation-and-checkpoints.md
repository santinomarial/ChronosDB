# ADR 0017: Manifest Generations, Part Installation, and Checkpoints

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB storage and recovery maintainers

## Context

CSEG v1 now produces exact validated immutable file images, but a valid candidate is not durable or
query-visible. Phase 6 needs an authority that can atomically substitute durable parts for sealed
heads, reconstruct retry and tablet state, and prove when a WAL prefix is no longer the only copy.
The protocol must recover to old-complete or new-complete state at every crash point and cannot
depend on an object-store listing, a mutable pointer torn across files, or opportunistic cleanup.

The current WAL is database-wide while mutable and durable data are tablet-scoped. One tablet may
flush farther than another, and exact retry no-ops advance application positions without adding
rows. A single undifferentiated row watermark is therefore insufficient for correct replay.

## Decision

Adopt the normative [Manifest v1 specification](../formats/manifest-v1.md) and the associated
[installation architecture](../architecture/manifest-installation-and-checkpointing.md):

- each durable generation is one checksummed immutable database-wide full snapshot;
- the highest consecutive final generation is authoritative, with no fallback on invalid highest
  state and no mutable `CURRENT` pointer;
- manifest state contains per-tablet durable recovery/schema boundaries, installed CSEG descriptors,
  all protected committed retry outcomes, and one global WAL reclaim coordinate;
- installed CSEG and manifest files use exact identity-derived final names, exclusive temporary
  creation, complete readback validation, file sync, atomic no-replace rename, and directory sync;
- parts cross their durability boundary before any manifest can reference them;
- in-memory publication atomically replaces a sealed-head reference with the selecting manifest
  generation only after the manifest directory sync; old snapshot owners retain their exact heads
  and manifest generation;
- checkpoint publication precedes WAL prefix deletion, and deletion is a separately synchronized,
  repeatable cleanup step;
- manifest-aware WAL recovery starts at the durable global coordinate and validates covered
  per-tablet retry no-ops before applying the suffix; and
- recognized temporary files are cleanup candidates, while unreferenced final parts are retained
  and reported until a later reclamation owner is accepted.

Phase 6 writes complete snapshots rather than a durable edit log. Its transition validator permits
monotonic tablet/checkpoint advancement, part addition, and retry-entry addition. Part removal and
retry pruning are rejected until later accepted compaction and retention contracts.

No production dependency implements manifests, installation, checkpoints, or recovery. The existing
POSIX I/O abstraction and common CRC32C primitive are the permitted foundations.

## Detailed rationale

A complete immutable snapshot makes one file sufficient to reconstruct authoritative state and
keeps interrupted edits out of the recovery model. Selecting the highest final name avoids another
mutable pointer file. The rename-before-directory-sync crash ambiguity is safe in either direction:
if the complete final name exists, all referenced parts are already durable; otherwise the previous
generation remains selected.

Failing rather than falling back from an invalid highest generation prevents silent state rollback,
especially after its checkpoint allowed WAL cleanup. Retaining older generations and orphan final
parts is conservative and preserves forensic/recovery options until an explicit pin-aware garbage
collector exists.

Per-tablet durable positions are required because WAL order is global but flush progress is not.
The global coordinate identifies the removable prefix; the tablet boundaries identify records in
the retained suffix whose effects are already in parts. Retry outcomes make those skips verifiable
and preserve the same-digest/conflict contract after WAL removal. Retaining every protected retry
entry is intentionally bounded by admission rather than silently inventing an expiration policy.

File sync makes content and inode metadata durable under the platform contract; same-directory
no-replace rename installs identity without overwriting an immutable object; directory sync makes
the final name durable. Readback validation catches short/misdirected storage before publication and
also exercises the production decoder at the trust boundary.

## Alternatives considered

- **One mutable manifest file:** fewer files, but overwrite/torn-write recovery needs slots, epochs,
  or a second authority and risks corrupting the last complete state.
- **Append-only version-edit log:** reduces bytes per edit but adds tail-repair, replay, snapshotting,
  and log-compaction rules before measured manifest size justifies them.
- **`CURRENT` pointer plus snapshot files:** common, but adds another ordered file installation and
  corruption/fallback decision. Highest immutable generation is sufficient while generations are
  retained.
- **Choose the highest valid generation:** appears available under corruption but can silently roll
  back a checkpoint whose WAL prefix has been removed.
- **Publish a manifest before synchronizing parts:** exposes references that may survive while their
  data does not, directly violating invariant 2.
- **Use only one global applied sequence:** either blocks independent tablet flush unnecessarily or
  replays already durable rows when tablets progress unevenly.
- **Drop retry state at checkpoint:** can retain rows while forgetting their identity and duplicate
  a logical retry after restart.
- **Automatically delete every orphan final part:** may destroy forensic evidence and precedes a
  general pin/reclamation contract. V1 reports and retains it.
- **Embed RocksDB/SQLite for the manifest:** contradicts ADR 0011 and obscures the exact crash and
  recovery state machine ChronosDB must own.

## Consequences

- Manifest writes are `O(parts + protected retries)` and old generations consume space until a
  later safe manifest-compaction protocol exists.
- Retry retention can backpressure ingestion; no infinite retention or silent eviction is claimed.
- Recovery validates all referenced parts before serving queries and may be expensive without later
  cached evidence.
- WAL recovery gains an external checkpoint context but WAL v1 physical bytes remain unchanged.
- A database root needs a stable nonzero `DatabaseId`, durable `parts/` and `manifest/` directories,
  and one manifest-writer advisory lock.
- Phase 6 cannot claim compaction, part deletion, retry pruning, schema-catalog persistence, remote
  objects, or distributed snapshots.

## Affected invariants

This decision directly enforces invariants [1, 2, 3, 4, 6, 8, 9, 10, 11, 14, 16, and
18](../architecture/invariants.md). The installation order prevents incomplete references; full
generation and retry state make recovery repeatable; per-tablet/global positions preserve replay
order and idempotency; immutable pins preserve old snapshots; and the format/checksums establish
bounded explicit compatibility.

## Validation plan

- Maintain independently reviewed manifest goldens and exact filename fixtures.
- Property-test canonical sorting, checked layouts, monotonic transitions, retry/tablet/part
  relationships, and prefix/suffix recovery against an independent state model.
- Fuzz the manifest decoder with hostile counts, lengths, offsets, descriptors, flags, CRCs,
  truncation, splicing, and unsupported versions.
- Crash a subprocess before and after every part/manifest write, readback, file sync, rename,
  directory sync, in-memory publication, WAL removal, and WAL-directory sync.
- Repeat recovery and cleanup from every captured image and compare selected generation, part
  hashes, tablet/retry state, replayed rows, remaining names, and diagnostics.
- Corrupt, remove, replace, and symlink referenced/unreferenced parts and manifest generations;
  require fail-closed classification before query state is published.
- Hold old snapshots through flush publication under TSan/ASan/UBSan and prove they retain the old
  head/part set while new snapshots see the replacement exactly once.
- Benchmark codec size/throughput, generation install sync amplification, flush throughput,
  foreground interference, recovery time, and temporary/durable space amplification using the
  repository benchmark contract.

## Migration or rollback considerations

No manifest generation exists yet. Before final installation, the implementation can be rolled
back with no durable migration. After generation 1 is installed, major 1/minor 0 bytes, names,
identity meanings, checksum ranges, and selection rules are immutable. A changed format needs a new
reader/converter and an atomic history transition that retains the old recoverable state until the
new one is durable.

Because WAL prefix removal may rely on a manifest checkpoint, rollback to a binary without
manifest-aware WAL recovery is not supported after reclamation begins. Deployment must upgrade the
reader before enabling segment deletion.

## Unresolved questions

- Retry horizon and pruning policy remain deferred; v1 retains all protected entries.
- Manifest-generation compaction and deletion remain deferred until pin and rollback requirements
  have measured evidence.
- Part removal/replacement, delta/base classification, and compaction are Phase 7.
- Durable schema catalog encoding and database creation/user naming are separate catalog contracts;
  recovery receives a retained catalog and binds exact identities.
- Remote installation, encryption, Raft snapshot identity, and distributed checkpoint coordination
  remain later phases.

## References

- [Manifest v1](../formats/manifest-v1.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
- [CSEG v1](../formats/cseg-v1.md)
- [WAL v1](../formats/wal-v1.md)
- [ADR 0005](0005-columnar-heads-and-immutable-cseg-parts.md)
- [ADR 0006](0006-wal-durability-and-group-commit.md)
- [ADR 0011](0011-dependency-and-build-versus-buy-policy.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
