# ADR 0018: Append-Only CSEG Compaction and Manifest Replacement

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB storage, query-snapshot, and recovery maintainers

## Context

Phase 6 durably installs immutable CSEG v1 parts and publishes complete Manifest v1 generations,
but its transition validator intentionally permits additions only. Phase 7 must reduce overlapping
part debt without weakening stable snapshots, recovery, or immutable-file ownership. CSEG v1 stores
only original `APPEND_ROWS` versions (`OPERATION = 1`); correction and tombstone bytes remain a
Phase 13 decision. Manifest v1 can already describe a full replacement part set, but assigns no
durable base/delta role.

A compactor therefore needs a deliberately narrow first authority. It cannot infer equivalence from
row counts or extrema, overwrite an installed identity, make an index part of query truth, discard
history under an unaccepted retention rule, or delete input files while an old publication may
still reference them.

## Decision

Adopt an append-only first compaction slice with these rules:

- Input and output objects remain exact CSEG v1 files. No CSEG v1 byte, operation code, checksum,
  ordering rule, or filename meaning changes.
- A plan selects nonempty installed parts from exactly one table, tablet, schema identity/version,
  and WAL identity. Selection is a policy decision; correctness cannot depend on a base/delta label.
- The reference merge is a deterministic stable merge by the complete frozen CSEG physical tuple.
  It preserves every input row and every stored user/system value exactly. An identical complete
  tuple appearing twice is corruption, not a row that the compactor may silently deduplicate.
- Output receives fresh nonzero `PartId` values and canonical CSEG v1 encoding. Inputs are never
  modified or reused as outputs.
- Before installation, an independent equivalence validator compares the complete logical input
  multiset with decoded output rows, including stored floating-point bits, variable bytes, nulls,
  WAL ID, record sequence, row ordinal, and operation. Counts or hashes alone are insufficient.
- A compaction-only Manifest v1 generation advances exactly once, retains database/WAL/tablet,
  recovery-schema, durable-boundary, checkpoint, and retry state, removes exactly the selected
  inputs, adds exactly the proven outputs, and leaves every unrelated part byte-identical.
- Output files cross the existing part installation boundary before the replacement Manifest is
  installed. The Manifest directory sync is the atomic durable selection point. Publication gives
  new snapshots the outputs while old snapshots retain their prior generation and inputs.
- Final input files are retained after publication until a separate pin-aware reclamation owner can
  prove that no publication, query, backup, subscription, or other retention owner can reference
  them. The first compactor may therefore accumulate safe garbage; it may not guess liveness.
- Sparse indexes and zone maps are optional pruning evidence. Missing, corrupt, or rejected index
  data falls back to scanning authoritative CSEG bytes and cannot alter results.

This slice does not resolve corrections, tombstones, system-history expiry, retry pruning, part-role
flags, multi-schema projection during merge, partition movement, distributed scheduling, or file
reclamation. Each needs its own accepted authority before implementation.

## Detailed rationale

Reusing CSEG v1 and Manifest v1 avoids a format migration before Phase 7 has measured whether a
durable part-role bit or sidecar index is worthwhile. Full-row comparison is intentionally slower
than trusting metadata: it supplies an executable oracle for the first implementation and prevents
a self-consistent encoder bug from authorizing data loss. Fresh identities plus full-generation
publication preserve the same old-or-new crash model already proven for flush.

Treating delta/base status as a rebuildable planning hint keeps recovery independent of tuning.
Overlap, size, age, and event-time extrema can regenerate candidates after restart. Retaining old
files is conservative but correct while the snapshot pin and garbage-collection protocol is still
unimplemented.

## Alternatives considered

- **Change CSEG v1 or assign Manifest v1 flags now:** durable role metadata may improve planning,
  but its necessity and compatibility behavior are unmeasured. Frozen readers reject such flags.
- **Trust counts, extrema, or one digest:** cheaper, but cannot prove multiplicity, cell content,
  system identity, or ordering equivalence independently.
- **Deduplicate equal keys during compaction:** conflates physical duplicates with the later
  correction/version policy and could erase retained history.
- **Delete inputs immediately after publication:** violates stable old snapshots because current
  publication pins do not yet authorize filesystem deletion.
- **Wait for Phase 13 temporal writes:** delays useful append-only overlap reduction and its crash
  protocol even though CSEG v1 already has a complete total row order.
- **Embed a general storage engine:** violates ADR 0011 and hides the core equivalence and recovery
  boundaries this phase exists to establish.

## Consequences

The first merge is CPU- and I/O-heavy and may temporarily use input plus output space indefinitely.
It provides no current-view deduplication benefit and no correction/tombstone reclamation. In
return, it gives Phase 7 a reviewable reference oracle, deterministic output, unchanged durable row
semantics, and atomic replacement without introducing new bytes.

Later optimized mergers must remain differentially equivalent to this reference path. A later
index format, role registry, temporal operation, or garbage collector can be layered behind new
accepted contracts without reinterpreting these outputs.

## Affected invariants

This decision directly enforces invariants [2, 3, 6, 7, 8, 10, 11, 12, 14, and
18](../architecture/invariants.md). Outputs are installed before reference, inputs remain immutable,
publication is old-or-new, full equivalence preserves rows and boundaries, retained input files
protect existing pins, and all reused formats keep their explicit checksums and compatibility.

## Validation plan

- Property-generate overlapping valid CSEG v1 inputs across every logical type, null shape,
  granule boundary, floating bit pattern, and variable-width boundary; compare every decoded cell
  and multiplicity with an independent reference merge.
- Reject duplicate complete tuples, mixed identities/schemas/WALs, missing inputs, corrupted pages,
  resource-limit violations, output identity reuse, and any input/output disagreement.
- Crash after each output write, readback, file sync, rename, directory sync, Manifest write/sync/
  rename/directory sync, and publication boundary; recovery must select the complete old or new set.
- Hold old publications through replacement under ASan/UBSan and TSan and prove their input
  descriptors and files remain usable while new publications expose outputs exactly once.
- Benchmark merge throughput, read/write/space amplification, foreground interference, overlap
  reduction, and temporary debt using declared distributions. Incorrect equivalence invalidates a
  run.

## Migration or rollback considerations

No deployed compaction generation exists. The slice writes only already accepted CSEG v1 and
Manifest v1 bytes, so older decoders can inspect each file, but Phase 6 transition writers must not
attempt to extend a history after a removal generation. Deployment must upgrade recovery and
transition authority before the first compaction Manifest is installed. Rollback after that point
requires a reader that accepts replacement generations; it cannot select an older generation after
newer checkpoint or publication state exists.

## Unresolved questions

- The exact candidate scoring, output size, fan-in, throttling, and scheduling policy require
  benchmark evidence.
- Durable sparse-index bytes and whether any part-role hint merits a future Manifest minor remain
  open.
- Pin-aware final-part and old-Manifest reclamation is the next ownership contract; until then all
  superseded finals remain.
- Correction, tombstone, and system-history retention semantics remain Phase 13 work.

## References

- [ADR 0005](0005-columnar-heads-and-immutable-cseg-parts.md)
- [ADR 0007](0007-event-time-system-time-and-row-versioning.md)
- [ADR 0011](0011-dependency-and-build-versus-buy-policy.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0016](0016-cseg-v1-layout-integrity-and-compression.md)
- [ADR 0017](0017-manifest-generations-installation-and-checkpoints.md)
- [CSEG v1](../formats/cseg-v1.md)
- [Manifest v1](../formats/manifest-v1.md)
- [Data model](../product/data-model.md)

**Retrospective note (2026-08-31):**
[ADR 0566](0566-bounded-single-node-append-only-compaction.md) composes the existing planner and
coordinator into the recoverable single-node product owner and assigns live output/nonces through
the locked Manifest namespace. It changes no CSEG v1 or Manifest v1 bytes.
