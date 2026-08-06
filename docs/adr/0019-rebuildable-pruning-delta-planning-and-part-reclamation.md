# ADR 0019: Rebuildable Pruning, Delta Planning, and Part Reclamation

- **Status:** accepted
- **Date:** 2026-08-06
- **Owners:** ChronosDB storage, query-scan, compaction, and snapshot-lifetime maintainers

## Context

ADR 0018 accepts append-only CSEG replacement but deliberately leaves three Phase 7 boundaries
open: how late immutable parts influence compaction selection, what pruning evidence a scan may
trust, and when superseded final parts may be unlinked. CSEG v1 already authenticates exact
part/granule event-time extrema and Manifest v1 records exact part extrema, record-sequence ranges,
and immutable identities. Neither format has a base/delta role or sidecar-index reference.

Adding durable role or index bytes before they show measured value would require compatibility,
installation, recovery, checksum, corruption, and reclamation protocols. Conversely, deleting a
compaction input merely because it disappeared from the current Manifest would violate a held
predecessor snapshot. The initial query engine also needs a correctness-first pruning API whose
failure mode is always extra scanning, never missing rows.

## Decision

Complete the initial Phase 7 boundary without changing CSEG v1 or Manifest v1:

- The authenticated CSEG part and granule event-time extrema are the v1 zone map and sparse
  event-time index. Planning normalizes supported event-time predicates to explicit inclusive or
  exclusive lower and upper bounds. A part or granule may be skipped only when its closed stored
  range is mathematically disjoint from the predicate. Unknown, non-event-time, nonconstant, or
  otherwise unrepresentable predicates select every granule.
- Planning consumes metadata only after ordinary CSEG metadata decoding and exact schema/tablet
  binding. Installed and startup-selected parts have already undergone full validation that
  recomputes extrema from event-time pages. A transient cached plan is only a hint: identity,
  generation, or metadata mismatch discards it and replans or scans. No optional index can make a
  row visible, hide a row, or repair corrupt authoritative CSEG bytes.
- `BASE` and `DELTA` are rebuildable in-memory access roles, never durable facts. Within one exact
  tablet/schema/WAL group, descriptors are ordered by `(maximum_record_sequence, PartId)`. The
  event-time frontier is the greatest maximum seen in earlier descriptors. A part whose minimum is
  below that frontier is classified `DELTA`; every other part is `BASE`. Equal sequence boundaries
  are made deterministic by `PartId`. Classification affects scoring and observability only.
- The initial deterministic compaction planner selects nonempty same-tablet, same-schema groups,
  prefers a delta together with event-time-overlapping neighbors, and otherwise selects an
  overlapping range component. Caller limits bound fan-in and estimated input bytes. Candidates
  are returned in strict `PartId` order for the accepted compaction coordinator. No candidate is a
  correctness requirement: scanning all currently published parts remains authoritative.
- Corrections, tombstones, current-version collapse, and history expiry remain prohibited in this
  slice. A late append remains the same CSEG v1 `APPEND_ROWS` version; `DELTA` does not grant row
  deletion or reinterpretation authority.
- Successful compaction publication creates a retirement record containing the exact predecessor
  generation, exact removed final-part identities, and a weak lifetime token for that predecessor
  publication. Every query, backup, subscription, inspection, or other consumer that can retain
  its descriptors or file access must retain the corresponding owning snapshot/token.
- The single-threaded storage owner may reclaim a retirement record only after its weak token has
  expired. It then rescans the locked namespace, rereads and exact-compares the currently selected
  Manifest owner, proves none of the candidate identities is currently referenced, unlinks only
  exact regular final-part names, and synchronizes the parts directory. A live pin returns a
  deterministic pending outcome without mutation. Already-absent unreferenced candidates make
  retry idempotent. Any failure after an unlink and before the directory sync poisons that storage
  owner for restart recovery.
- Reclamation never deletes a recognized temporary, current part, unknown name, or Manifest file.
  Old Manifest generations remain retained because the accepted namespace contract currently
  requires a consecutive history from generation one. A future Manifest-floor protocol requires
  its own ADR and crash design.

## Detailed rationale

The existing extrema are cheap, authenticated, and independently recomputed, so using them first
supplies real pruning without a second durable truth source. Event time is required in every CSEG
and is the first predicate needed by the accepted workloads. The conservative fallback gives the
scalar engine an unpruned oracle and makes false negatives directly testable.

Rebuilding delta roles from immutable descriptors keeps recovery and older binaries independent of
tuning. Record sequence approximates arrival order while PartId makes ties reproducible; a mistaken
classification can only produce a less useful candidate. The full-row equivalence proof and atomic
Manifest transition remain the only authorities for installing a replacement.

A weak predecessor token directly represents invariant 11: reclamation becomes possible only when
the final owner capable of exposing that predecessor disappears. Revalidating the selected
Manifest immediately before unlink prevents a stale retirement record from deleting a part that a
later generation references again, even though current writers prohibit identity reuse.

## Alternatives considered

- **Add a CIDX sidecar now:** could index more columns, but requires a new durable format and
  lifecycle before profiles show which keys justify it. The first scalar scan needs an unpruned
  oracle regardless.
- **Add base/delta flags to Manifest v1:** makes a tuning choice durable and breaks frozen readers;
  recovery does not need the role to find or validate rows.
- **Delete inputs immediately after the release-store:** a concurrent reader may still own the old
  publication and later open an input file.
- **Use only a global active-reader count:** cannot associate safety with the exact retired
  generation and is vulnerable to unrelated readers or missed ownership handoffs.
- **Retain every superseded part forever:** safe but fails the Phase 7 space-reclamation objective
  and prevents meaningful steady-state amplification evidence.
- **Implement correction-aware deltas now:** would assign temporal operation and retention
  semantics reserved for Phase 13.

## Consequences

Event-time-heavy scans can avoid disjoint parts and granules without new durable bytes. Other
predicates initially scan. Delta classification and candidate choice can evolve without migration,
but benchmark comparisons must name the exact policy and limits.

Superseded part space is reclaimed once exact predecessor pins drain; long queries intentionally
delay deletion and must be observable. Old Manifest files still grow slowly. All future retention
owners must participate in the snapshot-token contract or cannot safely access retired storage.

## Affected invariants

This decision enforces invariants [2, 3, 6, 7, 8, 10, 11, 13, and
18](../architecture/invariants.md). Pruning derives only from authenticated, recomputed metadata and
falls back to scanning. Roles never change row meaning. Reclamation waits for exact predecessor
ownership to end, revalidates current durable authority, and synchronizes deletion without
modifying immutable files.

## Validation plan

- Generate every open/closed/unbounded event-time interval across signed 64-bit boundaries and
  compare pruned scans with full scans; corrupt or stale cached hints must fall back safely.
- Generate descriptor arrival orders, overlaps, gaps, equal sequence bounds, and late ranges;
  require deterministic classifications/candidates and full equivalence after every executed plan.
- Pause readers before and after publication, release pins in controlled orders under TSan, and
  prove no candidate unlinks while any predecessor token exists.
- Inject missing files, current-generation references, stale generations, malformed names, unlink
  failures, and directory-sync failures. Repeat reclamation and recovery to convergence.
- Process-kill after each unlink and parts-directory sync; the selected Manifest must always reopen
  and every still-referenced part must remain.
- Benchmark selected/scanned granules and rows, metadata/decoded bytes, candidate fan-in, overlap
  reduction, compaction debt, retained bytes blocked by pins, and reclaim sync amplification under
  declared overlap/lateness distributions.

## Migration or rollback considerations

No durable byte changes are introduced. A binary without this planner scans more and ignores the
same rebuildable roles. Once a superseded unreferenced final has been durably reclaimed, rollback
remains safe because all retained Manifest generations that could be published have no active
owner; operators must not manually select an older historical Manifest as current. The selected
generation and every one of its parts remain sufficient for startup.

## Unresolved questions

- Profiles must determine whether a durable multi-column sparse or scoped secondary index earns
  its lifecycle complexity.
- Candidate scoring, fan-in defaults, output partitioning, and throttling may change behind this
  correctness contract when benchmark evidence exists.
- Old-Manifest reclamation needs a durable retained-floor marker and separate crash protocol.
- Correction/tombstone version resolution and history retention remain Phase 13 decisions.

## References

- [ADR 0007](0007-event-time-system-time-and-row-versioning.md)
- [ADR 0011](0011-dependency-and-build-versus-buy-policy.md)
- [ADR 0012](0012-correctness-testing-and-performance-evidence.md)
- [ADR 0016](0016-cseg-v1-layout-integrity-and-compression.md)
- [ADR 0017](0017-manifest-generations-installation-and-checkpoints.md)
- [ADR 0018](0018-append-only-cseg-compaction-and-manifest-replacement.md)
- [CSEG v1](../formats/cseg-v1.md)
- [Manifest v1](../formats/manifest-v1.md)
- [SQL v1](../product/sql-v1.md)
