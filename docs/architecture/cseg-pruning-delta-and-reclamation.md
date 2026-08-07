# CSEG Pruning, Delta Planning, and Reclamation

This is the implementation contract for the remaining append-only Phase 7 work accepted by
[ADR 0019](../adr/0019-rebuildable-pruning-delta-planning-and-part-reclamation.md). CSEG v1 and
Manifest v1 bytes remain unchanged. Corrections, tombstones, history expiry, old-Manifest deletion,
and durable secondary-index sidecars are outside this boundary.

## Pruning authority

One selected CSEG's authenticated metadata exposes a closed event-time range for the part and for
each granule. The planner accepts a normalized predicate with independently optional lower and
upper bounds and explicit inclusivity. It selects a range unless disjointness is provable:

```text
skip when maximum < lower
skip when maximum == lower and the lower bound is exclusive
skip when minimum > upper
skip when minimum == upper and the upper bound is exclusive
```

An empty normalized predicate selects nothing. An absent predicate selects everything. Arithmetic
is not used to convert open bounds, so `INT64_MIN` and `INT64_MAX` cannot overflow. Part pruning is
applied before granule pruning. Row/page counts in the returned immutable plan are checked and
bounded. Execution still validates each selected page through the projected CSEG reader.

The stored extrema are part of authoritative CSEG metadata, not a detachable truth substitute.
Full installation/startup validation recomputes them from the event-time pages. Any optional cache
must key the exact Manifest generation, PartId, file length, and metadata CRC; a mismatch discards
the cache and replans. If a future optional sidecar is missing or rejected, the executor scans the
authoritative CSEG.

## Rebuildable part roles and selection

For each exact tablet/schema group, descriptors are sorted by maximum record sequence and then
PartId. The first part is base. Thereafter, a minimum event time below the greatest earlier maximum
is a delta hint; the frontier then advances to the greater maximum. This detects immutable late
append ranges without claiming that all rows in a delta are late.

The deterministic selector prefers the earliest delta and includes range-overlapping neighbors,
then considers ordinary overlap components. It obeys explicit minimum/maximum fan-in and checked
input-byte limits, never crosses tablet/schema/WAL identity, and emits strict PartId order. If no
candidate satisfies limits, it returns no plan. The executor remains the accepted full-row
reference merger and coordinator, so role or scoring defects cannot change query results.

## Pin-aware final-part reclamation

Every selected part has one private lifetime pin shared through every aggregate publication epoch
that retains it. Compaction publication records the removed input identities and weak references to
their pins. This remains correct across tablet-only refreshes: an older publication object and the
immediate predecessor need not own each other, but both own the same selected-part pin. Any
component that retains descriptors or intends to open their files must retain the owning snapshot,
an explicit token copied from it, or a snapshot-bound loaded image.

The storage owner processes retirement records serially:

1. any live removed-part weak pin means `pending` and performs no I/O;
2. rescan the locked namespace and require the supplied current selected generation to remain the
   highest final generation;
3. reread the exact current Manifest bytes and reject a stale/mismatched owner;
4. prove every candidate is absent from the current part descriptors;
5. unlink only candidate names that are exact regular final-part entries; and
6. synchronize the parts directory once if any unlink occurred.

Missing, already-unreferenced candidates are counted as already reclaimed, making retries
idempotent. A failure after the first unlink poisons the storage owner until restart because the
directory may contain an unknown durable subset. Recovery accepts every such subset: no candidate
is referenced by the selected generation. Recognized temporaries retain their existing cleanup
path and old Manifest generations are not deleted.

## Evidence and observability

Property tests compare every pruned result to an unpruned scan across interval boundaries and
generated layouts. Planner tests cover deterministic overlap/lateness shapes and resource limits.
Controlled snapshot tests and process crashes cover every unlink/sync boundary. Metrics report
parts/granules/rows selected and skipped, base/delta counts, candidates and bytes planned,
retirements pending, files/bytes reclaimed, syncs, failures, and poisoned state.

Benchmarks must publish distributions and correctness gates, not product claims. Required inputs
include part/granule counts, overlap, lateness, predicate selectivity, fan-in, pin hold time, and
compression. Required outputs include pruning effectiveness, decoded bytes, compaction debt,
temporary/durable amplification, and reclaim delay.

The implemented first slice is `plan_cseg_v1_event_time_pruning()`. It returns an owned immutable
list of selected granule ordinals plus checked selected/skipped row counters, rejects a configured
granule limit before allocation, treats reversed or open-equal query bounds as an empty predicate,
and performs no endpoint arithmetic. Deterministic property tests compare it with an independent
integer oracle and require the plan to contain every possible oracle match; conservative false
positives remain legal. The CSEG benchmark measures 64, 4,096, and 65,536 authenticated granule
entries with declared selectivity and excludes metadata decoding from the timed loop.

The rebuildable classifier and deterministic bounded selector are also implemented. Their
benchmark uses one tablet/schema with every eighth arrival shifted four event ranges late and
records selected fan-in at 128, 4,096, and 65,536 input descriptors. It times validation,
classification, grouping, overlap selection, checked accounting, and owned-plan construction; it
does not execute or claim compaction throughput.

Compaction publication now issues move-only retirement records containing the exact predecessor
generation and removed part identities and lengths. A snapshot can produce a copyable explicit
retention token for consumers that need file lifetime without the rest of the query view. The
publisher queues records until the single writer drains them. `ManifestStorage` returns `pending`
without I/O while the predecessor is pinned; after pins expire it rescans, exact-compares the
selected Manifest bytes, proves every candidate unreferenced, unlinks exact final names, and syncs
the parts directory once. Repeating an already-completed record is successful and counts absent
parts. Unlink-prefix or directory-sync uncertainty poisons the owner for restart recovery.
The publication benchmark separately measures a pinned one-candidate check, which must perform no
filesystem I/O, and repeated idempotent verification after deletion, which includes namespace
classification and exact current-Manifest reread. Fixture construction, compaction, and the first
unlink are outside both timed loops; these are local mechanism measurements, not product latency
claims.
