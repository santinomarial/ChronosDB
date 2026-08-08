# Complete Append-Only Snapshot Tablet Scan

## Purpose and boundary

`create_snapshot_tablet_scan` is the first source that emits the complete current-visible row
multiset for one tablet from one `DatabaseStorageSnapshot`. It combines selected durable CSEG parts,
all published sealed heads, and the active head without mixing database epochs.

The exact claim is intentionally tied to today's accepted append-only operation set. CSEG v1 and
mutable heads accept append rows only. Corrections and deletes have no accepted encoding or winner
rule yet, so this source rejects any future unsupported operation through the underlying validators
rather than guessing at visibility semantics.

## Interface and stage order

The public factory receives the exact aggregate snapshot, its `SnapshotCsegPartScanPlan`, already
validated selected images, a schema lineage and destination projection, and
`SnapshotTabletScanLimits`. Construction performs:

1. snapshot/plan/tablet provenance validation;
2. uniform CSEG/head row-version shape validation;
3. finite head-count and retained-configuration admission;
4. one complete durable child using the existing prune-then-exact factory;
5. one exact or ordinary child per sealed head, then the active head; and
6. query-accounted sequential composition.

The physical sequence is useful for reproducibility but is not SQL order. An explicit physical sort
implements `ORDER BY`; unordered SQL observes only the multiset.

## Why concatenation is exact today

The aggregate publisher verifies that every live head row lies after the selected Manifest durable
record-sequence boundary. When a sealed head is flushed, one release-published epoch removes that
exact head and selects its durable replacement part. Old snapshots retain the old head; new
snapshots retain the part. A query therefore cannot observe both or neither across the boundary.

Since every current operation is append-only, no two published rows are competing logical versions
that require a winner. The row-version suffix remains available for deterministic downstream
ordering and future merge work, but it is not used to discard rows in this source.

## Ownership, failure, and complexity

The sequential parent reserves its container and allocation allowance before constructing children.
One last-owner reservation covers the exact aggregate publication across the parent, every child,
and borrowed CSEG output. Each child independently reserves image/source and output bytes. Completed
children are destroyed immediately; final end swaps out the container and releases parent credit.
Returned chunks own their exact local/shared credit and backing and may outlive the source.

Construction is `O(P + H)` for selected parts `P` and published heads `H`, in addition to storage
validation. Pull cost is the current child's page decode or head materialization. An error requests
shared cancellation and RAII releases every constructed child, pin, and reservation.

## Evidence and next steps

Deterministic tests compare all durable and live values with an independently assembled multiset,
verify record-sequence suffixes, select a head-only timestamp, and prove helper removal. Hostile
tests cover mismatched shapes and finite limits; allocation injection covers every new retained
allocation. `chronos_cseg_scan_fuzz` also varies aggregate limits, suffix agreement, exact bounds,
cancellation, and pull boundaries over an authenticated head-only snapshot.

`scan_complete_head_only_snapshot` measures aggregate serial execution for 64, 1,024, and 65,536
active-head rows with source construction excluded from timing.

Bound-SQL source selection is now connected through
[snapshot physical pipeline instantiation](snapshot-physical-pipeline.md). Shared publication credit
is now implemented. Asynchronous/mapped part providers, parallel morsel scheduling, spill selection,
and future correction/delete resolution remain separate work.

## Review questions

**Why not sort parts and heads here?** Storage traversal order is not SQL order. Sorting belongs to
the physical sort and requires explicit keys and memory policy.

**Why is there no duplicate suppression?** The only accepted operation is append. The publisher
proves a disjoint durable/head commit cut and exact flush substitution. Future competing versions
need a different accepted contract.

**Why require matching suffix modes?** Every sequential child must have one checked physical shape;
silently widening only some children would corrupt downstream ordinal meaning.
