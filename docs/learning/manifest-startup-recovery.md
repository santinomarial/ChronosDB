# Manifest Columnar Startup Recovery

> **Status: caller-catalog single-kind startup composition implemented.**
> `chronos::manifest::recover_manifest_columnar_database` connects selected Manifest/CSEG recovery,
> durable-prefix columnar WAL replay, temporary cleanup, and aggregate database publication. It does
> not persist a catalog, start services, dispatch future application kinds, or reclaim WAL files.

## Purpose and boundary

Startup must not expose a valid-looking subset assembled from unrelated recovery passes. The public
function therefore returns one move-only `RecoveredManifestColumnarState` only after all durable and
logical validation succeeds. The caller supplies expected database/WAL identities, the exact
Manifest schema bindings, the retained columnar tablet/schema/capacity registry, decode and resource
bounds, WAL tail-repair policy, and both directory configurations.

The returned owner holds:

- `ManifestStorage` and `manifest/LOCK`;
- fresh tablet and global retry state plus the locked reopened `WalWriter`;
- one `DatabaseStoragePublisher` containing the selected parts and uncovered mutable heads; and
- a report with selected counts, checkpoint, orphan count, and exact temporary cleanup work.

Members are declared so ordinary destruction drops the aggregate publication, then the WAL-bearing
state, then Manifest storage. If the writer is released to a live coordinator, that coordinator must
stop before the startup owner is destroyed to preserve WAL-before-Manifest lock release.

## Recovery sequence

The implementation performs one fail-closed sequence:

1. open existing Manifest storage and acquire `manifest/LOCK`;
2. select only the highest consecutive Manifest generation, exact-decode and bind it, and validate
   every referenced CSEG part;
3. reject caller-provided checkpoints or durable seeds;
4. translate the selected global checkpoint and every tablet recovery schema/boundary/retry
   descriptor into the pure ingest recovery configuration;
5. open the WAL while the Manifest lock remains held, verify and preflight the required suffix, and
   restore covered no-ops plus uncovered rows into fresh unpublished state;
6. require every seeded retry original and tablet boundary after the global checkpoint to have been
   observed in that suffix;
7. remove only recognized part/Manifest temporaries and synchronize changed directories; and
8. copy the fresh tablet snapshots into one aggregate Manifest/part/head publication and return the
   complete owner.

Any error destroys the reopened writer and partial in-memory state before releasing Manifest
ownership. Final orphan parts remain untouched and are counted in the report.

## Descriptor mapping and invariants

Manifest tablet identity selects exactly one configured tablet. Its table and recovery schema must
exist in that tablet's retained linear lineage. Each Manifest retry descriptor becomes one immutable
`ColumnarAppendRetryOutcome` shared by the database-wide directory and tablet retry table. No caller
may override this durable truth.

The global checkpoint may trail a tablet durable boundary. Records in that interval still occur in
the required WAL suffix but can only match protected retry outcomes; they add no rows. Records after
the tablet boundary use normal replay and populate mutable heads. `DatabaseStoragePublisher`
separately proves that every head row lies after the selected tablet durable boundary, so a returned
snapshot cannot expose a row both from CSEG and a head.

## Failure behavior and complexity

Manifest/part/WAL corruption and unsupported durable semantics retain their existing classifications.
Missing configured tablets report `NOT_FOUND`; caller-supplied seeds/checkpoints and ambiguous
configuration report `INVALID_ARGUMENT`; allocation and configured bounds report
`RESOURCE_EXHAUSTED`; lock and filesystem failures remain `UNAVAILABLE` or `IO_ERROR`. No fallback to
an older Manifest occurs.

Let `M`, `P`, and `R` be Manifest tablet, part, and retry counts and `B` the required WAL suffix
bytes. Manifest/CSEG validation remains linear in referenced durable bytes, seed construction is
`O(M + R)` apart from bounded tablet lookup, WAL verification/preflight/replay is `O(B)`, and
publication copies one snapshot per configured tablet. Memory is bounded by the caller's tablet,
retry, head, decoder, and Manifest/CSEG limits.

## Verification and deferred work

Real filesystem tests cover an empty generation with complete WAL replay and temporary cleanup, and
a generation-2 image with one installed CSEG part, one protected retry, one covered no-op, and one
uncovered append. Both repeat from unchanged bytes, return the exact next WAL sequence, and compare
durable and mutable row/retry counts. Hostile tests reject nested lock acquisition, caller durable
overrides, and a selected Manifest tablet absent from the recovery registry. Existing storage and
WAL suites cover corruption, missing parts, sync faults, tail repair, and crash transitions below
this composition.

Remaining work includes durable catalog/tablet-map reconstruction, multi-kind application dispatch,
service activation, optional covered-WAL cleanup policy, crash injection inside this composition's
cleanup/recovery ordering, and future query-state reconstruction.
