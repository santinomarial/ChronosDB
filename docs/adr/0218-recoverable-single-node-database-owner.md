# ADR 0218: Recoverable Single-Node Database Owner

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB runtime, metadata, WAL, ingestion, and query maintainers

## Context

The repository had durable metadata Raft, WAL replay, tablet state, and vector SQL components, but
no owner established their startup and shutdown order. The packaged daemon therefore could not use
them without risking an invented catalog, opening WAL before schema authority, or releasing root
ownership while background WAL admission remained active.

## Decision

`SingleNodeDatabase::open_or_create` owns the current single-process WAL-backed composition. It:

1. installs or validates Database Bootstrap v1 and retains the root lock;
2. creates or opens the multiplexed Raft log, elects its configured one-node metadata group, and
   replays committed metadata;
3. projects an owning catalog and admits only tables with an active complete schema, complete
   policy, and at least one placement owned solely by the bootstrap node;
4. reconstructs each retained schema lineage and a query catalog from those complete tables;
5. creates fresh bounded tablet/retry state for a new WAL or semantically preflights and replays an
   existing WAL into fresh state; and
6. transfers the open writer to one bounded `WalCommitCoordinator`.

A metadata prefix left by interrupted table creation remains absent from query and ingest routing.
If an existing WAL contains a command without a complete configured tablet, startup fails as
corruption instead of discarding the record. An empty database can create and reopen an empty WAL
without manufacturing a table.

Shutdown stops and drains WAL admission first, closes metadata Raft second, and releases the root
lock last. It is idempotent. Destruction invokes the same ordering best-effort.

## Rationale and alternatives

Metadata must precede WAL semantic replay because columnar commands borrow no schema authority from
their payload. Retaining one root lock covers both independently locked logs and prevents a second
cooperating database owner. A local side catalog or permissive unknown-table replay was rejected.
Using only the scalar reference engine was also rejected; the owner exposes stable tablet snapshots
and lineages to the vectorized tablet-state pipeline accepted by ADR 0217.

## Consequences and current boundary

The owner exposes the recovered bootstrap/catalog/query catalog, lineages, tablet states, global
retry directory, and WAL coordinator to the service adapter. It supports restartable initial local
table creation plus `ASYNC` and `LOCAL_SYNC` through existing executor semantics. Native-protocol
dispatch, schema evolution/drop, subscriptions, immutable-part flush/Manifest composition, and
multi-node tablet Raft remain the next integration layers and are not represented as successful
behavior here.

Operational head/retry/segment limits come from the durable bootstrap. Table retry capacity is the
smaller of bootstrap memory capacity and committed table policy. Nonlocal or replicated placements
are rejected by this explicitly single-node owner rather than silently downgraded.

## Invariants and validation

This owner strengthens acknowledged durability, committed-order application, stable snapshots,
idempotent recovery, retry identity, reference lifetime, and versioned-format invariants. Focused
tests cover empty create/reopen and a catalog + `LOCAL_SYNC` append + shutdown + semantic WAL replay
+ vector SQL count lifecycle. A packaged-daemon process case corrupts the established Bootstrap v1
checksum, requires startup to return that exact corruption before service admission, and proves the
descriptor is not rewritten. A second packaged case corrupts a covered WAL segment-header byte,
requires the exact CRC32C failure after bootstrap/metadata recovery, and proves the complete segment
is unchanged. A third creates a table and SQL INSERT through the socket, corrupts the complete WAL
record body, requires its full-record CRC32C failure before replay, and proves the segment is not
truncated. A fourth appends the minimal incomplete final tail and proves the owner's default
no-repair configuration exits while preserving it. Broader crash injection, incomplete-DDL
matrices, corruption/fault injection, concurrent workload shutdown, Manifest/CSEG recovery, and
process qualification remain deferred. The packaged matrix also corrupts a checksum-covered
metadata-Raft segment-header byte, requires its exact diagnostic before WAL recovery or service
admission, and proves the complete durable segment is unchanged. A companion case corrupts the
first complete metadata-record payload and proves the owner fails on its checksum before catalog
projection without rewriting the segment.

## References

- [ADR 0216](0216-durable-database-root-bootstrap.md)
- [ADR 0217](0217-vectorized-tablet-state-query-source.md)
- [WAL recovery](../architecture/wal-recovery.md)
- [Durable metadata state](../learning/durable-metadata-state.md)
- [Architecture invariants](../architecture/invariants.md)
