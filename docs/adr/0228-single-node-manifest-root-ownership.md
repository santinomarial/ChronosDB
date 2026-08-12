# ADR 0228: Single-Node Manifest Root Ownership

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB single-node, Manifest, WAL, and recovery maintainers

## Context

The empty Manifest initializer is useful only when the aggregate database owner invokes it after
the WAL identity is known and retains its lock for the service lifetime. Releasing the database-root
lock while Manifest storage remains live, or dropping Manifest ownership before stopping WAL
admission, would permit contradictory storage owners.

## Decision

`SingleNodeDatabase::open_or_create()` now opens or recovers its WAL writer, converts the durable
Bootstrap database UUID to the Manifest `DatabaseId`, and calls
`ManifestStorage::initialize_empty()` with that database identity and `WalWriter::wal_id()` before
starting the WAL commit coordinator. The resulting `ManifestStorage` owner is retained for the
complete live database lifetime.

Shutdown first stops and drains the WAL coordinator, then releases `manifest/LOCK`, closes metadata
Raft, and finally releases the database-root lock. Construction failures unwind in the inverse
ownership order and expose no service owner. Existing legacy Bootstrap/WAL roots with no storage
namespace receive the explicit identity-bound empty generation; repeated startup verifies rather
than replaces it.

## Consequences

Every configured single-node database now has a checksummed Manifest generation 1 and stable
Manifest writer ownership before it can accept requests. The frozen Bootstrap v1 file remains
unchanged. This step intentionally retains the prior WAL-only logical replay and mutable-head query
path; full selected-Manifest/CSEG recovery, aggregate publication, flush queue composition, and
checkpoint reclamation follow separately.

An established nonempty or advanced Manifest namespace is not yet accepted by this owner because it
still invokes the initialization-only boundary. The next composition must classify that namespace
and use `recover_manifest_columnar_database()` instead of replaying covered WAL rows into heads.

## Validation

The focused database test verifies the new directories/final generation, proves a second Manifest
writer is excluded while the database is live, then proves the lock is available after orderly
shutdown and the database can reopen. All database and native-service cases pass, including WAL row
restart recovery, and `chronosd` links with the new ownership.

## References

- [ADR 0218](0218-recoverable-single-node-database-owner.md)
- [ADR 0227](0227-empty-manifest-namespace-initialization.md)
- [Manifest startup recovery](../learning/manifest-startup-recovery.md)
