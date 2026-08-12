# ADR 0229: Manifest-Aware Single-Node Startup

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB single-node, Manifest, WAL, ingestion, and recovery maintainers

## Context

Retaining an empty Manifest lock did not make durable parts authoritative on restart. The owner
still replayed the complete WAL into mutable heads and invoked an initialization-only API on every
open, which would reject the first advanced generation and could duplicate part-covered rows.

## Decision

The owner now classifies whether a final Manifest exists. A pre-Manifest root opens or recovers its
complete WAL only long enough to obtain the actual WAL identity, installs empty generation 1, closes
both temporary owners, and then enters ordinary Manifest recovery. An established namespace never
uses initialization.

`ManifestStorage::selected_identity()` exact-decodes the selected checksummed generation under
`manifest/LOCK` and returns its generation, database identity, WAL identity, and sorted durable
tablet identities without claiming referenced-part validation or query publication. Startup matches
the database identity to Bootstrap and uses the durable tablet set to supply exactly the catalog
lineage bindings required by full selected-generation validation.

`recover_manifest_columnar_database()` is the sole live-state constructor after initialization. It
validates Manifest/CSEG state, derives durable tablet/retry seeds, replays only the required WAL
suffix, cleans recognized temporaries, and creates aggregate storage publication before releasing
its writer to the live coordinator. Zero-tablet columnar recovery is valid only when the verified
WAL contains no application records; any record still fails as an unconfigured tablet.

Newly created tables publish their empty tablet epoch into the aggregate publisher as well as the
runtime tablet map. Shutdown stops WAL before destroying the aggregate Manifest recovery owner.

## Consequences

The owner can open advanced Manifest generations without replaying part-covered rows into mutable
heads and retains selected CSEG descriptors plus the head suffix in one publication. Legacy roots
undergo a one-time exact initialization after complete WAL validation.

The native SELECT adapter still builds its source from `TabletState` head snapshots, so it does not
yet scan recovered Manifest CSEGs. Flush scheduling is also not yet attached to live tablet rotation.
Those are separate composition steps using the startup authority established here.

## Validation

All single-node and native-service restart cases pass through Manifest recovery, including an empty
zero-tablet database, catalog-plus-WAL replay, SQL INSERT recovery, and table creation. Existing
Manifest startup tests cover advanced generation/CSEG selection and suffix replay. Focused storage
coverage verifies selected identity and durable tablet projection.

## References

- [ADR 0227](0227-empty-manifest-namespace-initialization.md)
- [ADR 0228](0228-single-node-manifest-root-ownership.md)
- [Manifest startup recovery](../learning/manifest-startup-recovery.md)
