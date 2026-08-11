# ADR 0157: Durable Raft-tablet source-retirement installation

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, Raft, and distributed-systems maintainers
- **Extends:** [ADR 0156](0156-authorized-raft-tablet-source-retirement-manifest.md)

## Context

The source-retirement builder proves which Manifest v2 successor is authorized, but composition in
memory is not durable state. The ordinary storage installer intentionally rejects tablet removal.
Allowing callers to bypass that validator with arbitrary candidate bytes would weaken the Manifest
lineage boundary exactly when a source replica is being removed.

## Decision

`ManifestStorage::install_temporal_manifest()` retains its add-only default. A caller requesting
source retirement must also supply the completed movement and committed final-placement authority.
After rereading and exact-decoding the highest durable predecessor, storage independently invokes
`build_raft_tablet_source_retirement_manifest()` and requires its canonical bytes to equal the
candidate byte for byte. The candidate's remaining tablets must have exact schema bindings and every
remaining referenced final CSEG is reread and fully validated before filesystem mutation.

Installation then uses the existing write, exact readback, file sync, no-replace rename, and
directory-sync ordering. The removed source part remains in the parts directory; a durable
successor is not file-deletion or publication authority.

## Consequences and validation

Ordinary callers still cannot remove a tablet. A stale, incomplete, or mismatched movement/placement
proof fails before a temporary Manifest is created. A successful install makes exactly generation
`N+1` durable while leaving source data recoverable until later publication and reader-pinned
reclamation boundaries complete.

Focused storage tests reject the same candidate without retirement authority, reject inconsistent
authority without namespace mutation, install the exact rebuilt successor, and prove the retired
part is retained.

Invariants 1–6, 8, 10, 11, 14, and 18 apply.

## Migration and rollback

No durable format change. Before publication, recovery may continue selecting the older published
generation or reconcile the exact durable successor. After publication, rollback cannot re-expose
the removed source without a new committed placement and Raft membership transition.

## References

- [Manifest v2](../formats/manifest-v2.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
- [Tablet reconfiguration](../learning/tablet-reconfiguration.md)
