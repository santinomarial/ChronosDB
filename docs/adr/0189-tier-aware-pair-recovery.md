# ADR 0189: Tier-aware pair recovery for remote-only CSEGs

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB recovery, manifest, tiering, and storage maintainers
- **Extends:** [ADR 0186](0186-durable-tiered-pair-commit.md), [ADR 0187](0187-manifest-bound-tiered-cseg-loading.md)

## Context

Pair recovery previously called ordinary Manifest-v2 recovery before loading cold authority.
Ordinary recovery requires every referenced CSEG final to exist locally, so a correctly reclaimed
local file would make restart impossible even though the committed pair named an exact remote copy.
Loading cold authority first requires authenticated Manifest bytes, but those bytes must not become
publishable until every referenced part has been fully validated.

## Decision

`ManifestStorage::load_temporal_manifest_metadata` reads one exact installed generation and validates
its format, filename/generation, database, catalog, and source bindings without validating referenced
parts. It returns a move-only, explicitly non-publishable metadata owner. Full Manifest recovery now
builds on that preflight and accepts an optional `TemporalMissingPartValidator`; the validator is
called only when the canonical local final is absent from the locked namespace. A present local
file is always read and fully validated, so corruption never falls back to another source.

Tiered pair recovery owns that validator and rejects a caller-supplied one. It follows this order:

1. select and authenticate the highest pair record without fallback;
2. metadata-load the exact named Manifest and verify pair-record length/SHA-256;
3. exact-load and authenticate the named cold generation against those Manifest bytes;
4. fully reload the Manifest, validating present parts locally and absent parts through exact cold
   routes plus the configured `ObjectStore`;
5. recheck the complete Manifest length/SHA-256 and only then create the publication snapshot.

The remote validation helper requires exact route identity/length/SHA-256, authoritative per-key
metadata, a bounded complete read, recomputed SHA-256, and full CSEG/schema/tablet/source validation.
If a local part is absent but the committed pair has no route or no configured object store,
recovery fails closed. The remote store is not consulted while every referenced local final is
present.

## Consequences and validation

Startup may now reconstruct the exact committed pair after safe local reclamation. Recovery pays a
second Manifest read and one full remote read/validation for each absent part; this is deliberate
before publication and does not claim local-disk latency. The metadata-only owner cannot initialize
`TemporalDatabaseStoragePublisher`.

Existing Manifest tests prove unchanged strict local recovery. The tiering integration test commits
a real Manifest/cold pair, removes its local CSEG, verifies recovery fails without the object store,
then recovers through the exact remote object and immediately loads that recovered aggregate
snapshot. Local-corruption and remote-metadata mismatch tests continue to prove no permissive
fallback.

Invariants 2, 3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Publish metadata before part validation:** rejected because query authority would precede proof
  that its durable bytes exist and are valid.
- **Load the highest cold generation:** rejected because only the pair-named generation is committed.
- **Fallback on local validation error:** rejected because it masks corruption and permission faults.
- **Trust the pair record's digest without rereading Manifest bytes:** rejected because the record
  authenticates identity but does not supply the bytes needed to bind cold authority.

## Migration and rollback

Local-only pair recovery remains valid and does not require an object store while all referenced
parts exist. After any local final is removed, rollback to the ordinary standalone Manifest recovery
path is unsafe until that exact file is restored and fully validated.

## References

- [Tiered Pair Commit v1](../formats/tiered-pair-commit-v1.md)
- [Tiered CSEG loading](../learning/tiered-cseg-loading.md)
- [Architecture invariants](../architecture/invariants.md)
