# ADR 0186: Durable Manifest v2/cold pair commit and recovery

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, tiering, query, and recovery maintainers
- **Extends:** [ADR 0017](0017-manifest-generations-installation-and-checkpoints.md), [ADR 0185](0185-atomic-tiered-storage-publication.md)

## Context

Manifest v2 and cold-location generations are immutable and individually durable, but no ordering of
two directory renames makes them one crash-atomic pair. Installing either final first can leave a
restart with mismatched highest generations. Falling back to whichever lower file happens to decode
would silently roll back authority.

## Decision

Adopt [Tiered Pair Commit v1](../formats/tiered-pair-commit-v1.md) as the aggregate crash authority
after tiering is enabled. One checksummed fixed record names the exact database/store, Manifest v2
generation, optional cold generation, byte lengths, and SHA-256 digests. Pair commit generations are
immutable, consecutive, lock-protected files in a dedicated directory.

The coordinator first installs every referenced Manifest/cold final through its existing complete
write, readback, file-sync, no-replace rename, and directory-sync boundary. It then exact-decodes and
hashes both durable owners, writes and exact-reads the next pair temporary, synchronizes and closes
it, renames without replacement, and synchronizes the pair directory. That final directory sync is
the aggregate crash-commit boundary.

No WAL/checkpoint cleanup, local-part reclamation, or other irreversible action may rely on a newly
installed component generation before its pair marker crosses that boundary. Otherwise recovery of
the prior committed pair could find its required local durability source already removed.

Recovery selects only the highest consecutive pair commit and never falls back if it is corrupt or
unsupported. It exact-loads the named historical Manifest/cold generations, repeats their full
catalog/source/part and compatibility validation, and checks lengths and SHA-256. Higher uncommitted
Manifest or cold finals are retained as orphans and are not query authority. The recovered owning
pair can initialize the atomic in-memory tiered publisher.

Locks must be held in Manifest, cold-manifest, then pair-commit order. The deployment must prevent
out-of-band mutation. A pair marker does not itself authorize local or remote deletion.

## Consequences and validation

The extra 256-byte file and directory sync make the commit boundary explicit. Recovery may select an
older exact Manifest/cold final than the highest individually installed names, but this is not an
integrity fallback: it selects the highest aggregate commit and treats later component files as
uncommitted preparation. Standalone Manifest recovery remains unchanged for non-tiered databases.

Focused tests freeze the fixed codec, reject every truncation, damage, unknown version, and unknown
flag, prove idempotent commit, simulate a crash with higher uncommitted component finals, advance to
the new pair only after marker sync, and reject a damaged highest marker while a valid predecessor
exists. A subprocess kill/power-loss matrix remains a release-qualification obligation.

Invariants 2, 3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Install one component first and infer a pair:** rejected because either order has a mismatch
  crash window.
- **Fall back to the highest compatible component pair:** rejected because compatibility search is
  not an explicit durable commit and can roll back acknowledged authority.
- **Copy both full manifests into one file:** rejected because it duplicates potentially large
  snapshots and couples their independent formats.
- **Mutable current pointer:** rejected because overwrite introduces torn-pointer recovery.

## Migration and rollback

Creating pair generation one bootstraps the current validated local/cold state. An empty pair
directory means aggregate tiered commit has not been enabled. Once local bytes are reclaimed, startup
must use pair recovery; standalone selection of independently highest component generations is not a
safe rollback path.

## References

- [Tiered Pair Commit v1](../formats/tiered-pair-commit-v1.md)
- [Cold Location Manifest v1](../formats/cold-location-manifest-v1.md)
- [Architecture invariants](../architecture/invariants.md)
