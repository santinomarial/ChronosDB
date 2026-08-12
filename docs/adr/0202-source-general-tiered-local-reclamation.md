# ADR 0202: Source-general tiered local reclamation

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB manifest, recovery, tiering, and query maintainers
- **Extends:** [ADR 0190](0190-reader-pinned-tiered-local-reclamation.md)

## Context

ADR 0190 initially limited local CSEG reclamation to Raft-owned temporal parts because WAL-owned
startup recovery was still local-only. Tier-aware pair recovery now validates an absent local part
through the exact committed cold route using the tablet's declared commit source and source
identity. Keeping the Raft-only authorization guard therefore left WAL-owned cold parts permanently
duplicated locally despite having the same durable pair, reader-pin, remote-validation, and
ManifestStorage deletion proofs.

Removing the guard without replacing it would be too permissive: the candidate descriptor and its
owning tablet must agree about source authority before remote validation or deletion.

## Decision

Tiered local reclamation accepts both `kWal` and `kRaft` temporal parts. Authorization still requires
the exact currently published and durably committed Manifest/cold pair, a strictly sorted current
part set, an exact cold route, and all historical aggregate epochs that lack the route to drain.

For every candidate, the Manifest part's `commit_source` and `source_id` must exactly equal those of
its owning tablet. The existing remote validator then checks the complete CSEG v2 image against
that tablet source, schema, descriptor, length, and SHA-256 before any local mutation. The private
ManifestStorage capability continues to reread the exact selected Manifest, exact-check all local
files before the first unlink, remove the complete set, and synchronize the directory.

After deletion, restart must use tiered pair recovery. That recovery already dispatches validation
from the authoritative Manifest tablet source and never falls back from a present corrupt local
file. Local-only startup paths remain unsupported after reclamation, regardless of commit source.

## Consequences and validation

WAL-owned temporal parts can now complete the same hot-to-cold lifecycle as Raft-owned parts without
changing Manifest, CSEG, cold-manifest, or pair bytes. Operators must enable and retain tiered pair
recovery before invoking reclamation; rollback requires restoring exact local files.

A focused WAL-owned fixture creates matching CSEG/part/tablet source metadata, commits the exact
pair, authorizes and performs full remote-validated deletion, recovers the pair with the local final
absent, republishes it, and reads exact bytes through the remote query loader. The existing
Raft-owned unsafe-reader, corruption, idempotent-retry, and stale-pair tests remain unchanged.

Invariants 1–3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Retain local WAL-owned copies forever:** rejected because tier-aware recovery now provides the
  same exact remote durability proof and this prevents the planned cold transition.
- **Check only the part's source enum:** rejected because a copied source identity could validate
  under the wrong tablet authority.
- **Add a separate WAL reclaimer:** rejected because reader pins, pair authority, remote validation,
  local preflight, unlink, sync, and retry semantics are identical.
- **Permit standalone Manifest recovery after deletion:** rejected because that path intentionally
  requires local referenced finals and cannot authenticate cold authority alone.

## Migration and rollback

There is no durable migration. Existing Raft reclamation proofs remain valid. Once a WAL-owned local
file is removed, rollback to a non-tier-aware binary or startup path requires restoring its exact
canonical CSEG bytes before opening the database.

## References

- [Reader-pinned tiered local reclamation](0190-reader-pinned-tiered-local-reclamation.md)
- [Tier-aware pair recovery](0189-tier-aware-pair-recovery.md)
- [Tiered CSEG loading learning guide](../learning/tiered-cseg-loading.md)
- [Architecture invariants](../architecture/invariants.md)
