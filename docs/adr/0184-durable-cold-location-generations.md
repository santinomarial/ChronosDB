# ADR 0184: Durable cold-location generation installation and recovery

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB storage, tiering, and recovery maintainers
- **Extends:** [ADR 0017](0017-manifest-generations-installation-and-checkpoints.md), [ADR 0183](0183-separate-cold-location-manifest.md)

## Context

Cold Location Manifest v1 supplies checksummed bytes and exact Manifest v2 binding, but codec-valid
bytes are not durable authority. Tiering needs an installation and restart-selection boundary that
cannot expose a partial generation, overwrite an immutable name, silently fall back after damage,
or pair a cold registry with the wrong logical Manifest generation.

## Decision

One `ColdLocationManifestStorage` owns an existing dedicated directory and its `LOCK` for its full
lifetime. Final names are `generation-<20-digit generation>.clm`; generation one starts the chain
and final names must be consecutive. Recognized `.tmp` files are removed and the directory is
synchronized when ownership is acquired. Unknown entries, non-regular entries, noncanonical names,
and generation gaps fail closed.

Installation exact-decodes and binds the candidate to the caller's pinned Manifest v2 value before
mutation. Generation one or the exact next generation is accepted. Later generations must preserve
the database/store identities, never move the base Manifest generation backward, and retain every
predecessor location exactly; removal and rekeying remain unauthorized. The implementation creates
the exact temporary exclusively, writes and exact-reads it, repeats decode and binding, synchronizes
and closes the file, renames without replacement, and synchronizes the directory. The final
directory sync is the durability boundary. Failure after rename poisons the live owner because the
visible name's crash persistence is uncertain.

Recovery selects only the highest consecutive final generation, exact-decodes that generation, and
binds it to the caller's exact Manifest v2 value. It never falls back to a lower generation after
corruption, an unsupported version, an identity mismatch, or a base-generation mismatch. Retrying
an already durable generation succeeds only when its bytes match exactly.

This boundary does not itself publish a query-visible pair and does not authorize local or remote
deletion. [ADR 0185](0185-atomic-tiered-storage-publication.md) now owns the in-memory aggregate
publication/pin boundary, and [ADR 0186](0186-durable-tiered-pair-commit.md) owns cross-directory
crash commit. Reclamation proofs remain separate.

## Consequences and validation

The design repeats full-generation bytes and filesystem synchronization, favoring inspectable
recovery over write amplification. A dedicated lock prevents two ChronosDB writers but deployment
must still prevent out-of-band mutation. Recovery cost is one directory enumeration plus one read
and decode of the selected generation.

Focused tests cover exclusive ownership, exact names, idempotent retry, two-generation restart,
temporary cleanup, strict add-only transitions, wrong-base rejection, highest-generation damage
without fallback, and owner poisoning on an injected post-rename directory-sync failure. Installed
public-target compilation covers the exported API. A subprocess power-loss matrix and aggregate
Manifest-v2/cold publication crash matrix remain with the publication owner.

Invariants 2, 3, 6, 8, 10, 11, 14, and 18 apply.

## Alternatives considered

- **Mutable `CURRENT`:** rejected because it adds another ordered durable authority.
- **Replace one fixed filename:** rejected because overwrite can destroy the prior recoverable state.
- **Choose the highest valid generation:** rejected because fallback silently rolls back remote
  location authority.
- **Use the object-store listing:** rejected because enumeration is not an atomic metadata commit.
- **Permit removal in ordinary successors:** rejected until reader pins and deletion proofs exist.

## Migration and rollback

An empty directory represents local-only operation. A binary may stop creating new cold generations
while retaining all existing local CSEG files. Once a later phase reclaims local files, rollback must
first restore and validate those bytes; removing the cold registry alone is not safe.

## References

- [Cold Location Manifest v1](../formats/cold-location-manifest-v1.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
- [Architecture invariants](../architecture/invariants.md)
