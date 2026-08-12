# ADR 0227: Empty Manifest Namespace Initialization

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB Manifest, runtime, and recovery maintainers

## Context

Database Bootstrap v1 deliberately freezes a 128-byte descriptor and creates only `wal/` and
`raft/`. The implemented Manifest owner requires pre-existing `parts/`, `manifest/`,
`manifest/LOCK`, and at least one final generation. A recoverable aggregate database cannot safely
compose sealed-head flush by creating those names ad hoc or by pairing an existing WAL with a newly
invented storage identity.

## Decision

`ManifestStorage::initialize_empty()` is a distinct initialization-only boundary. Its caller must
hold aggregate database-root ownership and supplies the already durable database identity and the
actual opened WAL identity. It establishes `parts/` and `manifest/`, synchronizes their parent,
creates and synchronizes `manifest/LOCK`, and installs an exact checksummed Manifest v1 generation
1 with no tablets, parts, retries, or covered records. Its WAL checkpoint is exactly record 0,
segment 1, byte offset 64.

The generation-1 temporary nonce is the nonzero WAL identity. Candidate bytes are written, read
back, exact-decoded, compared, synchronized, closed, renamed without replacement, and followed by
a Manifest-directory synchronization. The final generation is the readiness marker; Bootstrap v1
bytes are not rewritten.

Recognized part and generation temporaries left before readiness are removed and their directories
are synchronized before rebuilding. They are never promoted. A pre-existing final must be exactly
the same empty generation 1 bound to the supplied identities. Later generations, final parts,
nonempty descriptors, identity mismatch, malformed entries, or post-directory state missing a
required sibling directory or writer lock fail closed.

## Consequences

Existing Bootstrap v1 roots can gain a crash-safe Manifest namespace without a bootstrap-format
migration. Ordinary `ManifestStorage::open_existing()` remains strictly non-creating. Service
startup must call this initializer only when it has classified an uninitialized storage namespace;
established databases use ordinary highest-generation recovery.

This decision provides the durable prerequisite but does not itself switch the single-node owner to
Manifest recovery, configure flush queues, publish aggregate storage epochs, or reclaim WAL.

## Validation

Focused filesystem tests cover first installation, exact identity-bound reopen, wrong-identity
rejection, cleanup of corrupt recognized generation/part temporaries, and fail-closed rejection of a
final generation missing its previously durable lock. The Manifest target and public header build.

## References

- [ADR 0017](0017-manifest-generations-installation-and-checkpoints.md)
- [ADR 0216](0216-durable-database-root-bootstrap.md)
- [Manifest v1](../formats/manifest-v1.md)
- [Manifest installation and checkpointing](../architecture/manifest-installation-and-checkpointing.md)
