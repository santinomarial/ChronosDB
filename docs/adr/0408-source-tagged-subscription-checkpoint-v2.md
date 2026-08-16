# ADR 0408: Source-tagged Subscription Checkpoint v2

- **Status:** accepted
- **Date:** 2026-08-15
- **Owners:** ChronosDB live-query, durable-format, and storage maintainers
- **Extends:** [ADR 0072](0072-explicit-wal-and-raft-commit-identities.md),
  [ADR 0099](0099-multi-tablet-subscription-checkpoint-v1.md),
  [ADR 0100](0100-durable-subscription-checkpoint-generations.md), and
  [ADR 0407](0407-source-tagged-resume-token-v2.md)

## Context

Multi-tablet Subscription Checkpoint v1 fixes every coordinator member and retained change to a
WAL ID. The in-memory coordinator and Resume Token v2 must also preserve Raft group/log-index
coordinates. Reinterpreting a Raft group UUID as a WAL ID would violate ADR 0072, while modifying
v1 entries would invalidate frozen durable bytes. Existing checkpoint directories can contain v1
generations when an upgraded owner reopens them.

## Accepted decision

Coordinator membership, source-state comparison, retained changes, and storage-owner identity use
an explicit `WAL` or `RAFT` source kind and exactly one source-specific identity. Equal UUID bytes
in different namespaces do not match.

[Multi-tablet Subscription Checkpoint v2](../formats/multi-tablet-subscription-checkpoint-v2.md)
uses a new major version and magic. Its source entry adds a one-byte source kind and seven
required-zero bytes before the source-specific identity. Its retained-change envelope uses the same
tagged position. Generation Envelope v2 binds one Checkpoint v2 value with its own new magic,
version, generation, size, and CRC32C.

New generation installation emits v2. Compatibility decoders accept v1 and v2, with v1 decoded as
WAL-only. A directory may therefore contain a contiguous v1 prefix followed by v2 generations.
Retry of an already installed generation remains exact-byte idempotent: the storage owner encodes
the requested logical state under each supported generation format and accepts only an exact match
to the installed bytes. It never rewrites a v1 generation as v2.

This decision does not extend the WAL-only Native Protocol 1.1 change envelope, physical snapshot
adapter, or WAL retention coordinator. Those boundaries continue to reject Raft sources until
their separately versioned integrations are implemented.

## Consequences and alternatives

Mixed WAL/Raft coordinators can checkpoint and recover their exact admission order without
identity aliasing. V2 costs eight additional bytes per source and per retained change. Unknown
versions, source kinds, required flags, identity mismatches, discontinuities, and checksum failures
fail closed before state becomes authoritative.

Reusing v1 reserved bytes was rejected because neither its source nor change entry reserves space
for a source tag. Rewriting old generations during reopen was rejected because installed files are
immutable and crash selection relies on their exact bytes. Treating logically equal v1/v2 bytes as
an idempotent retry was rejected because ADR 0100 requires exact durable-byte identity.

## Affected invariants and validation

Invariants 4, 8, 10, 12, 14, 15, and 17 apply. Tests retain the exact v1 golden, round-trip an exact
mixed WAL/Raft v2 golden, reject v1 Raft input and cross-version decoding, preserve source kinds
through coordinator checkpoint/restore, reject equal-byte cross-namespace aliases, recover a v1
generation, install its v2 successor, and checkpoint/reopen a durable mixed-source coordinator.
Protocol source tags and Raft-prefix reclamation remain follow-up work.
