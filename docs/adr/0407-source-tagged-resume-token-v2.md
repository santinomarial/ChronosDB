# ADR 0407: Source-tagged Resume Token v2

- **Status:** accepted
- **Date:** 2026-08-15
- **Owners:** ChronosDB live-query, durable-format, and distributed-systems maintainers
- **Extends:** [ADR 0068](0068-live-handoff-and-resume-token-v1.md),
  [ADR 0072](0072-explicit-wal-and-raft-commit-identities.md), and
  [ADR 0095](0095-multi-tablet-subscription-delivery-order.md)

## Context

Resume Token v1 fixes each source coordinate as `(tablet UUID, WAL ID, record sequence)`. The live
subscription model must also preserve the authoritative `(tablet UUID, Raft group UUID, log index)`
coordinate used by replicated tablets. Reinterpreting the equal-width group UUID as a WAL ID would
violate ADR 0072 and make resume, retention, and recovery ambiguous.

Existing v1 tokens may remain with clients across a server upgrade. Token bytes are opaque to Native
Protocol 1.1, so the server can migrate its issuer without changing the enclosing frame.

## Accepted decision

The in-memory `SourcePosition` carries an explicit `WAL` or `RAFT` kind, the corresponding 16-byte
identity, a tablet identity, and a logical sequence. Construction and validation require exactly one
source-specific identity. A source comparison includes tablet, kind, and source identity; equal
bytes in different namespaces never compare as one log.

[Resume Token v2](../formats/resume-token-v2.md) retains the authenticated 128-byte header and
HMAC-SHA256 trailer. Its 48-byte position entry adds a one-byte source kind, seven required-zero
bytes, one 16-byte source-specific identity, and the sequence. Major version 2 makes the incompatible
entry layout explicit. New tokens use v2. The compatibility decoder authenticates before accepting
either major version; v1 remains WAL-only and its encoder rejects Raft positions.

This decision does not reinterpret the WAL-only Protocol 1.1 change envelope or Subscription
Checkpoint v1. Replicated subscription delivery and durable coordinator recovery require separately
versioned protocol and checkpoint migrations before they can claim Raft-source support.

## Consequences and alternatives

WAL clients can resume old v1 tokens after the issuer moves to v2. V2 adds eight bytes per source and
can represent a mixed source vector without aliasing identity namespaces. Unknown source kinds,
newer versions, nonzero required fields, malformed identities, and cross-version decoder calls fail
closed.

Silently extending v1 reserved bytes was rejected because its 40-byte entry has no room for a source
tag. Inferring the kind from placement metadata was rejected because tokens must name a deterministic
self-contained committed boundary. Dropping v1 decoding was rejected because it would invalidate
otherwise safe outstanding client checkpoints during an upgrade.

## Affected invariants and validation

Invariants 4, 8, 12, 14, and 17 apply. Focused tests preserve v1 WAL round trips, prove v1 rejects a
Raft source, round-trip one mixed WAL/Raft v2 vector, inspect the exact tags and identity bytes,
authenticate before semantic decoding, reject cross-version decoding, and prove both versions pass
through the compatibility decoder. Checkpoint v2, Protocol 1.2 Raft change positions, and manager
source generalization are now implemented by follow-up ADRs 0408 and 0409. Raft-prefix reclamation
remains follow-up work.
