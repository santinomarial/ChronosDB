# ADR 0085: Raft Tablet Application Snapshot v1

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB ingestion, storage, and distributed-systems maintainers
- **Extends:** [ADR 0073](0073-committed-raft-tablet-application.md) and
  [ADR 0078](0078-two-stage-raft-snapshot-installation.md)

## Context

The Raft core can compact an applied prefix and durably install snapshot metadata, but the tablet
application owner can recover only by replaying a complete retained log. Persisted `applied_index`
does not reconstruct mutable rows or retry outcomes. Snapshot metadata alone also cannot prove that
external application bytes belong to the same group, table, tablet, term/index boundary, manifest
generation, part-set checksum, or membership checkpoint.

## Accepted decision

Raft Tablet Application Snapshot v1 is a source-specific, versioned, checksummed container for one
tablet's omitted application prefix. It binds the exact Raft group, table, tablet, complete
`SnapshotMetadata`, and a strictly increasing list of exact `COLUMNAR_APPEND v1` payloads at their
original term/index coordinates. Membership-only indexes may appear as gaps because the Raft
snapshot metadata independently carries the canonical voter checkpoint.

The fixed header, voter list, every entry payload, alignment padding, and complete file receive
explicit validation. Each entry has an independent CRC32C and the whole snapshot has header and
file CRC32C coverage. Counts, total bytes, voter count, entry payloads, and decoded allocations are
caller-bounded. Unknown major/minor layouts are unsupported; damaged canonical bytes are corruption;
configured resource limits return resource exhaustion.

This decision freezes only the application snapshot bytes and in-memory codec. It does not claim
that a snapshot has been durably installed, atomically paired with Raft metadata, or used for
recovery. Those owners must be implemented before log-prefix reclamation is enabled.

## Consequences and alternatives

Retaining exact accepted command bytes preserves the existing row/retry semantics and permits
deterministic rebuilding without inventing a second mutation representation. The v1 snapshot may be
larger than a compact row-oriented image; future measured formats may add a new major version without
reinterpreting v1.

Serializing native structs was rejected because padding, endianness, and ABI are not durable
contracts. Storing only visible rows was rejected because it loses retry identities and exact
application ordering. Treating Raft snapshot metadata as application state was rejected because it
would acknowledge an unrecoverable follower.

## Affected invariants and validation

Invariants 1, 3–8, 10, 11, 14, and 18 apply. Focused tests freeze canonical round-trip behavior,
reject changed complete bytes and non-increasing entry indexes, enforce caller limits, and compile
the public header independently. Golden bytes, hostile field-by-field corruption, fuzzing,
cross-compiler fixtures, durable installation, crash injection, state-machine recovery, and physical
log reclamation remain explicitly deferred.
