# ADR 0086: Durable Raft tablet application-snapshot installation

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB ingestion, storage, and distributed-systems maintainers
- **Extends:** [ADR 0085](0085-raft-tablet-application-snapshot-v1.md)
- **Extended by:** [ADR 0087](0087-raft-tablet-snapshot-recovery-composition.md),
  [ADR 0088](0088-owned-raft-tablet-snapshot-compaction.md), and
  [ADR 0128](0128-tablet-movement-rtas-handoff.md)

## Context

Raft Tablet Application Snapshot v1 defines recoverable bytes, but a codec does not make them
durable. Snapshot installation must prevent concurrent writers, avoid replacing an immutable index,
survive process interruption, reject changed bytes on retry, and make the final directory entry
durable before Raft is allowed to compact or acknowledge installation.

## Accepted decision

`RaftTabletSnapshotStorage` owns one existing directory and one Raft group under a nonblocking
exclusive advisory `LOCK`. Canonical files are named `snapshot-<20-digit-index>.rtas`; a single
recognized `.tmp` suffix represents an interrupted installation.

Installation encodes and validates the requested snapshot, rejects another group, removes only its
recognized prior temporary, creates a new temporary without replacement, writes all bytes, exact
reads and decodes them, synchronizes the file, closes it, atomically renames without replacement,
and synchronizes the directory. Failure of the final directory sync poisons the owner because the
name may be visible without a proven durability boundary. Existing final bytes make an exact retry
idempotent; different bytes at the same included index are corruption.

Opening ownership removes canonical regular temporary files and synchronizes that cleanup. Exact
loads revalidate size, checksum, format, group identity, and filename/index binding. Latest
selection parses every recognized final filename and opens the numerically greatest valid index;
malformed recognized names, non-regular entries, and corrupt bytes fail closed.

This storage owner still does not compact Raft or publish recovered tablet state. The caller must
pair an exact installed application snapshot with matching Raft `SnapshotMetadata` before either
action. The Raft core now rejects local compaction while a remote installation is pending, matching
this owner's same-index immutability rule instead of allowing two authorities to race.

## Consequences and alternatives

The protocol uses the existing descriptor-relative POSIX I/O abstraction and its atomic no-replace
rename. It never treats directory listing as application truth; listing chooses only among files
whose complete bytes and owner binding are revalidated.

Replacing an existing index was rejected because snapshot bytes are immutable durable state.
Trusting a completed write without readback was rejected because later Raft compaction would depend
on those bytes. A random temporary nonce was unnecessary under exclusive directory ownership; the
deterministic temporary makes crash cleanup narrowly recognizable.

## Affected invariants and validation

Invariants 1–5, 8, 10, 11, 14, and 18 apply. Real-filesystem focused tests cover exclusive ownership,
write/reopen/load, exact idempotent retry, highest-index selection, temporary cleanup, and installed
corruption. Syscall fault injection, process-crash points, directory-sync uncertainty, permission
matrices, application recovery composition, snapshot transfer, and physical-log reclamation remain
deferred.
