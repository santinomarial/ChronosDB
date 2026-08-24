# ADR 0216: Durable Database-Root Bootstrap

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB runtime, WAL, and Raft maintainers

## Context

The packaged daemon had no durable authority for database identity, metadata-group identity, local
node identity, or restart-sensitive memory/segment limits. The WAL and multiplexed Raft writers also
require pre-existing subsystem directories whose names have crossed the database-root directory
synchronization boundary. Inferring identity from directory contents or recreating missing paths on
open would make restart behavior ambiguous and could silently manufacture a new database.

## Decision

Database Bootstrap v1 is one fixed 128-byte, CRC32C-protected root descriptor. It records distinct
database and metadata-group UUIDs, the local node ID, mutable-head and retry limits, and WAL/Raft
segment targets. One root advisory lock excludes cooperating process owners.

First installation uses a synchronized `BOOTSTRAP.tmp` intent before creating and synchronizing the
`wal/` and `raft/` directory names. The intent is then renamed to `BOOTSTRAP` without replacement and
the root is synchronized again. Startup resumes a lone valid intent using its exact values. A final
descriptor is never replaced, and missing required directories fail closed.

## Rationale and alternatives

A durable temporary intent makes interrupted initialization idempotent without a cleanup heuristic
or regenerated identities. Installing the final name last makes it the readiness marker. Storing
these values in the WAL or Raft log would create a cycle because their directories and group
configuration must be known before either log can open. Environment-only configuration was rejected
because restart could silently select different identity or capacity.

## Consequences

The upcoming single-node owner can configure both logs from one recovered authority and retain the
root lock for its complete lifetime. Database-root creation itself remains a deployment boundary;
the API requires an existing dedicated root. Format ceilings and catalog policy still undergo
subsystem validation after bootstrap. Directory removal or repair is not attempted automatically.

## Invariants and validation

This decision supports invariants 1, 8, 10, and 14 through explicit directory durability ordering,
restartable exact intent, integrity coverage, and version rejection. Focused tests cover exact
round-trip, checksum damage, lock exclusion, final reopen, interrupted creation, and unexpected
state rejection. A packaged replicated-database subprocess additionally reaches a synchronous
`kRootOwnerReady` startup observation with the validated root lock live, dies by `SIGKILL`, and
proves that the next owner reacquires the root and recovers the exact committed state. Crash
injection at every bootstrap synchronization boundary and cross-platform persistence qualification
remain deferred. A separate packaged-daemon fault child proves that failure of either proposed
bootstrap UUID occurs before root mutation and that an ordinary retry can initialize the untouched
directory. It also fails the first WAL identity read after the final bootstrap and subsystem
directories are durable. The failed process installs no WAL identity; two ordinary `chronosd`
starts reopen that exact root and reach configured socket service. These cases cover the two
identity handoffs without claiming the remaining synchronization-boundary matrix.

## References

- [Database Bootstrap v1](../formats/database-bootstrap-v1.md)
- [WAL recovery](../architecture/wal-recovery.md)
- [Multiplexed Raft Log v1](../formats/multiplexed-raft-log-v1.md)
- [Architecture invariants](../architecture/invariants.md)
