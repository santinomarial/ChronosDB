# ADR 0103: Durable subscription plan registry

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB live-query, catalog, and storage maintainers
- **Extends:** [ADR 0097](0097-schema-bound-subscription-plan-identity.md)

## Context

A resume token and coordinator checkpoint bind a plan fingerprint, but a process restart cannot
execute that fingerprint unless it can recover the exact SQL definition and prove that preparing it
under the current catalog still yields the same semantics.

## Accepted decision

Subscription Plan Definition v1 durably binds one database, table, schema/version, plan fingerprint,
and exact `SUBSCRIBE SELECT` bytes in a bounded checksummed format. Definitions are immutable files
named by the lowercase fingerprint in one database-scoped advisory-locked directory.

`SubscriptionPlanStorage::install` prepares the SQL first, derives every stored identity from the
prepared plan, exact-decodes a temporary readback, synchronizes the file, renames without replacing
an existing fingerprint, and synchronizes the directory. Byte-identical retries are idempotent; a
fingerprint collision with different durable bytes is corruption. Directory-sync failure after
rename poisons the owner because crash durability is uncertain.

`load` validates the durable bytes, database owner, and filename fingerprint, then runs the normal
bounded parser/binder/lowering path against a caller-supplied catalog. It returns an executable only
if the recomputed fingerprint and bound table/schema/version exactly match the stored definition.
Missing or evolved catalog state fails closed rather than silently changing a resumed query.

## Consequences and alternatives

Physical operator objects are not serialized. Repreparing keeps durable data portable and subjects
recovery to the same checked planner, while the fingerprint comparison detects semantic identity
drift. Planner changes that intentionally alter accepted identity require explicit compatibility or
migration work; they cannot reinterpret an existing fingerprint silently.

Persisting only a fingerprint was rejected because it is not executable. Persisting an opaque
native physical-plan object was rejected because ownership, ABI, pointer, and version semantics are
not durable. Looking up SQL by an unversioned external name was rejected because deletion or name
reuse could alter resume behavior.

## Affected invariants and validation

Invariants 8, 10, 12, 14, and 17 apply. Focused tests cover exact codec round trip, checksum
corruption, finite SQL limits, exclusive ownership, synchronized immutable install, idempotent retry,
reopen/reprepare, missing-catalog rejection, and corrupt installed bytes. Crash cut points,
allocation sweeps, planner-upgrade compatibility matrices, registry reclamation, and multi-process
catalog races remain Phase 18 work.
