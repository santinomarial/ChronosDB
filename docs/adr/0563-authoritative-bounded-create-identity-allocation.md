# ADR 0563: Authoritative bounded CREATE identity allocation

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB service, metadata, schema, and common-foundation maintainers

## Context

The native CREATE adapter generated a complete nonnil, internally unique UUID vector before table
creation, but it did not compare those bytes with identities already installed in the database.
The metadata state machine would reject some same-type collisions only after a proposal committed.
In particular, a reused tablet UUID could be discovered after the new schema and policy were
committed, leaving an avoidable incomplete creation prefix. A broken or deterministic identity
source could also repeat an installed UUID indefinitely unless the allocation owner imposed a
finite retry policy.

## Decision

The thread-affine `SingleNodeDatabase` is the collision authority for CREATE identities. Its
`create_identity_in_use()` query compares exact UUID bytes with the database and metadata-group
bootstrap identities and with every table, schema, column, tablet, tablet-group, and table-policy
identity in the current durable metadata projection. The byte namespace is deliberately shared
across those durable identity kinds even though their C++ types remain nominally distinct.

Before the first metadata proposal for a fresh table, `create_table()` requires the proposed table,
schema, tablet, and column UUIDs to be mutually distinct and absent from that authority. During
recovery of an incomplete creation, the committed schema identities remain authoritative; only a
not-yet-committed tablet candidate is collision-checked. If a placement already exists, its tablet
identity wins exactly as before. A collision returns before any new metadata entry is admitted.

The native CREATE adapter requests each required identity through a bounded candidate loop. Nil
candidates, candidates used by the durable authority, and duplicates in the current allocation set
are skipped. Entropy/source errors propagate immediately. Exhausting the configured nonzero
per-identity attempt limit returns `RESOURCE_EXHAUSTED` and emits no metadata proposal. The service
and database share one owner thread, so no other local DDL can mutate the authority between the
membership check and `create_table()`; the database repeats the authoritative preflight at its
mutation boundary to protect direct callers.

## Consequences

CREATE can recover from a finite run of generator collisions without exposing them to clients, and
collision exhaustion is finite and observable. Direct callers cannot bypass collision validation,
and a colliding tablet candidate cannot leave a schema or policy prefix. The scan is linear in the
bounded metadata catalog and runs only on the cold DDL path.

This decision does not claim a database-wide allocator for WAL IDs, CSEG/Manifest installation
identities, retry identities, distributed control-plane IDs, or future dropped-object tombstones.
Those owners still require their own authority and no-reuse policy. DROP remains unavailable, so
catalog identity tombstone retention is not yet a live behavior.

## Validation

Focused owner coverage creates one complete table, proposes its tablet UUID for a differently named
table, and proves the collision returns before the metadata index or any schema/policy/placement
count changes. Native coverage skips a nil candidate plus both bootstrap UUIDs, replays all seven
UUIDs owned by a prior four-column table, and creates with the next available candidates. It then
drives a repeating collision source through an exact attempt limit and proves `OVERLOADED` with
unchanged metadata. Existing entropy-failure and incomplete-prefix recovery tests remain passing.

## References

- [ADR 0014](0014-logical-types-schema-identity-and-evolution.md)
- [ADR 0219](0219-restartable-single-node-table-creation.md)
- [ADR 0223](0223-native-create-table-dispatch.md)
- [Common binary foundations](../learning/common-binary-foundations.md)
- [Recoverable single-node database owner](../learning/single-node-database-owner.md)
