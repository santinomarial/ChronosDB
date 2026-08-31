# ADR 0219: Restartable Single-Node Table Creation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB SQL, catalog, metadata Raft, and service maintainers

## Context

SQL v1 could parse, bind, and materialize `CREATE TABLE`, but no service path durably published the
result. The existing accepted metadata formats intentionally separate immutable complete schema
definitions, mutable complete table policy, and tablet placement. A process can therefore stop
between those committed records. Exposing the schema prefix would route ingest without policy or a
tablet; deleting it on restart would contradict committed Raft authority.

## Decision

`SingleNodeDatabase::create_table` accepts DDL already bound against its current immutable query
catalog, explicit proposed table/schema/column/tablet identities, and a nonzero retry-retention
position count. Under ADR 0563, it first rejects a fresh proposed identity set that contains a
duplicate or any UUID already owned by the bootstrap/catalog authority. It materializes and
publishes, in order:

1. one Schema Definition v1 entry;
2. one Metadata Command v1 complete table-policy entry; and
3. one Metadata Command v1 local placement entry.

Every operation uses `ProposeExactRetainedOperation`, then applies the committed suffix and refreshes
the owning metadata projection. The runtime query catalog, lineage, and tablet routing are rebuilt
only after all three authorities exist.

If a matching schema-only or schema+policy prefix already owns the requested SQL name, retry
re-materializes the DDL using the durable table/schema/column identities and requires exact
definition equality. It similarly requires any durable policy or placement to equal the requested
values. A tablet identity proposed after a schema-only/policy prefix may be newly supplied because
no tablet identity was durable yet; once placement exists its tablet identity wins. Divergence
returns `ALREADY_EXISTS` and never overwrites committed state. A new tablet collision is checked
before even the policy proposal, so it cannot add another avoidable prefix.

## Rationale and alternatives

This preserves the accepted independent formats and their ability to evolve policy separately.
Adding a combined fourth format solely for initial creation would duplicate existing meanings and
complicate mixed-version application. Treating the three proposals as invisibly atomic at the
metadata map level was rejected because recovery must acknowledge the actual committed prefix.
Instead, completeness is a runtime publication rule and retries deterministically finish the prefix.

Bound-catalog pointer equality rejects a DDL plan built before a successful catalog mutation. IDs
are supplied outside SQL because SQL text is not allowed to manufacture durable identity; the
native service adapter will use its configured secure identity source.

## Consequences

Fresh databases can now create a query/ingest-routable local table without restart. The path supports
initial schema version 1 and one initial local tablet. It does not implement `ALTER`, rename, drop,
multi-tablet partition creation, authorization, or a client-visible DDL result message yet.

Allocation failure after all metadata commits can leave the current process without runtime tablet
publication; retry against the still-current old query catalog reconstructs it. Restart also
reconstructs it normally from complete metadata. No compensating delete or rollback is emitted.

## Invariants and validation

The design preserves committed-order metadata application, versioned formats, recovery idempotence,
stable catalog snapshots, and no partial durable state exposure. Focused tests cover complete create
and reopen, stale duplicate name rejection, and recovery/resume from a schema-only prefix with
different newly proposed identities. Fault injection after every proposal/apply/publication step,
concurrent DDL, authorization, and multi-process qualification remain deferred.

## References

- [ADR 0214](0214-durable-complete-schema-definitions.md)
- [ADR 0215](0215-complete-table-policy-metadata.md)
- [ADR 0218](0218-recoverable-single-node-database-owner.md)
- [ADR 0563](0563-authoritative-bounded-create-identity-allocation.md)
- [SQL v1](../product/sql-v1.md)
