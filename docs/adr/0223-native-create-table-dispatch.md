# ADR 0223: Native CREATE TABLE Dispatch

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, SQL, metadata, and native protocol maintainers

## Context

The recoverable database owner could publish a restartable table creation when given a bound DDL
plan and explicit identities, but Protocol v1 requests could execute only SELECT. SQL text must not
manufacture durable identifiers, and the service library must remain testable without relying on a
process-global random source. Protocol v1 has no separate DDL response message, but its described
query-result batches can represent durable completion data.

## Decision

`NativeProtocolService::execute_query` tokenizes the validated SQL and dispatches `SELECT` or
`CREATE TABLE` by its first keyword. Other statement families return an explicit unsupported error.
CREATE parsing and binding use the database owner's current immutable catalog.

The adapter receives an optional `NativeIdentityGenerator`. A CREATE-capable caller must inject one.
The adapter requests one table, schema, tablet, and per-column UUID. As refined by ADR 0563, nil,
same-allocation, and already-owned bootstrap/catalog candidates are retried under a configured
nonzero per-identity attempt limit. These identities and a configured nonzero retry-retention
position count are passed to `SingleNodeDatabase::create_table`, which repeats the authoritative
preflight. The database owner remains the sole authority for collision membership, durable proposal
ordering, incomplete-prefix recovery, and live publication.

Successful DDL returns one Protocol v1 result row followed by `QUERY_END`. The nonnullable columns
are `table_id UUID`, `schema_id UUID`, `tablet_id UUID`, `metadata_index UINT64`, and
`resumed_incomplete_creation BOOL`. All values use canonical network cell bytes. Failures return one
terminal error and no result frame.

## Consequences

Native clients can create a table and immediately query its live empty tablet without restart. The
service does not conceal durable identities or claim completion before the metadata index is
applied. Identity generation policy is separated from SQL and from the reusable service library;
the packaged daemon must provide a cryptographically secure implementation.

The current protocol has no client DDL retry identity. Reissuing matching SQL relies on the database
owner's deterministic incomplete-prefix rules but may append exact duplicate metadata operations
after a fully completed creation. ALTER, DROP, rename, SQL INSERT, authorization, and concurrent DDL
serialization are not added here.

## Validation

Focused tests inject deterministic unique identities, decode all durable result fields, verify a
nonzero applied metadata index and non-resumed completion, then execute a vector count against the
new table. A `SystemUuidGenerator` backed by an injected entropy source now fails on the fifth
candidate: the adapter returns one execution error, the table remains absent, and the next complete
identity set creates it with `resumed_incomplete_creation=false`. Catalog-collision coverage retries
every identity from an existing table before creating another, and exact exhaustion coverage proves
no metadata index advance. Existing direct DDL recovery tests and native ingest/query tests remain
passing.

## References

- [ADR 0219](0219-restartable-single-node-table-creation.md)
- [ADR 0222](0222-bounded-native-vector-query-results.md)
- [ADR 0563](0563-authoritative-bounded-create-identity-allocation.md)
- [SQL v1](../product/sql-v1.md)
