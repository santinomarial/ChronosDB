# ADR 0226: Native SQL INSERT Dispatch

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, query, ingestion, and WAL maintainers

## Context

The configured native service can execute SELECT and CREATE TABLE, while SQL v1 already binds and
materializes constant `INSERT ... VALUES`. Completing the single-node SQL write boundary must not
bypass the canonical append executor or acknowledge data before its named durability frontier.

## Decision

A native `QUERY_REQUEST` beginning with INSERT is parsed, bound against the immutable current query
catalog, evaluated into complete schema-ordinal rows, and converted to one canonical
`OwnedColumnarBatch`. Dispatch currently requires exactly one local tablet for the target table;
multi-tablet event-time and shard-key routing remains explicit future work.

The service obtains distinct nonnil client and batch UUIDs from its injected system identity source,
then calls `execute_columnar_append()` with `LOCAL_SYNC`. Success therefore means the WAL commit
coordinator synchronized the append before response construction and the tablet publication is
already query-visible. The response is one self-describing result row followed by `QUERY_END`, with
`applied_rows`, WAL record sequence/location, and `matching_retry`. All existing parser, binder,
columnar, protocol, and aggregate response-byte bounds apply.

The SQL query envelope does not carry a client-supplied durable idempotency key. Retrying an INSERT
after an ambiguous transport failure can therefore create a new mutation identity and append the
rows again. Clients requiring retry-safe ingestion must use the canonical native ingest request,
which carries explicit client and batch identities. The service never derives durable identity from
a connection-local query request number.

## Consequences

Configured `chronosd` now provides one coherent SQL workflow for CREATE, INSERT VALUES, and supported
vector SELECT over a single local tablet, and inserted rows survive owner restart through WAL replay.
It does not yet provide multi-tablet INSERT routing, server-side INSERT retry tokens, INSERT SELECT,
defaults, conflict handling, authorization, or immutable CSEG/Manifest publication.

## Validation

Focused native-service coverage inserts two rows containing fixed, variable, Boolean, and NULL
values, verifies nonzero LOCAL_SYNC WAL coordinates and immediate SELECT count, shuts the owner down,
reopens the same database root, and verifies the recovered count. The full focused native service
suite passes.

## References

- [ADR 0015](0015-columnar-batch-v1-and-wal-append-command.md)
- [ADR 0220](0220-native-protocol-ingest-service-adapter.md)
- [ADR 0224](0224-configured-single-node-chronosd.md)
- [ADR 0225](0225-sql-insert-columnar-materialization.md)
