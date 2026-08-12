# ADR 0222: Bounded Native Vector Query Results

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, query, tablet, and native protocol maintainers

## Context

The native service could durably ingest but not answer SQL. The vector engine and global
multi-tablet source could execute the supported single-source SELECT subset, while Protocol v1
already defined described row batches and an explicit query end. Connecting them must preserve
query-wide physical semantics, canonical cell bytes, empty-result lifecycle rules, and finite
memory. Returning an unbounded vector of encoded result frames would violate the project's bounded
resource invariant.

## Decision

`NativeProtocolService::execute_query` synchronously performs exact Protocol v1 request decoding,
SQL v1 parse/bind, physical lowering, stable acquisition of every local tablet publication for the
bound table, and one global vector pipeline. Output column names, logical types, and nullability come
from the bound plan. Every physical chunk is encoded row-major through the Protocol v1 result codec;
canonical byte cells are borrowed only during encoding and physical Boolean values are materialized
as their canonical one-byte representation.

The service config requires finite, nonzero caps for query memory, total rows, result batch count,
and aggregate encoded payload bytes. Protocol per-frame/result limits remain independently enforced.
The complete response sequence is retained only within those caps. An execution or limit failure
discards any locally accumulated frames and returns one terminal `ERROR`, so the worker never queues
a partial success followed by an unrelated failure.

A successful query always returns at least one described `QUERY_RESULT` before `QUERY_END`. If the
pipeline produces no chunk, the result has zero rows and the bound descriptors. This satisfies the
connection lifecycle without inventing data.

`SingleNodeDatabase::table_snapshots` acquires stable pins for every local placement in deterministic
metadata order and fails if runtime tablet state is missing. The global physical pipeline remains
above tablet/generation concatenation, preserving aggregate, sort, latest, and limit semantics.

## Consequences

Protocol callers can now execute the implemented single-source vector SELECT subset over recovered
and live mutable tablet state. The current synchronous accumulation favors a small clear correctness
boundary; a future streaming worker may queue each bounded frame directly while retaining the same
row/batch/byte accounting and terminal-error semantics.

Source-free SELECT, ASOF execution, Manifest/CSEG composition, SQL INSERT, DDL dispatch,
authorization, concurrent cancellation, and subscription delivery are not added here. SQL parse,
bind, or unsupported-lowering failures are returned explicitly, never as an empty success.

## Validation

Focused tests cover ingest followed by vector `count(*)` and decoded Protocol v1 output, real column
descriptors and result/end order, total-row overflow collapsing to one `OVERLOADED` error, a
described zero-row result, and all existing database recovery/DDL behavior.

## References

- [ADR 0022](0022-pull-based-vector-operator-memory-contract.md)
- [ADR 0221](0221-global-multi-tablet-vector-source.md)
- [Native Protocol v1](../protocol/native-v1.md)
