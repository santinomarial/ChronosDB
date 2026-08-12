# ADR 0237: Single-Node Applied Append Observation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, ingest, WAL, and live-query maintainers

## Context

The native ingest and SQL INSERT paths independently called the low-level append executor with the
database's tablet, retry directory, and WAL coordinator. That left no single production boundary at
which live processing could observe only a newly committed and applied append. Hooking before the
executor could expose failed or merely WAL-appended work; hooking every successful request would
duplicate matching retries.

An observer failure occurs after the storage mutation is committed and applied. Returning that
failure as though the write failed would invite a retry and misrepresent the selected durability
contract.

## Decision

`SingleNodeDatabase::execute_append` is the product append boundary. It validates the local tablet
and table routing before calling the existing executor with the owned retry and WAL authorities.
Both canonical native ingest and SQL INSERT use this method; the raw retry-directory and WAL-owner
accessors are no longer public.

`SingleNodeDatabaseConfig` may borrow one `SingleNodeCommittedAppendObserver` that outlives the
database. After the executor returns `kApplied`, the database invokes it exactly once with the
authoritative tablet/WAL position, immutable input batch, and committed retry outcome. A matching
retry, routing rejection, WAL failure, or apply failure emits no notification. Startup replay also
emits none because no live subscription is admitted before database recovery completes.

The callback is thread-affine, `noexcept`, and returns no status. It cannot retroactively reject an
already committed write. A live implementation must contain evaluation or retention failure by
overflowing/terminating affected subscriptions and recording its own diagnostics; it must not throw
or block ingestion indefinitely.

## Consequences

The service now has one exact post-apply seam for the committed-batch evaluator and future
materialized-view fan-out. The observer is optional, so databases without configured live state
retain the same append behavior and cost apart from one branch and shared-pointer retention during
the call.

This change does not itself register plans, evaluate results, publish into a coordinator, or route
subscription protocol requests. Those owners attach behind the observer in later composition work.

## Validation

Focused service tests perform a LOCAL_SYNC native append, verify one notification carries the same
record sequence, batch, tablet, and committed outcome, retry the exact mutation through ASYNC, and
prove the matching retry returns success without a second notification. SQL INSERT, recovery,
bounded query, service, and daemon builds remain green through the centralized method.

## References

- [ADR 0220](0220-native-protocol-ingest-service-adapter.md)
- [ADR 0236](0236-committed-append-subscription-result-changes.md)
- [Single-node database owner](../learning/single-node-database-owner.md)
