# ADR 0341: Fail-closed grouped query execution owner

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and distributed-systems maintainers
- **Extends:** [ADR 0171](0171-fail-closed-distributed-query-execution-owner.md),
  [ADR 0339](0339-finite-grouped-query-sender.md),
  [ADR 0340](0340-compatible-grouped-float64-snapshot-binding.md)

## Context

The grouped coordinator, finite per-tablet sender, and compatible grouped snapshot were separately
usable. An embedding still had to keep the Manifest pin alive, construct exactly one sender for
each bound dispatch, distinguish retry backoff from terminal failure, deliver a complete sender
stream once, and withhold the merged result until every tablet closed.

## Decision

`DistributedGroupedQueryExecution` is a move-only, single-owner portable orchestration object. It
accepts only a `CompatibleDistributedGroupedFloat64Snapshot`, never a caller-assembled admission or
dispatch vector. Creation verifies the shared query/database/generation authority, rejects duplicate
tablets, constructs one immutable finite sender per plan-ordered dispatch, and creates the grouped
coordinator from that exact tablet order. It retains the compatible snapshot for its full lifetime.

Tablet lookup uses an owned ordered index. `begin_attempt`, `accept_responses`, and
`record_transport_failure` delegate to one sender. A sender publishes nothing while ready, waiting,
or backing off. After it validates an entire terminally closed response vector, the execution
delivers each ordered payload to the coordinator once. A terminal sender failure is reported once
with its exact status code. Any coordinator admission failure poisons the tablet through
`worker_failed`; no partial result becomes public. `finish` remains the all-tablet coordinator
boundary.

The owner has no socket, thread, timer, or callback. The caller serializes methods, supplies
monotonic time, and drives transport attempts. No durable or network format changes.

## Consequences and validation

Creation is `O(tablets log tablets)` and retains one pinned snapshot, one bounded sender per tablet,
the bounded coordinator, and an ordered index. Event lookup is `O(log tablets)`. The object is
explicitly single-owner and unsynchronized, so no memory-ordering argument is required.

Focused tests prove that a validated grouped partial remains unavailable until another tablet's
terminal-only stream closes, the final result is exact, duplicate and foreign-tablet delivery
reject, retry backoff does not poison the coordinator, and exhausted transport failure
becomes the exact query failure. Header self-containment and installed-consumer compilation cover
the public API.

An end-to-end packaged follower allocation sweep selects every main-thread allocation from grouped
TLS response retention through sender acceptance, coordinator merge, `finish`, and result
publication. Each selected failure is sticky `RESOURCE_EXHAUSTED`, leaves no active attempt or
public result, and restores the exact Manifest pin; the no-fault boundary publishes the exact group.

Multi-tablet TCP scheduling, packaged metadata-backed grouped construction, multi-key/non-FLOAT64
grouping, general vector fragments, and broad fault/measurement evidence remain incomplete. No
Phase 16 exit gate is claimed.

Invariants 4, 5, 6, 10, 11, 14, 15, and 18 apply.

## References

- [Fail-closed distributed query execution owner](0171-fail-closed-distributed-query-execution-owner.md)
- [Finite grouped-query sender](0339-finite-grouped-query-sender.md)
- [Compatible grouped FLOAT64 snapshot binding](0340-compatible-grouped-float64-snapshot-binding.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
