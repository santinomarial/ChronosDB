# ADR 0180: Explicit whole-query authority rebinding

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, cluster, and metadata maintainers
- **Extends:** [ADR 0170](0170-compatible-multi-tablet-manifest-snapshot-binding.md), [ADR 0178](0178-pinned-multi-tablet-tcp-query-scheduling.md)

## Context

An immutable distributed dispatch cannot follow a leader hint or placement change without fresh
metadata, read admission, and snapshot evidence. Simply changing one failed tablet's route would
mix its new authority with partials already accepted from an older execution. Conversely, forcing
every embedding to discard and reconstruct scheduler state offered no structural check that the
replacement still represented the same logical query, did not regress its Manifest generation, and
did not extend an expired query deadline indefinitely.

## Decision

After a retryable terminal `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, or `IO_ERROR`,
`DistributedQueryTcpExecution::rebind` may replace the complete failed execution with a caller-built
`DistributedQueryExecution` and fresh routes. The caller remains responsible for obtaining current
metadata, all tablet admissions, read barriers, placement proofs, one compatible Manifest snapshot,
and TLS route contexts. The scheduler never treats an advisory hint as that authority.

Before consuming the replacement, rebinding exact-compares every plan-ordered dispatch's logical
identity: query, database, table, tablet, Raft group, destination schema, read policy, projection,
aggregate input, and event-time predicate. Tablet count and order must be unchanged, the database
owner must match, and the replacement Manifest generation cannot regress. Authority fields may
change only through the already validated replacement: serving node, placement epoch, applied and
observed positions, linearizable barrier, and snapshot generation.

Rebinding replaces the entire execution and discards every prior coordinator partial, sender,
socket, and pinned snapshot together. It cannot run while active, after success/cancellation, or
after a nonretryable failure. The original absolute execution deadline and configured finite
rebinding budget must match exactly, so a replacement cannot widen either. Cumulative attempt,
transport, retry, and rebinding metrics survive replacement. The scheduler also exposes each
sender's optional advisory leader hint for the serialized caller to include when consulting its
authoritative metadata service.

## Consequences and validation

Compatibility validation is `O(fragments * projection width)` and performs no network or durable
mutation. Constructing the replacement has the scheduler's existing bounded allocation costs. At
most 1,024 explicit rebindings are configurable, and the default permits three. Each accepted
replacement restarts all tablets, so correctness and a coherent declared snapshot take priority
over retaining otherwise usable old partial work.

A real mutual-TLS test first allows one old execution tablet to return aggregate value `100`, then
causes its peer to return terminal `UNAVAILABLE`. A replacement with a different query identity is
rejected while the old failure and zero rebinding count remain intact. A compatible fresh execution
then runs against two new servers and returns exactly `6`, not `106`, while cumulative metrics prove
four completed transport attempts and one whole-query rebinding.

Invariants 5, 6, 11, 15, and 18 apply.

## Alternatives considered

- **Retarget only the failed sender:** rejected because accepted peers could belong to a different
  Manifest/placement authority than the replacement.
- **Trust a leader hint as proof:** rejected because hints are advisory and do not prove current
  term, barrier, committed placement, schema, or durable snapshot availability.
- **Allow a new query ID or tablet set:** rejected because that is a new query, not a retry.
- **Keep successful old partials after rebinding:** rejected because no cross-generation snapshot
  contract makes that merge valid.
- **Refresh automatically through a scheduler-owned metadata client:** deferred until the packaged
  node runtime owns one production metadata service and its synchronization contract.

## Migration and rollback

This changes no durable or wire format. Serialized embeddings may inspect hints, acquire a fully
compatible replacement through existing metadata/read/snapshot boundaries, and call `rebind`.
Removing this API requires destroying the failed scheduler and recreating it externally; equivalent
code must enforce the same logical identity, generation monotonicity, deadline, finite budget, and
whole-query restart rules.

## References

- [Compatible multi-tablet Manifest snapshot binding](0170-compatible-multi-tablet-manifest-snapshot-binding.md)
- [Fail-closed distributed query execution owner](0171-fail-closed-distributed-query-execution-owner.md)
- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
- [Whole-query TCP cancellation and deadline](0179-whole-query-tcp-cancellation-deadline.md)
- [Distributed read admission](../learning/distributed-read-admission.md)
- [Architecture invariants](../architecture/invariants.md)
