# ADR 0328: Proof-revalidated grouped FLOAT64 worker

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, manifest, and distributed-systems maintainers
- **Extends:** [ADR 0167](0167-proof-revalidated-distributed-aggregate-worker.md),
  [ADR 0327](0327-group-scoped-grouped-float64-dispatch.md)

## Context

Grouped dispatch bytes bind coordinator-side authority, but a worker can receive them after local
placement, leadership, schema, or Manifest publication changes. Grouping physical CSEG rows would
also count superseded versions and tombstones. Empty selected input must close explicitly without
creating a NULL group.

## Decision

The ungrouped worker's route/group/node, placement epoch/membership/leadership, local read barrier,
database/generation, tablet/source/durable position, recovery schema, aggregate type, and part-range
checks are extracted into one internal validator. Both ungrouped and grouped executors call that
same validator only after re-encoding their in-memory dispatch proves structural validity.

`execute_distributed_grouped_float64_fragment` additionally exact-checks the grouped key index and
FLOAT64 type against the same current schema. Only then does it load the generation-pinned validated
temporal CSEGs, resolve current logical winners, remove tombstones, and apply the pushed event-time
predicate. It groups the selected key using the exchange's NULL/signed-zero/NaN canonical tokens and
accumulates the selected FLOAT64 aggregate input with the existing mergeable state.

Nonempty output is a value-owned vector in canonical key-token order. Sequences start at 1 and only
the final grouped partial is terminal. Empty selected input returns the distinct sequence-1 terminal
value, never a NULL-key partial. The existing temporal resolution output-row limit bounds the number
of possible groups and messages. The synchronous loader must invoke its consumer exactly once;
route/proof rejection occurs before loader I/O.

## Consequences and validation

The first grouped worker executes real installed CSEG data without weakening the ungrouped path or
duplicating its authority gates. Canonical token order is deterministic internal output, not SQL
`ORDER BY`. Per-row grouping is `O(log groups)` and state/output memory is `O(groups)` under the
existing resolved-row ceiling.

The focused real-Manifest/real-CSEG worker case still proves the ungrouped filtered aggregate and
now also proves two exact grouped partials, contiguous terminal sequencing, an event-filtered empty
terminal-only result, and stale-local-node rejection before the supplied loader is called. The
installed-consumer gate references the grouped worker entry point.

Authenticated grouped request/response transport, packaged multi-tablet grouped execution,
multi-key/non-FLOAT64 state, ordering/top-N/LIMIT, cancellation interruption, and broad fault/
measurement evidence remain incomplete. No Phase 16 exit gate is claimed.

ADR 0330 subsequently supplies distinct canonical grouped request/response codecs. Authenticated
receiver dispatch and bounded stream ownership subsequently follow in ADRs 0331 and 0332.
Production service adaptation, network lifecycle ownership, and packaged execution remain
incomplete.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Proof-revalidated distributed aggregate
  worker](0167-proof-revalidated-distributed-aggregate-worker.md)
- [Group-scoped grouped FLOAT64 dispatch](0327-group-scoped-grouped-float64-dispatch.md)
- [Distributed Grouped FLOAT64 Aggregate Exchange
  v1](../formats/distributed-grouped-float64-exchange-v1.md)
