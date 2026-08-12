# Committed Append Subscription Evaluation

## Purpose and public boundary

`evaluate_committed_batch` turns one already-applied Columnar Append into one immutable logical
subscription change. It takes a prepared plan, authoritative tablet/WAL position, immutable batch,
and query resources. It does not publish or mutate coordinator state, so a caller cannot expose a
partial result when evaluation fails.

## Why one change represents a whole append

Resume state advances by WAL record sequence. A batch is one record even when it contains many
rows, so emitting multiple coordinator changes would duplicate the position. The evaluator instead
combines the outputs of all physical chunks into one row-major, self-describing `QUERY_RESULT`
payload. An empty filtered result is still an UPSERT with zero rows; it records that this plan
observed the committed source position.

The fixed result key contains a versioned domain, the plan fingerprint, tablet, WAL, and record
sequence. Re-evaluating the same plan and source position yields identical key and payload bytes.

## Supported semantics

Filters, projections, computed expressions, and typed constants depend only on rows in the current
append and retain source order, so their vector results can be combined safely. Aggregate, grouped
aggregate, sort, LATEST, and LIMIT stages are rejected. Treating any of those as independently
batch-local would silently disagree with the historical snapshot and resume semantics.

This is an append-result contract. It does not yet derive stable application row keys, DELETEs, or
correction retractions. Windowed and aggregate live results use their accepted retained-state
machinery when composed later.

## Ownership, accounting, and failure behavior

The columnar source and every emitted chunk use ordinary query memory reservations. The evaluator
also charges the bounded chunk-owner array and, after exact output sizing, the row-major cell views,
descriptors, encoded payload, key, and conservative allocation overhead. Chunks stay alive while
cell views are encoded, so no payload view dangles. Returned bytes become the bounded live
coordinator's ownership domain after the query reservations are released.

All size arithmetic is checked. Schema identity and output shapes must match the prepared plan.
The result must fit both the query-result payload limit and the smaller capacity left after the
subscription-change envelope and 80-byte key. Cancellation, resource exhaustion, unsupported
stateful semantics, encoding failure, or allocation failure returns no change and performs no
publication.

## Applied-append fan-out

`SingleNodeLiveAppendFanout` is the production service boundary between the database observer and
durable plan coordinators. Its fixed, bounded binding vector borrows immutable plans, durable
coordinators, and query resource contexts; every owner must outlive the fan-out. Admission validates
the complete plan/coordinator identity and rejects stateful physical stages before writes begin.

The database owner calls the fan-out synchronously after an append is applied. Matching table and
tablet/WAL bindings are evaluated in configured order. Success publishes the exact result. A schema
identity change drives the coordinator's terminal schema transition. Evaluation or publication
failure instead advances the exact source through `mark_continuity_lost`, overflowing old sessions
and tokens so no client can resume across the missing result. Failure of that containment disables
the binding. None of these outcomes can reject or relabel the committed write.

The fan-out and its counters are single-thread-affine. Routing is linear in configured plans and
has an explicit maximum; indexing is not justified without a profile. Startup recovery does not
replay observer callbacks, so durable coordinator recovery and committed suffix replay must be
completed by the surrounding runtime before online admission.

## Complexity and review questions

Vector execution and encoding are linear in input cells plus selected output bytes. Peak memory is
the retained input, all output chunks for one append, the cell-view array, and one encoded payload.
Retaining chunks is deliberate because the existing encoder consumes borrowed cell views.

- Why can there be only one coordinator change per append record?
- Why does a zero-row result still advance the source position?
- Which physical stages are unsafe to evaluate independently per append?
- Why must nested result size account for the outer subscription envelope?
- At what exact point may the caller publish the returned change?
- Why does fan-out failure overflow replay state instead of failing the append?
