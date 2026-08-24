# ADR 0447: Fresh all-group Native query rebinding

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, query, cluster, Raft, and Native protocol maintainers
- **Extends:** [ADR 0436](0436-compatible-mutable-query-authority-rebinding.md),
  [ADR 0446](0446-reactor-visible-native-query-cancellation.md)

## Context

The mutable TCP scheduler could replace failed remote authority with a compatible fragment set, but
Native local/remote composition disabled that facility. A fresh leader can change a tablet from
local to remote or remote to local, so rebinding only the old remote subset is insufficient. It
could also combine local results proved under the old all-group barrier with remote results from a
new authority acquisition.

## Decision

The Native distributed row path owns a finite all-group authority-rebinding loop. Its initial
prepared fragment set is reduced to one exact logical identity containing the query, database,
table, destination schema, read policy, projection, event-time predicate, plan, result schema, and
canonical tablet/Raft-group vector. Serving node, applied and observed positions, placement epoch,
and linearizable barrier are deliberately excluded.

On a retryable `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, or `IO_ERROR` from a local worker or remote
scheduler, the service:

1. destroys the attempt's coordinator, remote scheduler/clients, and every local or remote result;
2. reacquires correlated current-term authority for all query groups through the replicated read
   barrier;
3. pins a new publication set covering those exact barriers;
4. prepares a complete new fragment and committed-route package from the original query identity
   and lowered SQL plan;
5. rejects any change to the complete logical identity; and
6. repartitions the replacement into current local and remote fragments and starts one whole new
   attempt.

`distributed_mutable_vector_query_logical_identity` provides the shared structural and logical
validation without constructing network senders, so it accepts self-led fragments. The ordinary
portable execution constructor reuses the same validator.

`maximum_authority_rebindings` defaults to three and is capped at 1024. Every failed acquisition
consumes budget. The original absolute execution deadline, query identity, cancellation token, SQL
plan, finalization bounds, authentication policy, and TLS contexts do not reset. Sender retries
within one immutable authority remain independently bounded; the Native layer constructs each
replacement scheduler with internal rebinding disabled. A successful all-group reacquisition may
change the local/remote partition.

Cancellation and deadline are checked before acquisition, after replacement preparation, and
before fragment execution. The read-barrier call remains synchronously bounded by its own request
timeout and cannot be interrupted mid-call; no replacement work starts after the query deadline is
observed.

## Consequences

A leadership or publication race no longer necessarily fails a Native query whose logical table
plan is unchanged. Partial rows from the failed authority are never mixed with its replacement.
Tablet movement that changes tablet/group identity, schema or plan drift, exhausted budget,
nonretryable failures, cancellation, or deadline expiry remains terminal and publishes no rows.

Each rebind repeats bounded barrier, snapshot, preparation, local execution, and remote connection
cost. The correctness-first whole-attempt restart may duplicate completed work. Metrics from
discarded subset schedulers are not yet aggregated into a public service metric. No durable or
network format changes.

## Affected invariants

- [Invariant 4](../architecture/invariants.md): every replacement position comes from a freshly
  pinned barrier-covered snapshot.
- [Invariant 5](../architecture/invariants.md): old request bytes are never retargeted to a new
  leader.
- [Invariant 6](../architecture/invariants.md): the complete tablet/group logical identity is exact
  across authority replacement.
- [Invariant 15](../architecture/invariants.md): rebind count and the original deadline are finite
  and nonresetting.
- [Invariant 18](../architecture/invariants.md): failed-attempt local and remote results are both
  discarded before replacement.

## Validation

Logical-identity tests prove that serving node, positions, epoch, and barrier may advance while a
tablet/group or plan change is detectable, including a self-led fragment. The replicated two-tablet
integration injects one local `UNAVAILABLE`, advances that tablet through a higher-term election,
reacquires all-group authority, executes both replacement fragments through the production worker,
and requires byte-identical finalized Native output. Existing scheduler tests retain real mutual-
TLS remote rebinding and deadline coverage. The normal and ASan/UBSan gates pass all 191 cluster
and 106 service tests plus 25 cluster and 3 service allocation-failure tests. The installed public-
target consumer and `chronosd` CLI gates pass. The LLVM 18 static-analysis build passes with the
repository's existing missing-field-initializer warnings; formatting is clean.

## Migration and rollback

The Native distributed query config gains a bounded rebind count with a default of three. Setting
it to zero restores one-shot authority behavior. Rollback removes the service loop and shared
logical-identity extractor without changing stored data or wire bytes.

## References

- [Compatible mutable query authority rebinding](0436-compatible-mutable-query-authority-rebinding.md)
- [Proof-revalidated local and remote Native row merge](0444-proof-revalidated-local-and-remote-native-row-merge.md)
- [Reactor-visible Native query cancellation](0446-reactor-visible-native-query-cancellation.md)
