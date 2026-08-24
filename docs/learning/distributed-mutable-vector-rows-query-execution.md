# Distributed mutable vector row request ownership

## Purpose and public interface

`DistributedMutableVectorRowsQueryTcpExecution` closes the ownership chain between proof-bound
mutable fragments and Native row payloads. `create` consumes fragments and finite execution, TCP,
and finalization limits. `poll_once` advances network work and performs terminal finalization.
`cancel`, `rebind`, `suggested_leader`, state, metrics, failure, and result observation expose the
request lifecycle without exposing mutable internal authority.

The optional `NativeDistributedMutableVectorRowsQueryConfig` constructor on
`NativeProtocolService` composes this owner with a real replicated `QUERY_REQUEST`. The service
retains correlated group authorities and a matching query snapshot, binds and lowers the supported
SQL row subset, asks the snapshot for the all-tablet fragment/route package, and drives the owner to
one terminal Native response sequence.

## Data structures and ownership

The owner stores the source node and portable-execution limits needed for a possible fresh rebind,
one move-only TCP scheduler, finalization limits, terminal state, diagnostic status, and an optional
`DistributedVectorRowsFinalizedResultV2`. The scheduler owns the portable senders, coordinator,
routes, and clients. Its TLS contexts and authentication policy are borrowed. After success,
`take_result` moves the intermediate plan and worker messages into finalization and clears the
scheduler's optional. The composite then owns only the result schema and encoded Native batches.

## Invariants and failure behavior

- Only proof-bound fragments admitted by the portable owner can be scheduled.
- The scheduler exposes no result until every tablet stream succeeds.
- The scheduler result can be transferred once, only from complete state.
- Composite complete state is published only after global validation, ordering, limit, and batch
  encoding finish.
- Cancellation, terminal transport failure, malformed worker output, resource exhaustion, or
  finalization failure leaves the result absent.
- Rebinding is allowed only after a retryable scheduler failure. The scheduler independently
  requires identical logical query, table, schema, plan, result schema, and tablet/group set.
- A finalization failure cannot be rebound because ownership of the intermediate result has already
  been consumed.

Every method is serialized by one caller thread. The class performs no atomic publication and has
no memory-ordering argument. Borrowed security and TLS owners must outlive both execution and any
rebind.

The service-level config is also borrowed. Its source node is the coordinator transport identity.
Fragments whose serving node equals that identity execute through the borrowed local production
worker because the authenticated carrier rejects self-routes; other fragments use the configured
TLS routes. The local worker, TLS-context span, and every referenced client context must remain
stable for the entire service lifetime. The current service bridge is synchronous and uses bounded
polls under one finite deadline. It disables rebinding because safe rebinding requires a fresh
all-group read-authority acquisition and correlation policy, and it cannot observe a later Native
cancellation request while the call owns the service thread.

## Complexity and tradeoffs

Creation inherits `O(tablets log tablets + routes log routes)` validation and indexing. Each poll is
`O(tablets)` plus descriptor readiness. Finalization is linear without order keys and
`O(cells + rows log rows * order keys)` with global ordering. The standalone owner intentionally
consumes the intermediate messages. Native local/remote composition instead revalidates remote
subset messages in one complete coordinator so global ordering and LIMIT run exactly once; this can
transiently retain the bounded remote messages twice. The extra copy favors an explicit atomic
publication boundary over an unproven consuming merge interface.

## Verification and likely interview questions

The integration gate uses a real mutual-TLS carrier and verifies the final schema-bearing Native
payload. Negative coverage proves that finalization failure suppresses output; adjacent scheduler
tests cover split leaders, retries, deadlines, cancellation, and fresh-authority rebinding.
Allocation injection covers construction and the finalizer separately.

The Native integration gate uses a replicated two-tablet snapshot and a real production worker TCP
server over mutual TLS. It submits projected, filtered, globally ordered and limited SQL, then
decodes the returned Native payload and checks its schema, row, and `QUERY_END`. The context is the
actual `ReplicatedIngestDatabase`: it reobserves the named local leader term, pins one committed
metadata/tablet snapshot, and exact-matches the fragment before the worker performs its independent
proof gates. The same query then executes through the local production worker and must produce the
identical result payload; a missing local worker fails before row publication. Daemon composition
remains the next deployment boundary.

Useful interview questions include:

1. Why does the scheduler need a consuming result API instead of returning a const reference?
2. Why is rebinding legal after a retryable transport failure but not after finalization failure?
3. Where are partial results retained, and at what point may they become externally visible?
4. Which objects own sockets, encoded messages, and final Native payloads, and which objects are
   borrowed?
5. Why is the composite constructor not `noexcept`, even though moves of the owner are `noexcept`?
6. Why does the Native bridge disable redirects and fresh-authority rebinding when distributed
   execution is configured?
7. Which cancellation behavior changes when a synchronous request handler owns network polling?
8. Why must local and remote tablet streams enter one global coordinator instead of being finalized
   independently?
