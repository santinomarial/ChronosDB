# Distributed mutable vector row request ownership

## Purpose and public interface

`DistributedMutableVectorRowsQueryTcpExecution` closes the ownership chain between proof-bound
mutable fragments and Native row payloads. `create` consumes fragments and finite execution, TCP,
and finalization limits. `poll_once` advances network work and performs terminal finalization.
`cancel`, `rebind`, `suggested_leader`, state, metrics, failure, and result observation expose the
request lifecycle without exposing mutable internal authority.

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

## Complexity and tradeoffs

Creation inherits `O(tablets log tablets + routes log routes)` validation and indexing. Each poll is
`O(tablets)` plus descriptor readiness. Finalization is linear without order keys and
`O(cells + rows log rows * order keys)` with global ordering. The finalizer intentionally consumes
the intermediate messages: this avoids a potentially large encoded-batch copy and makes
exactly-once publication clear, at the cost of making finalization failure terminal.

## Verification and likely interview questions

The integration gate uses a real mutual-TLS carrier and verifies the final schema-bearing Native
payload. Negative coverage proves that finalization failure suppresses output; adjacent scheduler
tests cover split leaders, retries, deadlines, cancellation, and fresh-authority rebinding.
Allocation injection covers construction and the finalizer separately.

Useful interview questions include:

1. Why does the scheduler need a consuming result API instead of returning a const reference?
2. Why is rebinding legal after a retryable transport failure but not after finalization failure?
3. Where are partial results retained, and at what point may they become externally visible?
4. Which objects own sockets, encoded messages, and final Native payloads, and which objects are
   borrowed?
5. Why is the composite constructor not `noexcept`, even though moves of the owner are `noexcept`?
