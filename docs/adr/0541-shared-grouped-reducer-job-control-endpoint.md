# ADR 0541: Shared grouped reducer-job control endpoint

- **Status:** accepted
- **Date:** 2026-08-26
- **Owners:** ChronosDB cluster transport, query, and service maintainers
- **Extends:** [ADR 0467](0467-authenticated-shared-query-control-endpoint.md),
  [ADR 0499](0499-shared-mutable-grouped-query-control-endpoint.md), and
  [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md)

## Context

The reducer-job session was authenticated and bounded but still assumed its own accepted socket.
Committed node metadata advertises one private query-control endpoint. A second inferred port would
escape route authority, while another listener cannot bind the committed address.

## Decision

Extend the existing authenticated query-control TLS dispatcher with the frozen `CHDVGJC1` magic.
The dispatcher still completes mutual TLS and maps the client certificate before reading the first
application byte. If a reducer-job service is installed, that magic selects the header-first job
reader; otherwise it remains unsupported. Existing `CHDMREQ1` mutable row/grouped dispatch and
`CHRRAUQ1` read-authority dispatch are unchanged.

After exact decode, the shared session passes the already authenticated peer and admission time to
the reducer-job service. It constructs the complete `CHDVGJR1` writer before publishing response
bytes. The bounded TCP listener retains each TLS session and descriptor in destruction-safe order,
reports a distinct completed-job-control metric, and polls the job service with zero additional
wait after processing control readiness. The outer listener wait remains bounded by its caller, so
job progress cannot add a second blocking interval.

Reducer-job dispatch is opt-in at the cluster owner boundary to preserve pre-alpha embedders that
have not yet supplied result-return TLS routes. A daemon claiming distributed grouped-shuffle
support must install the service; silently accepting the magic without it is forbidden.

## Consequences

Mutable rows, mutable grouped sufficient state, Raft read authority, and grouped reducer-job
control can share one committed authenticated endpoint. No port inference or pre-authentication
application parsing is introduced. Connection count, accepts, job count, frames, bytes, memory,
and poll work retain independent bounds.

The generic shared endpoint borrows the job service. Packaged service and `chronosd` ownership,
outbound result TLS route selection, and multi-daemon qualification remain.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): one authenticated connection selects and publishes
  exactly one complete protocol response.
- [Invariant 10](../architecture/invariants.md): reducer-job header and full-frame integrity remain
  required under multiplexing.
- [Invariant 11](../architecture/invariants.md): session, socket, listener, and borrowed job-service
  ownership and teardown remain explicit.
- [Invariant 14](../architecture/invariants.md): only frozen eight-byte magics select the four
  distinct response contracts.
- [Invariant 15](../architecture/invariants.md): connection admission, accepts, protocol buffers,
  job progress, and outer wait time are finite.
- [Invariant 18](../architecture/invariants.md): listener sharing changes no authentication,
  authority, admission, seal, or result-return guarantee.

## Validation plan

Use one real loopback listener and mutual TLS. Through the same bound endpoint, complete one mutable
row query, one mutable grouped query, one Raft read-authority request, and one all-local reducer-job
PREPARE. Require four authentication calls, isolated receivers, one active admitted job, four
distinct completion metrics, and no failed connection. Retain the existing pre-protocol principal
denial gate. Run full cluster/service, allocation-failure, sanitizer, formatting, static-analysis,
and diff gates.

## Migration or rollback considerations

No durable or wire bytes change. Rollback must disable outbound reducer-job control and remove the
borrowed service before restoring the three-protocol dispatcher. Existing protocols remain byte
compatible.

## Unresolved questions

- Package stable reducer-job and result-route ownership in the replicated service.
- Install that packaged owner in `chronosd` and qualify independent daemon processes.

## References

- [Authenticated shared query-control endpoint](0467-authenticated-shared-query-control-endpoint.md)
- [Mutually authenticated job-control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
