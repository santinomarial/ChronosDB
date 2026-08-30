# ADR 0553: Established Job Control black-hole qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, networking, reducer service, and test maintainers
- **Extends:** [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md),
  [ADR 0551](0551-authenticated-partial-job-control-frame-qualification.md), and
  [ADR 0552](0552-linux-netfilter-directional-job-control-partition-qualification.md)

## Context

The existing netfilter campaign inserted a directional rule only after a completed lease renewal.
It proved that a later finite Job Control acquisition failed and that reducer leases bounded the
abandoned query, but it did not isolate failure of the TCP/mTLS connection that had already
authenticated. The partial-frame campaign used an application-controlled peer rather than an
OS-owned packet black hole. In particular, there was no real-TCP evidence for either of these
ambiguous outcomes:

1. the client is authenticated but its request never reaches reducer dispatch; or
2. PREPARE is admitted exactly once, but its success response never reaches the client.

The second outcome must not cause an identical retry to allocate a second reducer job.

## Decision

Extend the opt-in Linux netfilter process-test target with a fixture that composes production
nonblocking `TcpSocket`, mutual-TLS Job Control client/server sessions, and the production reducer
job service. The fixture owns the TCP descriptors longer than the borrowing TLS sessions and uses
one retained loopback listener and reducer service across fault removal and retry. The same
explicit opt-in, root, executable, namespace, exact-port rule, RAII deletion, and successful-path
deletion requirements from ADR 0552 apply.

The request-direction case completes mutual TLS, stops with the client in `WritingRequest` and the
server in `ReadingRequest`, and then installs the exact listener `--dport` DROP rule. Client kernel
write acceptance is not treated as reducer acceptance. Both production session deadlines must
fail unavailable, reducer PREPARE metrics must remain zero, and removing the rule must allow a
fresh PREPARE and authenticated CANCEL through the same listener.

The response-direction case completes mutual TLS and request decode, then stops only after the
service has admitted PREPARE and the server enters `WritingResponse`. It installs the exact
listener `--sport` DROP rule before the response write. The server may finish its local write, but
the client must publish no result and must fail at its exchange deadline. After rule removal, an
exact PREPARE retry through the same listener and service must return success while incrementing
`duplicate_prepares`, retaining one active job, and accepting authenticated cancellation. A
cancelled job remains retained for deterministic retries until its original execution deadline;
advancing the service clock past that bound must reclaim it.

## Detailed rationale

Freezing at public TLS-session states avoids depending on OpenSSL record segmentation or kernel
buffer occupancy. The server invokes the service before it enters `WritingResponse`, which gives a
deterministic admitted-but-unacknowledged boundary. Exact service metrics distinguish a lost
request from a lost response and prove the idempotent retry path. Reusing both listener and service
is stronger healing evidence than creating a replacement endpoint.

This is deliberately a whole-application-frame black-hole gate. It does not claim controlled
interruption within a TLS record or application frame, nor does it inject TCP RST.

## Alternatives considered

- **Create a fresh reducer service after healing:** this cannot prove that the admitted job identity
  was reused idempotently.
- **Treat a completed server write as client acknowledgment:** TCP and TLS write completion does not
  prove application delivery, so the client response parser remains the success boundary.
- **Split TLS records with an OpenSSL implementation hook:** that would qualify a different,
  lower-level fault boundary and is deferred to a dedicated campaign.

## Consequences

ChronosDB now has OS-controlled evidence that authenticated established Job Control sessions remain
deadline-bound before request dispatch and after reducer admission. A lost PREPARE response can be
retried exactly without duplicate job allocation, and healed control traffic can cancel and later
reclaim the retained job.

The target still runs only with explicit opt-in inside an isolated privileged Linux namespace. It
adds no production code, dependency, durable format, or protocol change. Partial TLS-record and
application-frame interruption, TCP reset, delay, duplication, reordering, probabilistic loss,
bandwidth pressure, larger reducer sets, and durable query recovery remain unqualified.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): an identical PREPARE retry preserves exact query and
  reducer identity and is admitted as a duplicate rather than a second job.
- [Invariant 11](../architecture/invariants.md): borrowing TLS sessions are destroyed before their
  owned TCP descriptors, while the listener, TLS contexts, authenticators, and service outlive all
  attempts.
- [Invariant 13](../architecture/invariants.md): neither local request-write completion nor local
  response-write completion publishes a coordinator result.
- [Invariant 15](../architecture/invariants.md): both established-session black holes terminate at
  exact exchange deadlines, and terminal service retention ends at the execution deadline.
- [Invariant 18](../architecture/invariants.md): the gate composes production TCP, mutual TLS,
  request/response, authorization, service idempotency, and cleanup owners.

## Validation plan

Run all four packet-fault cases in at least three consecutive GCC 13 Ubuntu 24.04 repetitions with
`NET_ADMIN` and `NET_RAW` only, then require the namespace OUTPUT chain to equal its original
accept-only policy. Run the target under ASan/UBSan and TSan in the same isolated environment. Keep
the ordinary grouped-result process, cluster, and allocation-failure regressions passing. Run
formatting, static analysis, workflow pinning, and whitespace checks, and review the final diff for
scope expansion.

## Migration or rollback considerations

No migration exists. Rolling back removes only the two opt-in qualification cases, their reusable
test fixture, and this evidence. If a run is interrupted outside RAII cleanup, discard the required
ephemeral namespace.

## Unresolved questions

- Qualify controlled partial TLS-record/application-frame interruption and TCP reset.
- Add deterministic delay, duplication, reordering, probabilistic loss, and bandwidth campaigns.
- Qualify larger reducer sets, skew, CPU stalls, and deadline behavior under load.
- Specify durable query/job identity, fencing, and restart recovery before resumption is allowed.

## References

- [Mutually authenticated grouped reducer Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Authenticated partial Job Control frame qualification](0551-authenticated-partial-job-control-frame-qualification.md)
- [Linux netfilter directional Job Control partition qualification](0552-linux-netfilter-directional-job-control-partition-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
