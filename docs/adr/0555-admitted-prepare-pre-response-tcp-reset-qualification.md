# ADR 0555: Admitted PREPARE pre-response TCP reset qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, networking, reducer service, and test maintainers
- **Extends:** [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md),
  [ADR 0553](0553-established-job-control-black-hole-qualification.md), and
  [ADR 0554](0554-authenticated-partial-frame-tcp-reset-qualification.md)

## Context

The established-session partition gate proved retry after a complete PREPARE response was
black-holed. The partial-frame reset gate proved that abortive loss cannot dispatch an incomplete
request or publish an incomplete response. Neither isolated the ambiguity after a reducer has
authenticated, validated, and admitted a complete PREPARE but before its server session performs
the first response write.

This distinction matters because admission must survive an abandoned response session exactly once.
It also exposed a transport edge: after the kernel reports a readable peer reset, `SSL_write` can
still accept a small response into OpenSSL's buffer and make an application owner appear complete
without a surviving peer.

## Decision

Before the Job Control TLS server performs a response write, inspect a reported readable event for
peer input or shutdown. A TLS error or close fails the session. Any complete plaintext is an invalid
trailing request suffix and fails with corruption. `WantRead` permits the pending response write;
`WantWrite` retains write interest. This check does not claim remote acknowledgement of a successful
TCP write; it closes the narrower race in which peer failure is already observable before the first
response byte is offered.

Add a real-loopback-TCP production-session test that drives the coordinator client to
`ReadingResponse` and the reducer server to `WritingResponse`. That exact server state proves the
complete PREPARE has been dispatched and its response encoded while the response writer has not run.
Destroy the client TLS borrower, abortively close its owning socket with `SO_LINGER(0)`, and wait for
the server descriptor to report the failure before presenting read/write readiness to the server.

The server must fail immediately while the service retains exactly one active job. Through the same
listener, TLS contexts, authenticators, authorizer, and service, an identical PREPARE must succeed as
one duplicate rather than create a second job. Authenticated CANCEL must then retain terminal
identity only until the original execution deadline, after which polling reclaims it.

## Detailed rationale

The production server transitions from request reading to response writing only after service
admission and complete response encoding. Stopping at that transition is deterministic and avoids a
sleep-based approximation of the admission boundary. Waiting for kernel failure readiness before
calling the response state also distinguishes this gate from resets racing an already-buffered
response.

The service owns admitted job lifetime independently from the transport session. Failing the session
must therefore not roll admission back, while canonical duplicate PREPARE handling must return the
existing identity and increment the duplicate counter. Cancellation and original-deadline cleanup
prove that the ambiguous response does not leak permanent reducer state.

## Alternatives considered

- **Accept local `SSL_write` completion as session success:** a queued reset can already prove the
  peer is gone, so ignoring readable failure creates misleading session state.
- **Roll back PREPARE when response transport fails:** the peer cannot know whether admission
  happened, making retry unsafe and breaking idempotent ownership.
- **Reuse the response black-hole gate:** that path suppresses delivery after the server has written;
  it does not prove the pre-write state transition.
- **Claim every reset/response race:** a reset after response bytes enter TLS or kernel buffers is a
  separate nondeterministic boundary and remains open.

## Consequences

The production Job Control server now fails a response session when peer loss is already observable
before its first response write. The reducer retains one admitted job, exact retry is idempotent, and
authenticated cleanup remains bounded through the retained endpoint.

The change adds no dependency and changes no durable or wire format. It adds one nonblocking read
probe only when the event loop reports response-state readability. It does not provide remote
response acknowledgement and does not qualify resets racing TLS/kernel-buffered response bytes,
partial encrypted TLS records, or general packet delay, duplication, reordering, or probabilistic
loss.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): a failed response session cannot publish client
  success, while an admitted request remains exactly once in service state.
- [Invariant 10](../architecture/invariants.md): trailing plaintext after the complete request is
  rejected rather than interpreted as another message.
- [Invariant 11](../architecture/invariants.md): the TLS borrower is destroyed before its owning TCP
  socket and all retained dependencies outlive retry and cleanup attempts.
- [Invariant 14](../architecture/invariants.md): retry uses the unchanged canonical PREPARE bytes and
  identity.
- [Invariant 15](../architecture/invariants.md): peer reset is terminal immediately; admitted state
  is reclaimed at the original execution deadline.
- [Invariant 18](../architecture/invariants.md): the gate composes production TCP, mutual TLS,
  authentication, parsing, admission, idempotency, cancellation, and reclamation.

## Validation plan

All six focused Job Control TLS tests pass in 25 consecutive Apple arm64 repetitions. The complete
cluster suite passes 366 tests, the allocation-failure suite passes 80, and the grouped-result
process suite passes eight. The focused suite passes with GCC 13 on Ubuntu 24.04 normally and under
ASan/UBSan and TSan. The privileged Linux netfilter target passes all four cases and leaves the
OUTPUT chain at its original accept-only policy. Formatting, workflow-pinning, and whitespace checks
pass. Pinned clang-tidy 18 reaches only the known macOS 26 libc++ unsupported-builtin errors and
reports no project-source diagnostic. The final diff is reviewed for accidental scope expansion.

## Migration or rollback considerations

No migration exists. Rolling back restores the possibility that a response owner reports local
completion despite an already-readable peer failure and removes the exact idempotent-retry gate. No
stored or transmitted byte changes.

## Unresolved questions

- Qualify partial encrypted TLS-record interruption at controlled wire-byte boundaries.
- Qualify resets racing TLS- or kernel-buffered request/response suffixes.
- Add deterministic delay, duplication, reordering, probabilistic loss, and bandwidth campaigns.
- Qualify larger reducer sets, skew, CPU stalls, and durable query recovery/fencing.

## References

- [Mutually authenticated grouped reducer Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Established Job Control black-hole qualification](0553-established-job-control-black-hole-qualification.md)
- [Authenticated partial-frame TCP reset qualification](0554-authenticated-partial-frame-tcp-reset-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
