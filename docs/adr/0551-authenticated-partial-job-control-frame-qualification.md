# ADR 0551: Authenticated partial job-control frame qualification

- **Status:** accepted
- **Date:** 2026-08-30
- **Owners:** ChronosDB distributed-query, networking, reducer service, and test maintainers
- **Extends:** [ADR 0539](0539-header-first-grouped-reducer-job-control-transport.md),
  [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md), and
  [ADR 0550](0550-partial-multi-reducer-coordinator-loss-qualification.md)

## Context

The reusable Job Control v1-v4 readers already had direct byte-fragmentation tests, and the
production mutual-TLS owners had authentication, correlation, and deadline tests. Those separate
facts did not prove that the shared production query-control endpoint retained no reducer state
when an authenticated peer became silent in the middle of a request, or that the production client
withheld a result after receiving only a valid response prefix. A half-open connection must remain
bounded without treating a prefix as an admitted request or published response.

## Decision

Qualify three authenticated request prefixes through the real loopback TCP listener, mutual-TLS
handshake, shared query-control protocol discriminator, and Job Control request reader:

1. three bytes of the eight-byte protocol magic;
2. 25 bytes, covering the magic and part of the fixed header; and
3. the complete 128-byte checksummed PREPARE header plus one payload byte.

After writing each exact prefix, the client remains connected but sends no suffix. The shared
endpoint must close the connection at its configured exchange deadline, record no completed
control, increment failed-connection accounting, return to zero active connections, and leave
PREPARE and active-job counts at zero. After all three failures, a fresh production finite
acquisition must authenticate, PREPARE successfully through the same endpoint, and create exactly
one job.

Qualify the opposite direction with the production Job Control mutual-TLS client. The client sends
one complete PREPARE and reaches response-read state. Its authenticated peer sends the first 37
bytes of an otherwise valid, exactly correlated 100-byte v1 response and then remains connected but
silent. The client must retain no response result and fail `UNAVAILABLE` exactly at its exchange
deadline.

These are application-frame prefixes carried by real TLS over nonblocking sockets. The tests do
not add a transport hook or change production code, protocol bytes, retry policy, or reducer cleanup
semantics.

## Detailed rationale

Testing prefixes above and below the protocol discriminator, header validation boundary, and
payload-allocation boundary proves the shared owner does not depend on one fortunate split point.
Using a silent authenticated peer distinguishes bounded partial-frame ownership from malformed-byte
rejection or immediate connection close. Reusing the endpoint after all failures verifies that
cleanup does not poison listener capacity or later canonical admission.

The response case uses an exactly correlated prefix so corruption or identity rejection cannot
accidentally satisfy the test. No result exists until the fixed response reader owns all bytes and
the canonical decoder plus correlation checks complete.

## Alternatives considered

- **Rely only on direct reader tests:** those prove codec behavior but not TLS deadlines, shared
  endpoint connection reclamation, service non-dispatch, or endpoint reuse.
- **Use malformed complete frames:** those exercise integrity rejection, not ownership of a valid
  prefix on a half-open connection.
- **Claim packet-partition coverage:** application scheduling can create exact plaintext prefixes,
  but it does not control kernel packet boundaries, loss, reordering, retransmission, or firewall
  healing. Those remain a separate network fault campaign.

## Consequences

Authenticated partial Job Control requests cannot create reducer state, retain shared-endpoint
capacity beyond the exchange deadline, or prevent later admission. Partial responses cannot escape
as correlated success. Together with the existing header-first v1-v4 fragmentation tests, the
application-frame qualification gap is closed.

This does not qualify TCP RST at every byte, partial TLS records, directional packet loss,
reordering, retransmission delay, black-hole detection, or healed partitions. It also does not
replace the existing coordinator-level cancellation and reducer deadline/lease tests. An OS-level
fault harness must compose those production owners before packet-partition coverage is claimed.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): neither an incomplete request nor response can
  publish a query-correlated result or reducer identity.
- [Invariant 10](../architecture/invariants.md): payload admission still waits for the complete
  checksummed fixed header and full-frame integrity.
- [Invariant 11](../architecture/invariants.md): partial TLS/TCP state has one owner and is reclaimed
  before the endpoint admits its fresh acquisition.
- [Invariant 14](../architecture/invariants.md): the tests use unchanged frozen v1 request and
  response bytes through the version-discriminating shared endpoint.
- [Invariant 15](../architecture/invariants.md): exchange deadlines and connection bounds cap both
  silent request and response prefixes.
- [Invariant 18](../architecture/invariants.md): qualification exercises the production TLS,
  endpoint, reader, client, and acquisition owners without weakening their contracts.

## Validation plan

Two focused tests cover three inbound prefix boundaries, service non-dispatch, connection
reclamation, fresh endpoint reuse, outbound response non-publication, and exact deadline failure.
They pass in 25 consecutive repetitions. The complete warning-as-error cluster suite passes 364
tests. The unchanged allocation-failure suite passes 80 tests. Focused ASan/UBSan and TSan, GCC 13
Ubuntu 24.04, formatting, workflow pinning, and whitespace checks pass. Pinned clang-tidy 18 reaches
only the known macOS 26 libc++ unsupported-builtin errors and reports no project-source diagnostic.

## Migration or rollback considerations

No durable or network migration exists because only tests and documentation change. Rollback
removes production-owner partial-frame evidence without changing any deployed behavior or bytes.

## Unresolved questions

- Qualify asymmetric packet loss and healing with a Linux network namespace or equivalent
  OS-controlled fault harness around the production coordinator and reducers.
- Qualify larger reducer sets, skew, CPU stalls, and deadline behavior under load.
- Specify durable query/job identity, fencing, and restart recovery before resumption is allowed.

## References

- [Job Control v1](../formats/distributed-vector-grouped-aggregate-shuffle-job-control-v1.md)
- [Header-first Job Control transport](0539-header-first-grouped-reducer-job-control-transport.md)
- [Mutually authenticated Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
