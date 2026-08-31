# ADR 0558: Controlled partial client-authentication record qualification

- **Status:** accepted
- **Date:** 2026-08-31
- **Owners:** ChronosDB networking, cluster-security, distributed-query, and test maintainers
- **Extends:** [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md),
  [ADR 0540](0540-mutually-authenticated-grouped-reducer-job-control-session.md), and
  [ADR 0557](0557-controlled-partial-tls-handshake-record-qualification.md)

## Context

ADR 0557 qualified an incomplete ClientHello and the first encrypted server-handshake record. It
stopped before the asymmetric point where the coordinator-side TLS owner has authenticated the
reducer certificate, but the reducer has not yet received a complete encrypted client
certificate/Finished flight. At that point, client-side authorization may legitimately exist while
server-side authorization must still be impossible.

The security boundary needs executable evidence that a partial first encrypted client-flight record
cannot make the reducer's application authenticator observe a coordinator fingerprint, cannot
dispatch PREPARE, and cannot leave either session owner alive after the proxy reports reset.

## Decision

Extend the controlled handshake test with a third three-cut campaign using the same retained backend
listener, TLS contexts, authenticators, authorizer, reducer service, and raw two-leg proxy.

For each fresh attempt:

1. capture and forward the complete initial ClientHello;
2. drive the production server and forward every complete server handshake record;
3. after each raw record, wait for endpoint readability and advance only the corresponding
   production handshake owner;
4. forward any complete client compatibility record; and
5. capture, but do not forward completely, the first outer application-data record emitted by the
   client authentication flight.

The proxy polls both raw directions so server-flight segmentation and client response scheduling do
not determine which record is selected. For the captured encrypted client record, forward exactly:

1. 2 bytes, inside the five-byte TLS record header;
2. 6 bytes, the complete header plus one encrypted payload byte; and
3. all record bytes except the final byte.

Before each cut, the client-side Job Control authenticator must have observed one additional verified
server fingerprint, while the reducer-side authenticator must still have observed zero client
fingerprints across the entire campaign. After forwarding the prefix, reset both proxy-owned legs.
Both production session owners must enter `Failed`, with zero PREPARE requests and zero active jobs.

After all three failures, a direct connection through the retained backend listener must increment
the reducer-side authenticator exactly once, complete PREPARE, authenticate CANCEL, and reclaim the
terminal job at its original execution deadline.

## Detailed rationale

Counting authenticator invocations distinguishes transport verification from application
authorization more strongly than checking only terminal states. The coordinator is allowed to trust
the complete server flight before emitting its own encrypted response. The reducer is not allowed to
trust a client flight that is missing even one outer-record byte.

The proxy selects the first client content-type-23 record without decrypting or claiming how OpenSSL
groups Certificate, CertificateVerify, and Finished handshake messages inside it. The qualified
contract is therefore precise: this first encrypted client-authentication-flight record cannot be
partial at the reducer. Subsequent records in an unusually segmented flight remain separate.

Polling both proxy directions avoids an ordering assumption between complete server records and the
client's compatibility response. Waiting for the destination endpoint to become readable before
advancing it also prevents a fast synthetic readiness loop from being mistaken for record loss.

## Alternatives considered

- **Assume the server-flight cut covers mutual authentication:** rejected because the reducer has
  not yet received any client certificate response at that boundary.
- **Assert only zero PREPARE dispatch:** rejected because an incorrectly invoked application
  authenticator could still violate the security boundary before protocol dispatch.
- **Identify inner handshake messages by decrypting the record:** rejected because the proxy must
  remain below and independent of production TLS key ownership.
- **Depend on one fixed record order:** rejected because OpenSSL may emit compatibility records or
  split a handshake flight differently across supported environments.

## Consequences

ChronosDB now has exact real-TCP evidence for the complete asymmetric mutual-authentication
transition: initial client framing, encrypted server flight, and first encrypted client response.
Server trust at the coordinator does not imply client trust at the reducer. A partial client record
causes bilateral terminal failure without authentication, dispatch, or retained job state, and the
backend listener remains reusable.

The change adds only test and documentation code plus an authenticator invocation counter in the
fixture. It adds no dependency, production API, trust shortcut, network byte, or durable format. It
does not qualify every later client-flight record under unusual segmentation, multi-record
application frames, resets racing buffered suffixes, or general packet delay, duplication,
reordering, probabilistic loss, and bandwidth pressure.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): partial client authentication cannot enter committed
  query visibility machinery.
- [Invariant 6](../architecture/invariants.md): asymmetric transport trust cannot create query or
  reducer state.
- [Invariant 11](../architecture/invariants.md): production TLS borrowers retain their endpoint
  descriptors while the proxy resets only its independently owned legs.
- [Invariant 14](../architecture/invariants.md): unchanged production mutual-TLS negotiation and Job
  Control bytes are exercised before and after every failure.
- [Invariant 15](../architecture/invariants.md): observed reset terminates both handshake owners
  immediately rather than consuming the handshake deadline.
- [Invariant 18](../architecture/invariants.md): the qualification does not bypass OpenSSL record
  integrity, certificate verification, or either application authenticator.

## Validation plan

All eight focused Job Control TLS tests pass, and the expanded nine-cut handshake case passes 25
consecutive Apple arm64 repetitions. The complete cluster suite passes 368 tests, the
allocation-failure suite passes 80, and the grouped-result process suite passes eight. The focused
suite passes with GCC 13 on Ubuntu 24.04 normally and under ASan/UBSan and TSan. The privileged Linux
netfilter target passes all four cases and leaves the OUTPUT chain at its original accept-only
policy. Formatting, workflow-pinning, and whitespace checks pass. Pinned clang-tidy 18 reaches only
the known macOS 26 libc++ unsupported-builtin errors after reporting no project-source diagnostic.
The final diff is reviewed for accidental scope expansion.

## Migration or rollback considerations

No migration exists. Rolling back removes only the first encrypted client-flight cuts, proxy
direction selection helper, fixture invocation counter, and their evidence. No deployed identity,
session, stored state, or compatibility boundary changes.

## Unresolved questions

- Qualify every later encrypted client-flight record under forced unusual segmentation.
- Qualify multi-record application frames and resets racing buffered record suffixes.
- Add deterministic delay, duplication, reordering, probabilistic loss, and bandwidth campaigns.
- Qualify larger reducer sets, skew, CPU stalls, and durable query recovery/fencing.

## References

- [Maintained mutual-TLS socket carrier](0144-maintained-mutual-tls-socket-carrier.md)
- [Mutually authenticated grouped reducer Job Control session](0540-mutually-authenticated-grouped-reducer-job-control-session.md)
- [Controlled partial TLS handshake-record qualification](0557-controlled-partial-tls-handshake-record-qualification.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
