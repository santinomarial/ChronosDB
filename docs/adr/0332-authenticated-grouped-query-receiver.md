# ADR 0332: Authenticated grouped query receiver

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, cluster, and networking maintainers
- **Extends:** [ADR 0168](0168-authenticated-distributed-query-transport.md),
  [ADR 0181](0181-authenticated-distributed-leader-hint-publication.md),
  [ADR 0331](0331-bounded-grouped-query-partial-io.md)

## Context

Grouped bytes and partial-I/O owners do not establish peer identity or safely connect decoded
requests to worker execution. A multi-frame success also must not expose an encoded prefix before
the receiver proves the complete worker stream is correlated, contiguous, terminal, and bounded.

## Decision

`DistributedGroupedQueryReceiver` requires an authenticated peer result before request decoding.
It authorizes that principal for the claimed source node, exact-matches the local target, and only
then invokes an embedding-owned `DistributedGroupedQueryWorkerService` once. The service supplies
the complete proof-revalidated grouped worker result synchronously and must outlive the receiver.

For a nonempty grouped result, the receiver requires one-based contiguous sequence, exact query and
tablet identity, no early terminal, and a terminal final partial. A terminal-only result must match
the same identity at sequence one. The configured response-frame limit is nonzero and at most the
coordinator's 65,536-message hard ceiling; its default is 1,024. Every response is encoded into one
local owned vector before the complete vector is returned, so validation or encoding failure
publishes no prefix.

Worker failures become one correlated status response. `UNAVAILABLE` may consult the existing
committed-metadata leader-hint provider for the exact tablet and Raft group after authentication and
worker failure. Provider failure aborts publication. Worker exceptions are contained and become one
`INTERNAL` response; allocation failures become `RESOURCE_EXHAUSTED`. A successful stream over the
configured frame limit becomes one correlated `RESOURCE_EXHAUSTED` response.

The receiver is single-owner and synchronous. It consumes an authentication result but does not own
TLS, descriptors, connection closure, or response writes.

## Consequences and validation

Untrusted peers cannot invoke the worker or committed metadata provider before authentication,
source authorization, decoding, and local-target validation. Complete successful response memory
is bounded by the configured frame count times the 252-byte frame maximum plus vector ownership.
CRC integrity, peer authentication, and worker-local Raft/snapshot authority remain distinct gates.

One focused receiver case proves authentication/source/target ordering, a two-part contiguous
terminal stream, terminal-only success, exact unavailable hint lookup, malformed-sequence rejection
without a response vector, exception containment, and configured frame exhaustion. Together with
codec and stream coverage, all six grouped transport cases pass. The installed-consumer gate
references receiver construction and the public worker boundary is abstract.

ADR 0333 subsequently supplies the production request-local real-CSEG service adapter.
ADR 0335 subsequently supplies ordered multi-response TLS closure and write ownership.
TCP acquisition/listener ownership, sender/coordinator integration, packaged multi-tablet grouped
execution, and broad fault/measurement evidence remain incomplete. No Phase 16 exit gate is
claimed.

ADR 0334 subsequently packages that service with this receiver under stable move-only ownership.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Query Transport
  v1](../formats/distributed-grouped-float64-query-transport-v1.md)
- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
- [Authenticated distributed leader-hint publication](0181-authenticated-distributed-leader-hint-publication.md)
- [Proof-revalidated grouped FLOAT64 worker](0328-proof-revalidated-grouped-float64-worker.md)
