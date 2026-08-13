# ADR 0306: Authenticated Raft observation transport

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, query, Raft, and security maintainers
- **Extends:** [ADR 0303](0303-correlated-follower-read-proof-binding.md),
  [ADR 0304](0304-packaged-bounded-stale-query-construction.md)

## Context

The bounded-stale binder requires complete same-group, same-term leader and follower observations.
The packaged constructor still accepts those observations from its caller because no authenticated
protocol can ask a remote node for its ordered local state. Raft Transport v1 cannot supply that
state: append responses omit follower applied position and complete membership, and consensus
messages must not be extended in place with query-only observation semantics.

## Decision

Raft Observation Transport v1 is a separate checksummed cluster protocol. A fixed request binds
source, target, nonnil group, and nonzero correlation identity. Its response reverses the route and
exactly repeats group/correlation plus either one status or one complete bounded
`RaftGroupObservation`. The payload preserves role, term, leader, ordered log/commit/apply indexes,
all current/committed/joint voter sets, and membership transition flags. Strict semantic validation
rejects state that the local Raft core cannot legitimately expose.

`RaftObservationReceiver` requires a transport-authenticated principal before decoding, authorizes
that principal for the claimed source, exact-matches its configured local node, and then invokes an
embedding-owned `RaftObservationService`. That service must acquire the observation through the
node's single ordered durable Raft owner; cached scalar reconstruction is not valid. Service
failures become correlated status responses, thrown exceptions are contained, and uncorrelated
success is rejected rather than encoded.

This task intentionally ends at the exact codec and receiver boundary. A subsequent carrier must
provide header-first bounded stream reads, move-only short-write ownership, mTLS deadlines,
principal mapping, finite retry, and coordinator fan-out to both selected leader and follower.

## Consequences

Remote nodes now have a versioned, bounded, authenticated way to publish the exact evidence the
existing follower binder consumes, without changing Raft Transport v1 or treating observations as
consensus messages. Correlation prevents a response for one node/group/acquisition from being
silently paired with another.

The default maximum retains 31 IDs per voter set and validation is linear in total voter IDs. The
codec owns request/response vectors; decoding returns owning voter sets. No durable format changes.
Socket integration and complete multi-node acquisition remain explicitly incomplete, so the
Phase 16 exit gate is not claimed.

## Alternatives considered

- **Infer follower applied state from AppendEntries response:** rejected because match index is not
  application visibility and the response lacks complete membership.
- **Add a ninth Raft Transport v1 message:** rejected because it changes a frozen consensus
  protocol and mixes query evidence with mutation traffic.
- **Return only term/commit/apply scalars:** rejected because group, node, role, leader, and stable
  membership correlation are part of the bounded-stale proof.
- **Trust caller-supplied observation bytes before authentication:** rejected because CRC detects
  damage, not node identity or authority.

## Failure modes and operations

Unauthenticated, unauthorized, wrong-target, damaged, unknown-version, over-limit, and
uncorrelated-service results fail before an observation is exposed. A legitimate local observation
failure is returned as one exact correlated response status. Metrics and carrier logs should retain
route/group/correlation identity without logging certificate material.

## Validation

Focused tests round-trip exact request, success, and failure frames; freeze their current lengths;
and reject damage, checksum-valid unknown versions, voter limits, inconsistent leader state, joint
flags, and missing success payloads. Receiver tests prove authentication before decode,
source-principal authorization, target matching, no service call on trust failure, exact successful
correlation, correlated service failure, uncorrelated success rejection, and exception containment.
The public header is self-contained.

Invariants 4–6, 10, 11, 14, and 18 apply.

## Migration and rollback

This is the first release of a separate cluster protocol and has no existing peer migration. A
deployment must not send these bytes to a Raft Transport or distributed-query listener. Rolling
back removes remote observation serving and leaves the existing caller-supplied packaged
bounded-stale constructor unchanged.

## References

- [Raft Observation Transport v1](../formats/raft-observation-transport-v1.md)
- [Correlated follower read proof binding](0303-correlated-follower-read-proof-binding.md)
- [Packaged bounded-stale query construction](0304-packaged-bounded-stale-query-construction.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
