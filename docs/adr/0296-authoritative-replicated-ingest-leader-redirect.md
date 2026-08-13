# ADR 0296: Authoritative replicated-ingest leader redirect

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, metadata, Raft, and networking maintainers
- **Extends:** [ADR 0280](0280-authoritative-replicated-ingest-routing.md),
  [ADR 0295](0295-negotiated-native-leader-redirect.md)

## Context

Replicated ingest already derives one tablet group from committed metadata and obtains an ordered
Raft observation before proposal. When the local node is a follower, returning only an execution
error forces a client to rediscover information the node can sometimes report exactly. An advisory
metadata leader hint is insufficient because it need not reflect the ordered consensus observation.

## Decision

After consuming the ordered group observation, the coordinator reacquires the immutable metadata
catalog and repeats its existing active-schema, tablet placement, group binding, and stable
membership checks. It emits a Protocol-2 `LEADER_REDIRECT` only when all of these also hold:

- the request negotiated the leader-redirect feature;
- the observed role is follower in a nonzero current term;
- the observation names a nonlocal leader; and
- that leader belongs to the exact committed placement replicas.

The payload uses the derived group, observed leader and term, and the revalidated current placement
epoch. A local leader continues to submit under its exact observed term. Candidate state, an absent
leader, joint/finalizing membership, placement/voter divergence, an out-of-placement leader, or a
missing feature fails with the existing correlated terminal error. A later leadership loss after
proposal admission also remains an error because that operation has no fresh ordered replacement
leader observation.

`chronosd` advertises the redirect bit only with its replicated service, alongside QUORUM_SYNC. The
coordinator counts terminal redirects independently of completed and rejected requests.

## Consequences and validation

The response is exact for the one group named by canonical ingest and never relies on a Raft peer
endpoint. Whole-query redirect remains unavailable because one SELECT can require leaders from
several groups. A real asynchronous durable-runtime test installs committed placement/binding,
delivers a leader heartbeat, and proves the exact redirect; the same observation without negotiated
capability produces an error.

Multi-process client endpoint routing, redirect retry limits, failover races after proposal, and
redirect metrics export remain later integration/hardening tasks.

## References

- [Native Protocol v2](../protocol/native-v2.md)
- [Replicated ingest coordinator](../learning/replicated-ingest-coordinator.md)
- [Native server operations](../operations/native-server.md)
