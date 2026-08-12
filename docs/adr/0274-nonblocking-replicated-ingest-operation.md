# ADR 0274: Nonblocking replicated ingest operation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, ingestion, Raft, and network maintainers

## Context

Protocol 2.0 and the worker-affine application owner provide the two ends of QUORUM_SYNC, but a
service must correlate the exact canonical command, term-bound proposal, applied receipt, retry
outcome, and acknowledgement without blocking the durable worker or inferring from latest state.

## Decision

`ReplicatedIngestOperation` is one move-only, nonblocking service owner. Submission exact-decodes a
canonical COLUMNAR_APPEND, retains its retry and mutation identities, and admits one Raft proposal
with an exact required leader term. `poll` first consumes the post-sync proposal completion and
requires one successful persistence transition whose group, current term, final log entry type,
term, and exact decoded command identities match the submission. It then registers the exact
group/term/index applied-quorum completion.

After that completion succeeds, the operation acquires one tablet snapshot and exact-validates the
published retry outcome. Its original Raft record sequence distinguishes `APPLIED` from a later
`MATCHING_RETRY`; both attempts receive their own exact applied quorum receipt. The projection
function encodes the existing frozen 64-byte protocol-v2 acknowledgement.

Destruction cancels receipt waiting through weak ownership but cannot undo an admitted durable Raft
entry. The embedding owns request deadlines, disconnect cancellation, polling/wakeup integration,
routing, schema/placement authorization, and response-queue backpressure.

## Consequences and validation

No durable or network format changes. Focused tests cover an applied command, an exact matching
retry at a later Raft index, acknowledgement round-trip, and rejection outside the required leader
term. Delayed multi-node service polling, reactor integration, disconnect/deadline races, crash
cuts, TSan, and load measurement remain deferred.

## References

- [ADR 0271](0271-native-protocol-v2-quorum-sync-negotiation.md)
- [ADR 0273](0273-bounded-term-bound-applied-quorum-completions.md)
