# ADR 0244: Pre-Observation Raft Message Validation

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB consensus and recovery maintainers

## Context

`RaftNode::receive` deliberately validates message-local fallible state before observing a newer
term, because a rejected higher-term message cannot mutate persistent term/vote state without also
returning the persistence transition required by the runtime. That gate covered embedded source
identity and append payload bounds, but a fresh node could still grant a structurally valid-looking
term-zero vote. Response conflict fields and vote/append predecessor pairs also admitted
noncanonical combinations before higher-term observation.

## Decision

Every received Raft message must carry a nonzero term before role or persistent state changes.
RequestVote last-log index/term and AppendEntries previous index/term use exact zero-pair semantics;
their log term cannot exceed the message term. Existing source/candidate/leader identity checks
remain mandatory.

A successful AppendEntries response cannot carry conflict state, its match index cannot exceed the
leader's log, and an optional conflict term must be nonzero. Failed responses may retain the
follower's last-known match index because the core emits that diagnostic during conflict repair.
A successful snapshot response must name a nonzero installed index within the leader's log.
Existing read-context, append-entry, and snapshot-metadata checks remain in the same pre-observation
validation pass.

An AppendEntries predecessor below the installed snapshot index is unavailable even when it is
index zero. Index zero is the canonical empty-log predecessor only before any snapshot exists; once
index one is compacted, accepting `(previous=0, entry=1)` would alias snapshot metadata with a
retained log entry whose bytes no longer exist. The follower returns an ordinary negative conflict
response. An exact predecessor at the snapshot boundary and retained suffix predecessors continue
to use their stored terms.

Invalid messages return `INVALID_ARGUMENT` without changing term, vote, role, log, commit, apply, or
snapshot state and without emitting a response. The authenticated transport envelope duplicates
several structural checks as defense in depth, but direct deterministic callers receive the same
core safety boundary.

## Consequences

Term zero remains only the local pre-election initialization value; it cannot become a voted Raft
term through input. A checksum-valid or in-memory malformed higher-term response cannot force a
step-down and then fail without a durable transition. Canonical messages produced by `RaftNode`
continue to round-trip through Raft Transport Envelope v1.

Compacted prefixes are never indexed through the retained-log offset function. A higher-term
request with such a predecessor still returns the updated persistent term/vote state alongside the
negative response, preserving persist-before-response ordering.

This does not prove every response-state combination or replace deterministic model exploration.
Those broader schedules and fault matrices remain Phase 18 work.

## Validation

Focused regression coverage presents zero-term votes and higher-term malformed vote, append,
append-response, snapshot-response, and read-barrier messages. Every message is rejected and the
complete persistent state remains byte-for-value equal. The election/replication/failover gate and
the full Raft suite remain green. A dedicated snapshot-index-one regression presents both current-
and higher-term `(previous=0, entry=1)` requests, requires an exact negative conflict response,
proves no retained entry is read or installed, and requires the higher term/vote reset to accompany
that response as persistent state.

## References

- [ADR 0069](0069-deterministic-raft-and-multiplexed-state-record.md)
- [ADR 0243](0243-canonical-raft-transport-envelope.md)
- [Raft Transport Envelope v1](../formats/raft-transport-v1.md)
