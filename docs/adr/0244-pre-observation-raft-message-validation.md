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
noncanonical combinations before higher-term observation. Snapshot metadata had the same omitted
term relation: its last-included term could exceed the leader term carried by the request. Failed
append responses could likewise omit their conflict index or claim a conflict term newer than the
response term, while recovery admitted snapshot metadata newer than the node's current term. Remote
snapshot admission also accepted the maximum logical index even though recovery reserves it for
exhaustion detection. Vote requests could advertise the same impossible last-log index and obtain a
vote after higher-term observation even though no canonical local state can contain that entry. The
transport codec also admitted the reserved value as the predecessor of an empty AppendEntries
heartbeat even though the deterministic core rejected it. Both boundaries admitted the same value
as `leader_commit`, allowing an impossible higher-term commit claim to change candidate state.
Failed AppendEntries responses could likewise report the reserved value as their last known match
index and change candidate state before leader-context processing.
Failed InstallSnapshot responses could report it as their installed boundary and cause the same
higher-term state change.

## Decision

Every received Raft message must carry a nonzero term before role or persistent state changes.
RequestVote last-log index/term and AppendEntries previous index/term use exact zero-pair semantics;
their log term cannot exceed the message term, and a candidate's advertised last-log index must
remain below the reserved `UINT64_MAX`. An AppendEntries predecessor must remain below the same
bound even when the request contains no entries, and `leader_commit` must also remain below that
reserved value. Existing source/candidate/leader identity checks remain mandatory. An
InstallSnapshot request likewise requires its nonzero last-included term not to exceed the request
term and its last-included index to remain below `UINT64_MAX` before any role, term, vote, or pending
external-install state changes.

A successful AppendEntries response cannot carry conflict state, and its match index cannot exceed
the leader's log. A failed response requires a nonzero conflict index; an optional conflict term must
be nonzero and no greater than the response term. Failed responses may retain the follower's last-
known match index because the core emits that diagnostic during conflict repair. In both response
states that actual match position must remain below `UINT64_MAX`.
A successful snapshot response must name a nonzero installed index within the leader's log. A
failed response may report the zero empty-snapshot boundary, but both response states must remain
below `UINT64_MAX`.
Existing read-context, append-entry, and snapshot-metadata checks remain in the same pre-observation
validation pass.
An AppendEntries request that would replace a committed entry is corruption, whether it changes the
entry term or reuses the term with different type or payload bytes. That comparison also completes
before a higher request term is observed.

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

Recovered persistent state must also be inductive: an installed snapshot's last-included term may
not exceed the node's current term. Retained entries already obey the same relation.

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
append-response, snapshot-request, snapshot-response, and read-barrier messages. The impossible
snapshot request carries a last-included term above its own request term and is also rejected by
checksum-valid transport decoding and outbound encoding. Every direct message is rejected while
the candidate role and complete persistent state remain unchanged. The election/replication/
failover gate and the full Raft suite remain green. A dedicated snapshot-index-one regression
presents both current- and higher-term `(previous=0, entry=1)` requests, requires an exact negative
conflict response, proves no retained entry is read or installed, and requires the higher term/vote
reset to accompany that response as persistent state.
Failed append-response coverage rejects a missing conflict index and a conflict term above the
response term through direct-core, outbound encoding, and checksum-valid decoding paths. Recovery
separately rejects a persisted snapshot term above current term.
Maximum-index snapshot coverage rejects direct-core admission without changing candidate state and
rejects both outbound encoding and checksum-valid decoding. No external install owner is published.
Maximum-index vote coverage applies the same three-path rejection and proves the candidate cannot
step down or persist a vote for an impossible log advertisement.
Maximum-index AppendEntries predecessor coverage proves direct-core rejection without candidate
state changes and rejects both an outbound empty heartbeat and its checksum-valid decoded form.
Maximum-index leader-commit coverage uses the same three paths and proves that an impossible
higher-term commit advertisement cannot change candidate role or persistent state.
Maximum-index AppendEntries match coverage rejects a failed response through direct-core, outbound
encoding, and checksum-valid decoding before a higher-term response can change candidate state.
Maximum-index snapshot-response coverage applies the same three paths while preserving zero as the
canonical failed response for a follower without an installed snapshot.
Committed-prefix overwrite coverage presents higher-term requests with both a different entry term
and matching-term divergent bytes, requires `CORRUPTION`, and proves exact term, vote, role, and
persistent-state preservation.

## References

- [ADR 0069](0069-deterministic-raft-and-multiplexed-state-record.md)
- [ADR 0243](0243-canonical-raft-transport-envelope.md)
- [Raft Transport Envelope v1](../formats/raft-transport-v1.md)
