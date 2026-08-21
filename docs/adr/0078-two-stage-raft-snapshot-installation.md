# ADR 0078: Two-stage Raft snapshot installation

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB distributed-systems and storage maintainers
- **Extended by:** [ADR 0085](0085-raft-tablet-application-snapshot-v1.md) and
  [ADR 0129](0129-tablet-movement-raft-snapshot-completion.md)

## Context

A lagging follower whose `next_index` precedes the leader's retained log needs a snapshot. The pure
Raft core cannot claim that tablet rows, retry state, metadata state, manifests, or part files are
installed merely because it received snapshot metadata. A response sent before those external
bytes and the new Raft state are durable could expose a partial state or let the leader count data
the follower cannot recover.

## Accepted decision

Replication emits `InstallSnapshotRequest` when a peer needs an index at or below the leader's
snapshot boundary. Receipt validates the nonzero term, requires the snapshot's last-included term
not to exceed that message term, rejects the reserved maximum logical index, and validates leader
identity, snapshot identity, membership checkpoint, and bounds before returning a pending-install
transition without acknowledging success.
The application/storage owner must independently transfer, verify, and durably install the exact
snapshot named by its manifest generation and part-set checksum.

Exactly one external snapshot installation may be pending per node. An exact duplicate request
coalesces without republishing application work or responding early. A different request receives a
negative response and cannot replace the original source/term/metadata identity. If that competitor
carries a higher term, the normal term/vote transition accompanies the negative response and must
be persisted first. Once the original completion succeeds or fails, a later request may be admitted.

Request admission is prepare-before-publish. Stale, already-installed, and competing requests own
their exact response and any higher-term persistent state before changing the node. A new request
owns two exact metadata copies before publication: the core's pending completion identity and the
external installation task returned to the application owner. Allocation failure returns
`RESOURCE_EXHAUSTED` without demotion, leader-identity change, or partial installation ownership.

Only `complete_snapshot_install(..., installed=true)` may atomically install the Raft snapshot
metadata, membership checkpoint, commit/applied boundary, and compatible retained suffix. Its
persistent transition crosses `DurableMultiRaftRuntime`'s synchronization boundary before the
success response is released. Rejection returns a negative response without changing the snapshot.
Conflicts with an already committed local prefix fail as corruption without consuming the pending
completion identity; the owner can still explicitly reject that exact installation.

Completion also prepares before publication. A rejection owns its response before clearing the
pending identity. A success owns the retained suffix, derived membership, exact new persistent state
for both the node and returned transition, commit notification, and success response before changing
the node. Allocation failure is `RESOURCE_EXHAUSTED` and retains the original completion authority
for exact retry.

Local `compact_snapshot` is allowed only for a strictly newer applied prefix under a stable
configuration, with an exact local term and nonzero application manifest generation. It is
unavailable while an externally owned snapshot installation is pending; that installation must be
completed or explicitly rejected before another snapshot identity can be created. Raft fills the
canonical voter checkpoint and removes only entries covered by that application snapshot.

## Consequences and alternatives

Consensus remains deterministic and does not own large snapshot bytes, while the caller cannot
forge installation by merely delivering an RPC. Restart preserves the installed boundary through
the versioned full-state record. Snapshot transfer may be retried; no success leaves the follower
before both application installation and Raft persistence complete.

Remote installation and local compaction are serialized at the core boundary. This prevents two
immutable application snapshots with different manifest/checksum identity from racing at one Raft
index and prevents local compaction from invalidating the retained suffix calculation for a pending
completion.

Competing remote requests are serialized for the same reason. Coalescing an exact retransmission
avoids duplicate external ownership, while rejecting a different request preserves one completion
identity that the application owner can resolve deterministically.

Embedding unbounded snapshot bytes in one Raft message was rejected because it bypasses chunking,
resource limits, durable file installation, and resumability. Immediately accepting metadata was
rejected because it creates a fake application state. Log-only catch-up was rejected because it
prevents bounded history and reclamation.

The tablet and metadata application owners still need versioned snapshot contents and installation
adapters. Production transport encoding, chunk orchestration, crash injection, and physical shared-
segment reclamation remain follow-up work.

## Affected invariants and validation

Invariants 1, 4, 5, 8, 10, 11, 14, and 18 apply. Focused core tests cover applied-prefix
compaction, pending installation without early response, synchronized completion, suffix catch-up,
membership restoration, durable reopen, and committed-prefix conflict rejection. AppendEntries
predecessors below the installed snapshot boundary now receive an ordinary negative response
without consulting compacted entry storage; a higher-term request retains the required persistent
term transition before that response. A pending-install regression attempts a different same-index
local compaction, requires `UNAVAILABLE` with byte-for-value state preservation, then completes the
original installation and verifies its exact retained suffix and success response. A second branch
rejects the installation, verifies its negative response, and then permits the local compaction.
Request-level coverage coalesces an exact duplicate, rejects a different same-term snapshot without
losing the first completion, persists a higher-term competitor before its negative response, clears
the stale pending work through negative completion, and then admits the higher-term retry.
Allocation sweeps separately cover stale rejection, higher-term acknowledgement of an already
installed boundary, and higher-term publication of a new pending installation. Each failed owned
allocation preserves the exact leader, durable state, and pending work; retry returns the complete
response or the same external installation task.
Completion sweeps cover explicit rejection and successful installation with a compatible retained
suffix. Every failed response, suffix, membership, and state-copy allocation preserves both the
pending authority and byte-for-value node state; retry returns the exact negative response or full
durable installation transition.
An impossible higher-term request whose snapshot term is newer than its own leader term is rejected
before term/vote/role observation or external installation publication.
The same boundary rejects `UINT64_MAX` before publication so explicit completion can never persist a
snapshot that recovery must reject as index-exhausted.
Committed-prefix conflict coverage also requires complete persistent-state preservation and proves
the pending authority remains available for an explicit negative completion.
