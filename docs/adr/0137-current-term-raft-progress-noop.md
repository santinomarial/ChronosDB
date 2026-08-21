# ADR 0137: Current-term Raft progress no-op

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** ChronosDB distributed-systems, metadata, and tablet maintainers
- **Extends:** [ADR 0075](0075-durable-metadata-raft-commands.md),
  [ADR 0076](0076-joint-consensus-raft-membership.md), and
  [ADR 0136](0136-idempotent-retained-reconfiguration-action-replay.md)

## Context

An exact placement or membership action can remain uncommitted in the retained log after its leader
term ends. A new leader must not append a duplicate logical command, but Raft cannot advance its
commit index based only on a majority-replicated entry from an earlier term. ADR 0136 therefore
failed such retries with `UNAVAILABLE` until a safe current-term progress mechanism existed.

The progress entry is durable Raft state and is observed by metadata and tablet application owners.
Its identity, validation, application behavior, and rollback boundary must consequently be explicit
rather than represented as an ordinary application command.

## Decision

Logical Raft entry type `253` is reserved as the leader progress no-op. Its payload is exactly empty.
Types `253`, `254`, and `255` are Raft-internal and generic proposal interfaces reject all three.
Persistent-state construction and AppendEntries preflight reject a type-253 entry with any payload.

`RaftNode::commit_current_term` is leader-only. If the retained log already contains an entry from
the leader's current term, it returns an empty successful transition because that entry can provide
the current-term commit proof. Otherwise it appends one empty type-253 entry, updates local
replication state, attempts commit advancement, persists the resulting state, and emits the normal
bounded AppendEntries messages. `MultiRaftRuntime` and `DurableMultiRaftRuntime` expose the operation
without changing their persist-before-send contract.

The append path executes against a prospective copy of the complete deterministic node. It owns the
no-op, self progress, any immediate commit and membership derivation, complete replication batch,
and returned persistent state before replacing the live leader. Allocation failure returns
`RESOURCE_EXHAUSTED` with exact state preservation and retry at the same index.

When an exact-retained application, joint-membership, or final-membership retry finds its matching
uncommitted entry in an earlier term, it invokes this progress operation. The retry never appends a
second logical command. Repeated delivery after the no-op is retained is empty-success and does not
grow the log.

Metadata and tablet application owners treat every committed type-253 entry as an ordered internal
no-op. They advance and durably persist the applied index but do not decode it as an application
command or change configuration. Tablet snapshot compaction covers the index without serializing an
application mutation. Membership checkpoints continue to derive only from types `254` and `255`.

## Detailed rationale

Raft permits a leader to advance commit by counting replicas only for an entry from its current
term. Once the empty no-op commits, every preceding retained entry is also committed by log order.
Reserving a distinct type prevents an empty application payload from acquiring subsystem-specific
meaning and lets every application owner handle the boundary uniformly.

The operation does not append a no-op automatically on every election. It is explicit and is also
triggered by prior-term exact replay, keeping log growth tied to a current need. Reusing any existing
current-term entry avoids redundant progress records while preserving the same Raft argument.

## Alternatives considered

- Append the logical command again was rejected because both copies could commit and apply.
- Mark a prior-term entry committed solely from its replication count was rejected because that
  violates Raft's current-term commit rule.
- Encode progress as an empty metadata or tablet command was rejected because application entry
  types have different codecs and ownership.
- Append a no-op on every leadership acquisition was deferred because the present requirement is
  bounded retry progress, not a general election policy.

## Consequences

Prior-term exact reconfiguration retries can progress without duplicate application. One durable
entry and one replication round may be added in the new leader term. Empty successful retries after
retention do not add persistence or outbound messages. Progress still depends on the applicable
stable or joint voter quorum and does not prove application completion.

Every application owner must recognize the internal no-op before type-specific decoding. A missing
handler fails closed rather than silently interpreting it as application data.

## Affected invariants

Invariants 4, 5, 8, 9, 10, 11, 13, 14, and 18 apply. The durable record is bounded and semantically
validated, exact logical actions are not duplicated, commit advancement follows the current-term
Raft rule, and application indexes advance only after ordered handling succeeds.

## Validation plan

Node tests cover exact application and joint-membership retries across a term change, repeated no-op
suppression, reserved proposal rejection, and malformed durable no-op rejection. Metadata and tablet
state-machine tests commit the internal entry, advance their durable applied boundary, and ensure
the next application command remains correctly ordered. Allocation sweeps cover a three-voter
replication batch and single-voter immediate commit, failing every prospective-node, progress,
commit, message, and returned-state allocation before exact retry. Full Raft and ingest suites remain
required.

## Migration or rollback considerations

The multiplexed Raft record envelope version does not change because its existing entry type and
payload fields already encode the new value. Persisting type `253` does create a semantic rollback
boundary: a binary that predates this ADR does not recognize the entry in application replay and
must not be used to reopen a group whose retained log or snapshot-covered history includes it.
Operators must upgrade all potential application owners before allowing the new operation and may
roll back only to a binary that implements type-253 ordered no-op handling.

## Unresolved questions

Automatic election-time no-ops, authenticated remote duplicate-delivery handling, metadata
application-completion reconciliation, and safe ledger reclamation remain outside this decision.

## References

- [Multiplexed Raft Persistent-State Record v1](../formats/multiplexed-raft-log-v1.md)
- [Raft Membership Command v1](../formats/raft-membership-command-v1.md)
- [Joint-consensus membership learning guide](../learning/joint-consensus-membership.md)
- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)
