# ADR 0136: Idempotent retained reconfiguration action replay

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** ChronosDB distributed-systems and metadata maintainers
- **Extends:** [ADR 0075](0075-durable-metadata-raft-commands.md),
  [ADR 0076](0076-joint-consensus-raft-membership.md), and
  [ADR 0135](0135-bounded-asynchronous-prepared-reconfiguration-admission.md)
- **Extended by:** [ADR 0137](0137-current-term-raft-progress-noop.md)

## Context

A ledger-prepared action can be submitted, retained in the Raft log, and then retried before its
commit or application becomes visible to reconciliation. Joint membership state already derives
from retained entries, but repeating the same begin/finalize call returned an invalid-operation
error. Metadata placement reconciliation sees only applied metadata, so repeating its exact
`ProposeOperation` could append the same canonical placement command more than once. The second copy
would later violate the strictly increasing placement-epoch contract during application.

## Decision

Exact membership retries are idempotent while the local node is leader. Repeating a begin request
whose sorted new voter set exactly matches the retained joint command, or repeating finalize while
the exact final command is pending, returns an empty successful transition when that entry is
committed or belongs to the current leader term. Different membership intent remains invalid.

`ProposeExactRetainedOperation` is an explicit internal durable-runtime operation for canonical
commands whose logical identity is contained in their exact payload. `RaftNode` validates the same
type/size/reserved-membership constraints as a normal proposal, then exact-compares retained log
entry type and payload. A committed or current-term exact match returns an empty transition;
otherwise the command is proposed normally.

Prepared tablet reconfiguration execution converts only its ledger-validated placement
`ProposeOperation` into `ProposeExactRetainedOperation` after decoding/preparation. Tablet
Reconfiguration Action v1 remains unchanged and continues to encode the original proposal. Both
synchronous execution and asynchronous admission use the converted request.

An exact uncommitted match from an earlier term returns `UNAVAILABLE` without appending. Repeating
the application command could eventually commit both copies. Progress for such an entry requires a
separately specified current-term Raft no-op or another safe current-term command; this decision does
not invent one.

## Detailed rationale

Canonical placement bytes include tablet identity and the next placement epoch, so exact equality
denotes the same logical command rather than a legitimate repeated mutation. Membership commands
likewise include exact old/new configuration identity. Suppressing only committed or current-term
entries preserves ordinary current-term Raft commit progress while refusing the unsafe prior-term
case.

## Alternatives considered

- Always append a retry was rejected because duplicate placement application violates monotonic
  epoch semantics.
- Suppress any exact retained entry regardless of term was rejected because an old-term uncommitted
  entry may never become commit-eligible without a current-term entry.
- Treat every generic proposal payload as idempotent was rejected because identical application
  bytes may represent intentional repeated operations outside this control-plane contract.
- Change Tablet Reconfiguration Action v1 was rejected because retry semantics are an execution
  policy and the existing durable action bytes already identify the exact command.

## Consequences

Same-process, restart, and asynchronous redelivery of exact current-term reconfiguration work do not
grow the log or advance the physical durability sequence. Empty successful transitions carry no
outbound messages; normal timers/transport continue replication of the retained entry. Callers must
still inspect the operation status and reconcile authoritative state.

Prior-term uncommitted exact actions now fail safely but may remain unavailable until a current-term
commit-progress mechanism exists. Divergent pending metadata commands, remote authentication,
application completion, and ledger reclamation remain outside this boundary.

## Affected invariants

Invariants 4, 5, 8, 9, 10, 11, 13, 14, and 18 apply. Exact logical commands are not applied twice,
membership intent cannot silently change, durable sequence advances only for a new transition, and
old-term ambiguity fails without mutating the log.

## Validation plan

Node tests cover exact current-term proposal retry, prior-term uncommitted refusal, exact joint and
final membership retry, and divergent membership rejection. Durable-runtime coverage proves an
exact retry adds no persistence or outbound transition and does not advance the durable sequence. A
real-filesystem reconfiguration test prepares a placement action, executes it twice, and proves one
retained metadata entry and one synchronization boundary. Full Raft suites remain required.

## Migration or rollback considerations

No durable or wire bytes change. The new operation is an in-process variant used only after action
preparation. Older binaries can recover every resulting log because only the established metadata
command entry is persisted.

## Unresolved questions

The Raft current-term no-op contract, divergent pending-command diagnostics, authenticated remote
deduplication, metadata application completion, and safe evidence reclamation remain Phase 16 work.

## References

- [Metadata Command v1](../formats/metadata-command-v1.md)
- [Raft Membership Command v1](../formats/raft-membership-command-v1.md)
- [Joint-consensus membership learning guide](../learning/joint-consensus-membership.md)
- [Tablet reconfiguration learning guide](../learning/tablet-reconfiguration.md)

## Retrospective (2026-08-10)

[ADR 0137](0137-current-term-raft-progress-noop.md) adds the separately specified safe progress
mechanism left unresolved here. A prior-term exact match now appends, or reuses, one empty
current-term internal no-op instead of returning `UNAVAILABLE`; it still never appends a second copy
of the logical command. The original refusal remains the historical behavior of this decision
before ADR 0137.
