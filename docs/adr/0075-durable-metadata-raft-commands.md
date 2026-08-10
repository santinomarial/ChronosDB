# ADR 0075: Durable metadata Raft commands and recovery

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB catalog and distributed-systems maintainers
- **Extended by:** [ADR 0136](0136-idempotent-retained-reconfiguration-action-replay.md)

## Context

The metadata state machine could deterministically update nodes, schema identities, tablet
placements, leader hints, and retention, but callers supplied in-memory variants directly. No
stable command bytes connected it to the durable metadata Raft group, and restart could not rebuild
the state.

## Accepted decision

Logical Raft entry type 2 contains one exact, versioned, checksummed Metadata Command v1 value.
`DurableMetadataStateMachine` owns fresh deterministic metadata state for one configured group,
borrows `DurableMultiRaftRuntime`, pre-decodes committed entries, applies them in exact index order,
and persists the final applied index only after successful application.

Recovery replays the entire retained committed prefix even when Raft's persisted applied index is
already current, because catalog state itself is process memory. A nonzero Raft snapshot boundary is
rejected until a versioned metadata snapshot can restore the omitted prefix. Unknown command types,
corrupt bytes, invalid epochs or membership, resource limits, and out-of-order application fail the
owner closed. The current leader may compose applied metadata with its quorum-sync receipt.

## Consequences and alternatives

Placement epochs and catalog policy now have deterministic durable command identity and restart
behavior. Full-log replay and retained history are temporary costs. Encoding native variants or JSON
was rejected because neither supplies canonical durable layout, bounded parsing, or independent
integrity. Treating local metadata as last-writer-wins was rejected because it bypasses the single
committed cluster order.

This command carries schema identity/version, not a complete `TableSchema` definition; the durable
schema-definition catalog and application snapshot remain follow-up work. Membership changes use
the separate joint-consensus protocol from ADR 0076 rather than merely changing placement metadata;
orchestration between those two state machines remains follow-up work.

## Affected invariants and validation

Invariants 4, 8, 9, 10, 11, 13, 14, and 18 apply. Tests cover all command kinds, canonical replica
ordering, deterministic re-encoding, checksum/version/limit rejection, committed application,
applied quorum proof, and full reconstruction after durable log reopen. Golden fixtures, fuzzing,
application snapshots, catalog-definition persistence, placement-driven membership orchestration,
crash injection, and
large-catalog measurements remain required.
