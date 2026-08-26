# ADR 0525: Authenticated complete grouped shuffle result stream

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB distributed-query and networking maintainers
- **Extends:** [ADR 0524](0524-proof-bound-grouped-shuffle-result-frame.md)

## Context

One valid reduced-partition frame is not a complete result. A coordinator must reject missing,
duplicated, reordered, cross-partition, or post-terminal frames and must not expose an accepted
prefix. It must also bind the transport-authenticated principal to the authority destination node.

## Decision

Add single-thread-affine result-stream sender and receiver owners. The sender constructs every frame
before exposing bytes, assigns contiguous sequence from one, marks only the last frame terminal,
and emits one empty terminal for an empty partition. Frame count and total encoded bytes are finite.

The receiver requires an already authenticated peer. Its first frame calls the borrowed node
principal authorizer for the claimed result source and locks the stream to that source and
partition. Every later frame must have the next exact sequence. Input ending before terminal,
bytes after terminal, source or partition drift, authority drift, and unauthorized claims fail the
whole owner and discard retained batches. A complete product transfers exactly once only after
terminal closure. The authority, raw schema, and authorizer outlive the receiver.

This layer does not itself perform TLS or TCP. It is the authenticated application-stream boundary
consumed by the forthcoming connected session.

## Consequences

Reduced partition bytes now have all-or-none stream ownership and source-principal binding. Empty
partitions, fragmented input, and short writes share one canonical path. Mutual-TLS connection
ownership, request authorization, retry, and integration with remote destination processes remain.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): every retained frame belongs to one query, source,
  coordinator, and partition authority.
- [Invariant 10](../architecture/invariants.md): authenticated principal authorization precedes
  publication of any claimed source stream.
- [Invariant 11](../architecture/invariants.md): sender bytes and receiver batches have explicit
  single-owner lifetimes and one-shot transfer.
- [Invariant 15](../architecture/invariants.md): frame count, frame bytes, and total stream bytes are
  bounded.
- [Invariant 18](../architecture/invariants.md): only contiguous terminal-closed partition results
  can enter later global finalization.

## Validation plan

Drive two result batches through fragmented writes into one authorized receiver and compare exact
complete bytes. Reject unauthenticated peers, wrong first sequence, incomplete input, terminal
suffix, and total-byte exhaustion. Prove the canonical empty terminal and deterministic allocation
classification for sender construction, receiver construction, and atomic retention. Run cluster,
allocation-failure, sanitizer, formatting, static-analysis, and diff gates.

## Migration or rollback considerations

No existing format changes. Rollback removes only the new owners; callers must not expose frame
prefixes without an equivalent closure and authentication boundary.

## Unresolved questions

- Define the mutual-TLS connected request/response session.
- Add result-return TCP client/server and finite exact retry.
- Rehydrate complete remote streams into global shuffle gathering.

## References

- [Result format](../formats/distributed-vector-grouped-aggregate-shuffle-result-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
