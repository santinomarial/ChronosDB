# ADR 0409: Source-tagged Native Subscription Changes

- **Status:** accepted
- **Date:** 2026-08-15
- **Owners:** ChronosDB network, live-query, and distributed-systems maintainers
- **Extends:** [ADR 0060](0060-native-protocol-v1-framing.md),
  [ADR 0094](0094-native-protocol-1-1-subscriptions.md),
  [ADR 0072](0072-explicit-wal-and-raft-commit-identities.md), and
  [ADR 0407](0407-source-tagged-resume-token-v2.md)

## Context

Protocol 1.1 freezes `SUBSCRIPTION_CHANGE` payload format 1 with a zero byte at offset 3 and an
untagged 16-byte WAL identity. Resume Token v2, Checkpoint v2, and the logical coordinator now
preserve either a WAL sequence or a Raft group/log index. Reinterpreting a group UUID as a WAL ID
would alias distinct source namespaces, while changing format 1 would invalidate accepted bytes.

Protocol 2.0 inherited the Protocol 1.1 subscription payload. Its independently accepted ingest
and redirect bytes cannot be changed as a side effect of Phase 11 subscription work.

## Accepted decision

Native Protocol 1.2 retains the 40-byte frame and subscription feature bit 0. It assigns
`SUBSCRIPTION_CHANGE` payload format 2. The envelope remains 84 bytes: offset 3, which format 1
requires to be zero, carries source kind `1` for WAL or `2` for Raft. The existing 16-byte identity
then carries the selected WAL ID or Raft group UUID, and the existing u64 carries the WAL record
sequence or Raft log index. Every other subscription payload remains format 1.

A negotiated 1.2 connection emits and accepts format 2 for every change, including WAL changes.
Protocol 1.1 and Protocol 2.0 continue to emit and accept only format 1 and reject Raft positions.
Decoders select the one allowed payload format from immutable negotiated connection context; they
do not auto-detect or infer a source kind. Unknown kinds, cross-version payloads, nil identities,
and zero positions fail closed.

The subscription service retains the request's negotiated protocol context for its complete
lifetime, applies the connection-specific payload ceiling, and emits response tasks with that same
context. A Raft-backed source set is rejected before registration or resume mutation unless the
connection negotiated 1.2.

## Consequences and alternatives

Source-tagging adds no per-change bytes and preserves all format-1 offsets after byte 3. Existing
1.1 clients remain byte compatible for WAL subscriptions. A client needing source-tagged delivery
negotiates Protocol 1.2; simultaneous Protocol 2 ingest extensions and tagged subscription changes
would require a separately accepted Protocol 2 minor extension.

Reusing format 1's reserved byte without changing its payload format was rejected because old
decoders require zero there. Inferring the source from placement metadata was rejected because a
change must be self-describing and replay-stable. Accepting format 1 on a 1.2 connection was
rejected because it would make the selected minor ambiguous and permit downgrade-by-payload.

## Affected invariants and validation

Invariants 4, 10, 12, 14, and 17 apply. Tests preserve Protocol 1.1 WAL behavior, round-trip exact
1.2 WAL and Raft tags without changing the envelope size, reject cross-version payloads and unknown
kinds, negotiate 1.2 through server and client state machines, preserve Raft group bytes through the
live bridge, and prove the service emits under retained negotiated context. Raft-backed historical
snapshot execution and physical prefix reclamation remain separate follow-up work.

## Retrospective note (2026-08-15)

[ADR 0410](0410-raft-subscription-snapshot-and-prefix-reclamation.md) completes those historical
snapshot and physical reclamation follow-ups without changing the Protocol 1.2 bytes accepted here.
