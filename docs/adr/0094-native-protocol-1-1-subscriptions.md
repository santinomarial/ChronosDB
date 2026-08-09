# ADR 0094: Native Protocol 1.1 subscriptions

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB networking and live-query maintainers
- **Extends:** [ADR 0060](0060-native-protocol-v1-framing.md),
  [ADR 0061](0061-native-protocol-handshake-and-request-lifecycle.md), and
  [ADR 0068](0068-live-handoff-and-resume-token-v1.md)
- **Extended by:** [ADR 0095](0095-multi-tablet-subscription-delivery-order.md)

## Context

Native Protocol 1.0 deliberately assigned no subscription messages. The logical subscription
manager now has bounded snapshot handoff, retained replay, acknowledgement, cancellation, and
authenticated resume tokens, but exposing those operations by reusing query completion or adding
unnegotiated 1.0 types would break the frozen compatibility contract.

## Accepted decision

Protocol minor 1 assigns feature bit 0 to subscriptions and message types 23 through 28 to
`SUBSCRIBE_REQUEST`, `SUBSCRIPTION_READY`, `SUBSCRIPTION_CHANGE`, `SUBSCRIPTION_ACKNOWLEDGE`,
`SUBSCRIPTION_CHECKPOINT`, and `SUBSCRIPTION_END`. The 40-byte frame layout, checksums, limits, and
major version do not change. Hello frames retain minor 0 framing; the hello payload negotiates the
highest common minor and the intersection of requested and supported features. Every later frame
uses the selected minor exactly. Subscription types require selected minor 1 and the feature bit.

A new request carries either UTF-8 SQL plus a client-chosen subscription UUID or one opaque Resume
Token. Historical rows reuse self-describing `QUERY_RESULT` batches and end that finite snapshot
with `END_STREAM`. `SUBSCRIPTION_READY` follows with the initial safe token and begins live state.
Canonical changes carry delivery sequence, complete tablet/WAL source position, schema identity and
version, operation, result key, and payload. Deletes have no payload.

The client acknowledges a delivered sequence on the still-active request ID. The server accepts
only a nondecreasing acknowledgement no later than delivered state and returns a checkpoint token.
External effects remain at least once. Cancellation retains request ownership until an orderly
`SUBSCRIPTION_END` or `ERROR`, allowing the terminal frame to carry the last safe token. Bounded
outbound framing and reactor overload behavior are unchanged.

`SubscriptionManager` remains the logical authority. A thin live/network bridge encodes its
registration, immutable delivery records, accepted acknowledgements, and terminal token; the
reactor owns framing, partial I/O, and socket backpressure.

## Consequences and alternatives

Protocol 1.0 clients and default encoders continue to emit exactly minor 0 and cannot construct or
receive subscription types. A 1.1 peer may still use the 1.0 ingest/query families, but every
post-handshake frame identifies minor 1. Unknown features and subscription messages under minor 0
fail closed.

Using `QUERY_END` for an unbounded stream was rejected because it cannot represent acknowledgement
or resumable termination. Embedding a checkpoint token in every change was rejected because it
amplifies bytes and MAC work; explicit acknowledgements name the safe boundary. Treating socket
write completion as acknowledgement was rejected because it does not prove consumer processing.

## Affected invariants and validation

Invariants 10, 12, 14, 15, and 17 apply. Focused tests preserve the exact 1.0 golden frame, reject
minor-0 subscription frames, round-trip and corrupt every new bounded envelope, negotiate the
feature, enforce snapshot-ready-change-ack-checkpoint-end ordering on server and client state
machines, and bridge a real manager poll through checkpoint and termination. Real-socket partial
delivery/reconnect, hostile allocation sweeps, cross-version peers, sustained backpressure, and
disconnect races remain in the Phase 18 ledger.
