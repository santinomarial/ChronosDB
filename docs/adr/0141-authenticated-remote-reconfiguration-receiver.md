# ADR 0141: Authenticated remote reconfiguration receiver

- **Status:** accepted
- **Date:** 2026-08-10
- **Owners:** cluster integration, networking, and Raft subsystems
- **Supersedes:** none

## Context

Locally reconciled tablet actions are durably prepared and asynchronously executable, but routing
the same intent to another node needs an explicit internal wire contract and a receiver that cannot
be tricked by a claimed source, stale leader observation, foreign tablet, or wrong Raft group. The
Raft core must remain transport-agnostic, and ADR 0066 still forbids remote plaintext while the
network stack has no maintained TLS record backend.

## Decision

`chronos_cluster` composes the existing network authentication and Raft durability boundaries. A
versioned, bounded, checksummed Remote Tablet Reconfiguration Request v1 binds one canonical action
to nonzero source and target node IDs and an exact required leader term.

The receiver is configured for one tablet, its tablet group, the metadata group, the local node,
one tablet-bound action ledger, one asynchronous Multi-Raft owner, and a borrowed embedding-owned
principal authorizer. It requires an authorized nonzero `PeerAuthenticationResult`, authorizes the
principal against the claimed source node, and exact-validates target/tablet/action-kind/group
binding before durable preparation. Unauthorized work cannot mutate the ledger or Raft.

After preparation, admission copies the exact retained action into the bounded asynchronous owner
with ADR 0140's leader-term fence. Queue rejection leaves immutable preparation evidence. A stale
term or follower produces a normal per-operation `UNAVAILABLE` result with no Raft transition.
Successful completion retains local persist-and-sync semantics only.

This decision implements receiver-side authenticated admission, not a TLS/socket carrier. Remote
listeners must use `TLS_REQUIRED` once a maintained backend exists; loopback development can use a
custom nonzero principal. No code may describe the missing carrier as implemented.

## Detailed rationale

A separate composition target keeps deterministic Raft free of network policy and keeps the native
client protocol free of internal cluster operations. Layered CRCs allow safe framing and preserve
the independently versioned action contract. Per-tablet receiver configuration makes authorization
and group binding explicit without inventing a global mutable registry.

## Alternatives considered

- **Add the request to Native Protocol v1:** rejected because client negotiation and internal
  cluster authorization have different trust and compatibility domains.
- **Trust a source node field after TLS:** rejected because certificate/principal-to-node
  authorization remains necessary above transport authentication.
- **Prepare after the Raft operation completes:** rejected because a crash could lose the durable
  retry identity after execution.
- **Put network types into `chronos_raft`:** rejected to preserve the transport-agnostic consensus
  boundary.

## Consequences

An authenticated carrier now has a complete safe receiving endpoint and canonical bytes to
deliver. Duplicate delivery is ledger-idempotent and exact retained replay does not duplicate the
action. The embedding must own receiver/ledger/runtime lifetimes and authorization synchronization.
TLS transport, sender response correlation, retry/backoff, leader refresh, and connection-level
overload integration remain required.

## Affected invariants

Invariants 1, 4, 5, 8, 9, 10, 14, and 18 apply. The receiver preserves prepare-before-execute,
single-owner ordering, uncommitted invisibility, exact retries, layered integrity, explicit wire
versioning, and fail-closed stale routing.

## Validation plan

Focused codec tests round-trip exact bytes and reject damage and invalid route identities. A real
filesystem/runtime test rejects anonymous and source-mismatched principals before ledger mutation,
durably prepares and executes an authorized request, suppresses an exact duplicate, and proves a
stale-term retry adds no log entry. Route tests reject wrong node, tablet, and group before
preparation. TLS carrier, partial socket delivery, disconnect, response, and multi-process fault
matrices remain deferred until that carrier is implemented.

## Migration or rollback considerations

There is no deployed cluster protocol. Version 1.0 is exact and unsupported versions fail closed.
Rolling mixed-version transport must negotiate support before sending this envelope; otherwise the
sender must keep its prepared action and retry through a compatible leader.

## Unresolved questions

TLS provider and certificate identity policy, sender-side leader refresh and retry deadlines,
response envelope/correlation, completion-to-metadata application scheduling, and evidence
reclamation remain Phase 16 work.

## References

- [Remote Tablet Reconfiguration Request v1](../protocol/remote-tablet-reconfiguration-v1.md)
- [ADR 0066](0066-authentication-and-tls-integration-boundary.md)
- [ADR 0135](0135-bounded-asynchronous-prepared-reconfiguration-admission.md)
- [ADR 0140](0140-atomic-current-leader-term-admission.md)
- [Architecture invariants](../architecture/invariants.md)

## Retrospective (2026-08-11)

[ADR 0142](0142-bounded-remote-reconfiguration-retry.md) supplies the exact response correlation,
advisory leader hint, and finite sender retry behavior left unresolved here. TLS transport and the
receiver completion-to-response service adapter remain separate.
