# ADR 0143: Nonblocking reconfiguration response publication

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** cluster integration and asynchronous Raft runtime subsystems
- **Supersedes:** none

## Context

The receiver returns an owning asynchronous durable completion, while the internal response format
and sender retry state machine consume wire bytes. A carrier-facing adapter must connect those
boundaries without blocking a reactor, losing the original route identity, publishing before local
sync, or allowing the completion to be consumed twice.

## Decision

`RemoteTabletReconfigurationAdmission` retains the exact request source node, target node, required
leader term, action identity, immutable-ledger duplicate flag, and owning durable completion.
`try_finish_remote_tablet_reconfiguration_admission` is a single-owner nonblocking poll operation.

Before readiness it returns an empty optional and changes nothing. Once ready it consumes the
completion exactly once, requires exactly one durable operation result, and encodes a canonical
Remote Tablet Reconfiguration Response v1 with source/target reversed for the return path. A
top-level worker failure uses its exact status code; otherwise the response uses the operation's
status. The admission is terminal after consumption even if response allocation/encoding fails.

An optional leader hint may be supplied only by the caller from a separately ordered authoritative
observation. The adapter does not inspect worker state or fabricate a hint. An `OK` response retains
receiver-local persist-and-sync semantics and is not commit/application evidence.

## Detailed rationale

Keeping correlation fields beside the completion avoids reparsing or retaining borrowed request
bytes across asynchronous execution. Poll-before-wait composes with a reactor continuation while
the completion's existing acquire edge preserves result publication. Exact-once response finishing
prevents two transport paths from racing to consume one result.

## Alternatives considered

- **Block in the receiver:** rejected because disk synchronization cannot run on a reactor owner.
- **Reparse the original request after completion:** rejected because it requires retaining another
  potentially large byte buffer and creates a second validation path.
- **Return `OK` immediately after queue admission:** rejected because it would precede durable
  execution and expose outbound work too early.
- **Read a leader hint directly from the worker:** rejected because it would violate exclusive
  ownership; ordered observations already provide the correct boundary.

## Consequences

The receiver now has a complete nonblocking request-to-local-response lifecycle for an embedding or
future carrier. Callers must serialize access to each admission and retain it until response bytes
are accepted by the carrier. Pre-admission authentication/decoding errors, socket write ownership,
timeouts, and disconnect cleanup remain carrier/service responsibilities.

## Affected invariants

Invariants 1, 4, 5, 10, 14, and 18 apply. Response publication follows the existing local sync edge,
preserves one-operation ordering and local-versus-commit semantics, and emits only the frozen
checksummed response format.

## Validation plan

Focused real-filesystem/runtime tests drive authorized, duplicate, and stale-term admissions
through the nonblocking adapter. They verify exact reversed route correlation, action/term identity,
local status, duplicate flag, optional separately supplied hint, and rejection of a second finish.
ASan/UBSan cover the same paths. Reactor wakeup, partial socket writes, disconnect races, and
multi-process failures remain tied to the future carrier.

## Migration or rollback considerations

No format change is introduced. The adapter emits Response v1 and can be removed without changing
durable ledger or Raft bytes. A mixed-version carrier still negotiates the request/response formats
before admission.

## Unresolved questions

Maintained TLS transport, connection request multiplexing, pre-admission error policy, response
write deadlines, disconnect retry ownership, and automatic authoritative reconciliation remain.

## References

- [Response v1](../protocol/remote-tablet-reconfiguration-response-v1.md)
- [ADR 0114](0114-bounded-asynchronous-multi-raft-owner.md)
- [ADR 0141](0141-authenticated-remote-reconfiguration-receiver.md)
- [ADR 0142](0142-bounded-remote-reconfiguration-retry.md)
- [Architecture invariants](../architecture/invariants.md)
