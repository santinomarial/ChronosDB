# ADR 0423: Packaged loopback SQL client command

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB native-client, service, and operations maintainers

## Context

Configured single-node `chronosd` already executes CREATE TABLE, SQL INSERT VALUES, and supported
SELECT statements durably, but the packaged client exposes only version and replicated
`quorum-sync` commands. Users otherwise need to build a protocol client before they can exercise the
implemented single-node data plane.

## Decision

`chronosctl sql --host 127.0.0.1 --port PORT --execute "SQL"` executes exactly one statement. The
command composes the existing bounded Protocol v1 `NativeClientSession`, query/result codecs, and
plaintext IPv4 loopback server. It requires the literal loopback host and a canonical nonzero port,
uses a five-second timeout for each blocking socket send or receive, and validates the complete
handshake and response lifecycle before exit zero.

Results use a tab-separated text form with one header and canonical scalar decoding across every
result batch. Text control characters and backslashes are escaped, binary values use lowercase hex,
decimals preserve their declared scale, and NULL is explicit. Server `ERROR` frames and local
transport/protocol failures write a diagnostic to stderr and exit nonzero. Option errors retain the
existing exit-2 plus usage contract.

## Detailed rationale

Reusing the session owner prevents a CLI-only framing or state machine. Restricting this first SQL
surface to the daemon's existing plaintext boundary makes its security scope explicit while
delivering the smallest complete user-visible single-node workflow.

## Alternatives considered

- Add SQL to `quorum-sync`. Rejected because that command owns authenticated Protocol v2 replicated
  ingest of an already encoded canonical payload, not single-node query execution.
- Build a second ad hoc frame client. Rejected because it would duplicate negotiation, request IDs,
  buffering limits, and response validation.
- Add remote plaintext or TLS options now. Rejected because remote plaintext violates the protocol
  security boundary and TLS query routing needs a separately scoped authenticated client surface.

## Consequences

The implemented single-node CREATE/INSERT/SELECT/restart slice is directly runnable. Each command
opens one connection and buffers at most the existing session/frame bounds; it is not an interactive
shell. SQL INSERT still has no client retry identity, so an ambiguous transport failure cannot be
automatically replayed. Distributed routing, subscriptions, JSON/CSV modes, and TLS remain outside
this command.

## Affected invariants

- [Invariant 1](../architecture/invariants.md): exit zero for SQL INSERT follows the server's existing
  `LOCAL_SYNC` acknowledgement path; the client does not strengthen that durability mode.
- [Invariant 9](../architecture/invariants.md): the command does not retry ambiguous SQL INSERT.
- [Invariant 14](../architecture/invariants.md): it composes the accepted Protocol v1 codecs and
  negotiation without changing the wire format.

## Validation plan

A Linux real-process test starts the shipped daemon and invokes the shipped client for CREATE,
INSERT, SELECT, a server-side SQL error, restart with the same root, and recovered SELECT. CLI tests
cover help and strict option failures. Existing client-session, protocol, service, formatting,
static-analysis, and sanitizer checks remain required.

## Migration or rollback considerations

This is an additive command with no new durable or wire state. Rollback removes `chronosctl sql`
while every database root and Protocol v1 peer remains compatible.

## Unresolved questions

Authenticated remote SQL, caller-selected whole-operation deadlines, interactive input, and
additional output formats require separate user contracts.

## References

- [Native Protocol v1](../protocol/native-v1.md)
- [Native server operations](../operations/native-server.md)
- [ADR 0067](0067-bounded-native-client-session.md)
- [ADR 0224](0224-configured-single-node-chronosd.md)
- [ADR 0226](0226-native-sql-insert-dispatch.md)
