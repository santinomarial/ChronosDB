# ADR 0419: Packaged native QUORUM_SYNC client command

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** ChronosDB service, native-client, security, and operations maintainers
- **Extends:** [ADR 0418](0418-owning-native-client-tls-routes.md)

## Context

The native-client stack could securely own an authenticated route and TLS bundle, preserve one
exact QUORUM_SYNC request across bounded redirects, and drive its TCP/TLS execution to a terminal
receipt. Operators still had no packaged entry point. An ad hoc wrapper could accept ambiguous
row input, omit a whole-operation deadline, trust a redirect outside the configured authority, or
report transport progress as durable success.

## Decision

`chronosctl quorum-sync` performs exactly one Protocol 2 QUORUM_SYNC operation. The caller must
provide a non-nil lowercase canonical group UUID, positive canonical initial node and minimum
placement epoch, the strict native route file, client certificate, private key, trust store, one
already-encoded canonical Columnar Append v1 payload file, and a timeout from 1 millisecond through
one hour. Options may appear in any order but cannot be repeated. A fixed maximum of eight
authenticated redirects bounds replay.

The command opens the append file with `O_NOFOLLOW`, requires a nonempty regular file within the
Columnar Append v1 maximum, reads its exact descriptor length, rejects observed truncation or
growth, and exact-decodes the complete payload before loading routes or opening a socket. It then
loads the all-or-nothing TLS route owner and copies its stable route records into one operation
whose authority/context pointers remain borrowed from that owner until execution destruction.

Exit `0` means the client received and validated one canonical `APPLIED` or `MATCHING_RETRY`
QUORUM_SYNC receipt. Text output is one stable field line; `--json` emits one JSON object with the
receipt and attempt/redirect counts. Option errors use exit `2`, no stdout, and usage on stderr.
File, security, protocol, transport, deadline, server, and allocation failures use exit `1`, no
success output, and one status on stderr. No transport ambiguity is retried; only authenticated
Protocol 2 redirects can select another configured route.

## Detailed rationale

Accepting the existing canonical application payload preserves its embedded client/batch identity,
schema/tablet identity, digest, and exact replay bytes. Defining a second row or SQL input language
would add an unnecessary encoding contract. A finite caller-visible deadline prevents an operator
command from waiting forever, while the existing phase deadlines remain defense in depth.

## Alternatives considered

- Accept JSON rows or SQL and encode them in the tool. Rejected because schema resolution and a new
  input compatibility surface are outside this client-composition task.
- Accept a preframed native request. Rejected because it would duplicate session request IDs and
  durability negotiation instead of letting the protocol owner construct them.
- Retry ambiguous connection failures. Rejected because the retained client/batch identity makes a
  later explicit invocation safe, but a carrier cannot prove whether an interrupted attempt
  committed.
- Defer the packaged command. Rejected because all required bounded owners now exist and the absent
  composition is the documented client gap.

## Consequences

Operators and automation can invoke the implemented replicated durability path without embedding
C++ when the target embeds the mutual-TLS native server. They must generate a canonical Columnar
Append v1 payload and maintain protected route/TLS files. The packaged `chronosd` remains plaintext
loopback-only and is not yet a compatible target. The command is synchronous and handles one
operation; packaged native-server TLS, batching, schema lookup, credential reload, cancellation
output, DNS routes, and query routing remain outside this decision.

The packaged-server gap is resolved by [ADR 0420](0420-packaged-native-mutual-tls-server.md).

## Affected invariants

- [Invariant 5](../architecture/invariants.md): the caller names its minimum placement authority,
  while only an authenticated server redirect can change the destination.
- [Invariant 6](../architecture/invariants.md): the exact canonical append and its embedded digest
  are retained through every attempt.
- [Invariant 9](../architecture/invariants.md): exit `0` is emitted only after the existing exact
  quorum-sync receipt proof boundary completes.
- [Invariant 11](../architecture/invariants.md): the route/TLS owner outlives every borrowed context
  and authority pointer in the operation.
- [Invariant 14](../architecture/invariants.md): the command composes existing Protocol 2 and
  Columnar Append v1 bytes without changing either format.
- [Invariant 18](../architecture/invariants.md): strict file qualification, mTLS identity, bounded
  routes, and authenticated redirects fail closed.

## Validation plan

CLI process tests cover help, legacy version JSON, required and duplicate options, canonical UUID
and decimal rules, the deadline cap, unknown options, exit/output contracts, and pre-network exact
append rejection. Existing focused integration tests cover real mutual TLS, certificate/node
authority, redirect replay of identical bytes, exact receipt validation, deadlines, and terminal
execution scheduling. Formatting, warnings-as-errors, clang-tidy, ASan/UBSan, and the full serialized
suite are required before completion.

## Migration or rollback considerations

This is an additive command. Rollback removes `quorum-sync` while preserving `chronosctl version`;
no server, route-file, durable, or wire state changes.

## Unresolved questions

Human-readable row construction and schema discovery require a separate client API and compatibility
decision. Signal-aware cancellation diagnostics and multi-operation scheduling can be added without
changing this one-operation contract.

## References

- [Native client route configuration](../operations/native-client-route-config.md)
- [Columnar Append v1 and WAL application payload](0015-columnar-batch-v1-and-wal-append-command.md)
- [Exact native QUORUM_SYNC redirect replay](0414-exact-native-quorum-ingest-redirect-replay.md)
- [Bounded native QUORUM_SYNC TCP execution](0416-bounded-native-quorum-ingest-tcp-execution.md)
