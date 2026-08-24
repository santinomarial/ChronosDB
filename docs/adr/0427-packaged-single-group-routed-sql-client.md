# ADR 0427: Packaged single-group routed SQL client

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, native-client, query, security, and operations maintainers
- **Extends:** [ADR 0426](0426-bounded-native-query-tcp-execution.md)

## Context

The native finite-query stack could retain exact SQL, follow an authenticated Protocol 2 redirect,
withhold bounded results through `QUERY_END`, and drive the complete operation under one deadline.
Operators still had no packaged entry point. Reusing the plaintext loopback command would conceal
the group and placement authority required by the redirect policy, while calling a general command
"distributed SQL" would overstate the still-absent multi-group remote-fragment execution.

## Decision

`chronosctl routed-sql` executes exactly one finite query whose redirect authority is one explicitly
named Raft group. The caller supplies a non-nil lowercase canonical group UUID, positive canonical
initial node and minimum placement epoch, the strict native route file, client certificate, private
key, trust store, nonempty exact SQL, and a timeout from 1 millisecond through one hour. Options may
appear in any order and cannot repeat. At most eight authenticated redirects are accepted.

The command loads one all-or-nothing `NativeClientTlsRouteOwner`, copies its stable leader routes,
and keeps that owner alive while `NativeQueryTcpExecution` borrows its TLS contexts and authority.
Only the execution owner drives sockets. No result bytes are written to stdout until the complete
operation has accepted `QUERY_END`. The command then decodes every retained canonical batch again,
requires one unchanged schema, and prints escaped tab-separated column names and rows.

Exit `0` means the complete result stream was validated and printed. Option failures use exit `2`,
no stdout, and usage on stderr. Route, TLS, authorization, transport, redirect, deadline, protocol,
server, result-limit, allocation, decoding, and output failures use exit `1` and never print a
success record. Ambiguous transport failures are terminal; only an authenticated redirect starts a
fresh connection and request ID.

## Consequences

The exact finite-query client composition is now available without embedding C++. The command name
and required group make its boundary explicit: it can follow a server redirect only when the whole
query has exactly that one authority. It does not infer a group from SQL, discover placement, merge
multiple groups, retry partial output, or provide remote mutable-tablet fragments. The packaged
server must separately prove a plan is single-group before emitting a redirect.

Memory remains bounded by the route/session configuration and the query owner's aggregate batch,
row, and payload ceilings. One command thread owns all state, so no inter-thread memory-ordering
argument applies. No durable or wire format changes.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): an authenticated exact-group observation is the only
  authority that can change the destination.
- [Invariant 6](../architecture/invariants.md): exact SQL and complete result ownership survive each
  fresh attempt, and stdout remains empty before terminal validation.
- [Invariant 11](../architecture/invariants.md): the route/TLS owner outlives all borrowed pointers;
  execution owns and releases every carrier attempt.
- [Invariant 14](../architecture/invariants.md): the command composes existing Protocol 2 frames and
  canonical result batches without changing them.
- [Invariant 18](../architecture/invariants.md): authentication, finite redirects, phase and whole-
  operation deadlines, result bounds, and sticky failure remain mandatory.

## Validation

CLI process tests cover help, required, duplicate, malformed, excessive-timeout, and unknown-option
contracts. A real process test launches the actual command against two loopback mutual-TLS servers
with distinct authorized node certificates, proves exact SQL and request ID one on both fresh
sessions across one redirect, and verifies that only the terminal tab-separated result is printed.
Formatting, warnings-as-errors, static analysis where supported, ASan/UBSan, the full serialized
network suite, and installed consumption remain required gates.

## Migration and rollback

This is an additive command. Rollback removes `routed-sql` while preserving the plaintext `sql`
and replicated `quorum-sync` commands. No server, route-file, durable, or protocol state changes.

## References

- [Native client route configuration](../operations/native-client-route-config.md)
- [Exact native finite-query redirect replay](0424-exact-native-query-redirect-replay.md)
- [Deadline-bound native finite-query TCP client](0425-deadline-bound-native-query-tcp-client.md)
- [Native Protocol v2](../protocol/native-v2.md)
