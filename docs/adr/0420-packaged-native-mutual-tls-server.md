# ADR 0420: Packaged native mutual-TLS server

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** ChronosDB service, native-server, security, and operations maintainers
- **Extends:** [ADR 0145](0145-bounded-epoll-mutual-tls-admission.md) and
  [ADR 0419](0419-packaged-native-quorum-sync-client.md)

## Context

The epoll reactor already implemented fail-closed mutual TLS and certificate-principal admission,
and `chronosctl quorum-sync` already owned a strict mutual-TLS client route bundle. Packaged
`chronosd` exposed only plaintext loopback, leaving the packaged client unable to target the
packaged server. Reusing Raft peer identity would incorrectly grant node and source-address
authority to client certificates.

## Decision

`chronosd` accepts an atomic native security bundle: `--native-client-principals`,
`--native-tls-cert`, `--native-tls-key`, and `--native-tls-ca`. The first file uses the strict
`CHRONOSDB_NATIVE_SERVER_PRINCIPALS_V1` format and maps unique verified client leaf-certificate
SHA-256 fingerprints to strictly increasing positive principal IDs. It grants coarse protocol
admission only and carries no node, group, leadership, placement, table, operation, or source-IP
authority.

The bundle requires epoll and configures `TLS_REQUIRED` with mandatory client certificates. Its
immutable authority owner outlives the reactor that borrows it. With TLS, `--listen` may name one
canonical nonzero IPv4 address. Without the complete bundle, plaintext remains restricted to IPv4
loopback. Partial bundles, TLS with io_uring, noncanonical addresses, unknown certificates, and
invalid credentials fail closed without protocol dispatch.

The daemon opens final paths with `O_NOFOLLOW`, bounds regular-file size, rejects group/other writes
to the authority, certificate, and trust store, and requires the key to have no group/other access.
It reports `native_transport=tls|plaintext` only after reactor startup. Configuration and
credential reload remain restart operations.

## Alternatives considered

- Reuse replicated peer configuration. Rejected because peer certificates bind node identity and
  source address, while native clients are not cluster nodes.
- Infer allowed clients from the trust store. Rejected because chain trust alone does not provide a
  stable application principal or a bounded explicit leaf allowlist.
- Permit partial TLS configuration or opportunistic TLS. Rejected because downgrade and ambiguous
  startup behavior violate the security boundary.
- Add TLS to io_uring in this task. Rejected because its reactor has no implemented TLS session
  owner; it continues to return an explicit unsupported configuration.

## Consequences

The packaged `chronosctl quorum-sync` can target packaged `chronosd` through mutual TLS. Operators
must maintain a distinct native-client allowlist and protect credential parent directories. The
principal is available to the established connection state but does not yet implement per-table or
per-operation RBAC. IPv6, DNS bind addresses, live reload, certificate revocation services, and
io_uring TLS remain outside this decision.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): client admission does not confer placement or leader
  authority.
- [Invariant 6](../architecture/invariants.md): TLS packaging does not alter canonical append bytes
  or retry identity.
- [Invariant 9](../architecture/invariants.md): durable acknowledgements remain owned by the
  existing QUORUM_SYNC proof boundary.
- [Invariant 11](../architecture/invariants.md): the authority owner outlives every reactor borrow.
- [Invariant 14](../architecture/invariants.md): no native wire or durable format changes.
- [Invariant 18](../architecture/invariants.md): file qualification, mandatory mutual TLS, and
  explicit leaf authority fail closed.

## Validation plan

Parser and authority tests cover canonical input, hostile ambiguity, bounds, construction,
unverified and unknown certificates, stable principals, and concurrent immutable reads. CLI tests
cover the atomic bundle, plaintext bind restriction, canonical IPv4, epoll-only TLS, help, and
diagnostic contracts. The Linux process test provisions a real replicated root, starts packaged
`chronosd` with mutual TLS, invokes packaged `chronosctl`, and requires `APPLIED` then
`MATCHING_RETRY` for one exact append. Formatting, warnings-as-errors, clang-tidy, ASan/UBSan, and
the full serialized suite remain release gates.

## Migration or rollback considerations

The options and authority format are additive. Existing loopback plaintext deployments keep their
default behavior and gain only the `native_transport=plaintext` startup field. Rollback removes the
new bundle and returns packaged remote serving to unavailable; it changes no durable or wire state.

## Unresolved questions

Fine-grained authorization needs a separate policy model. IPv6/DNS binding, live credential reload,
revocation, and io_uring TLS require independent ownership and operational decisions.

## References

- [Native server principal configuration](../operations/native-server-principal-config.md)
- [Native client route configuration](../operations/native-client-route-config.md)
- [Native server operations baseline](../operations/native-server.md)
