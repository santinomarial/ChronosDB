# ADR 0421: Descriptor-bound in-memory TLS credentials

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** networking, service, cluster-security, and operations maintainers
- **Extends:** [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md),
  [ADR 0289](0289-owning-authenticated-raft-transport-runtime.md),
  [ADR 0418](0418-owning-native-client-tls-routes.md), and
  [ADR 0420](0420-packaged-native-mutual-tls-server.md)

## Context

The native-client route owner and packaged daemon qualified each final TLS path with `O_NOFOLLOW`,
regular-file, size, and permission checks. OpenSSL's path APIs then reopened that name while
constructing a context. An attacker able to replace a directory entry between those steps could
make qualification and parsing refer to different inodes. Packaged Raft and native serving shared
the same gap.

## Decision

The maintained carrier accepts either one complete path bundle or one complete owning
`TlsPemCredentials` bundle. The alternatives are mutually exclusive. Empty, partial, oversized,
embedded-NUL, or ambiguous in-memory bundles fail before context publication. The existing path
fields remain available to embedders whose filesystem ownership contract makes them appropriate.

For the memory source, the carrier creates bounded memory BIOs, loads every trust certificate into
the context store, loads the leaf and remaining certificate chain, loads the private key, and
requires the key to match the leaf. Server mutual-certificate requirements and client SAN identity
verification are unchanged. OpenSSL types remain private. A shared immutable credential owner may
feed multiple expected-identity contexts; successful contexts retain OpenSSL's parsed state rather
than borrowing PEM strings.

`NativeClientTlsRouteOwner` and packaged `chronosd` no longer select the path source. They open each
final credential path once without following a final symlink, qualify the already-open regular
file, allocate its bounded initial size, read that descriptor to exact EOF, and reject truncation or
growth. Private keys remain inaccessible to group and other. Native server certificate and trust
files retain their group/other-write rejection. The native route owner applies the same write
policy to its certificate and trust files. Packaged Raft retains its established permission policy.

The native route owner shares one immutable byte bundle across every route context. Packaged Raft
shares one bundle across inbound server and per-peer client contexts. Packaged native serving
passes the same kind of bundle into epoll context construction. Changing, replacing, or unlinking a
configured name after its descriptor read cannot change the credentials parsed during that startup.
Credential rotation and reload remain restart operations.

## Consequences

Qualified bytes, not a later pathname resolution, now cross the packaged TLS security boundary.
Each composition retains at most one bounded shared copy of each PEM file rather than one copy per
expected-identity context. Contexts and sessions keep their prior ownership and synchronization
rules. No wire, durable, certificate-authority, node-authority, or application-principal semantics
change.

Callers that explicitly use the retained path source still receive its previous behavior and must
own path stability. Migrating that compatibility surface or adding descriptor-native OpenSSL store
providers would require a separate decision. Parent-directory access control still matters before
the one qualified open, and revocation, encrypted-key callbacks, hardware keys, and live reload are
not introduced here.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): credential loading does not add node, leader, group,
  or placement authority.
- [Invariant 11](../architecture/invariants.md): shared PEM owners outlive every synchronous parse,
  and published contexts own all later TLS state.
- [Invariant 14](../architecture/invariants.md): no network or durable format changes.
- [Invariant 18](../architecture/invariants.md): packaged startup cannot silently parse bytes from a
  path replacement that was not qualified.

## Validation

Carrier tests construct both endpoints exclusively from in-memory PEM, complete a real mutual-TLS
handshake, verify both peer identities, and reject ambiguous path-plus-memory and malformed-memory
configuration. Native route-owner tests continue to cover unsafe permissions, final symlinks,
bounds, malformed credentials, stable context addresses, and a real handshake. The Raft runtime
configuration gate accepts the in-memory source while preserving duplicate-group preflight.
Packaged CLI and Linux process gates continue exercising native and Raft startup through actual
`chronosd`.

## References

- [OpenSSL dependency record](../dependencies/openssl.md)
- [Native client route configuration](../operations/native-client-route-config.md)
- [Native server principal configuration](../operations/native-server-principal-config.md)
- [Deferred validation ledger](../development/deferred-validation.md)
