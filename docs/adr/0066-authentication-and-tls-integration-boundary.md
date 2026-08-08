# ADR 0066: Authentication and TLS integration boundary

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** networking and security subsystems
- **Supersedes:** none

## Context

CRC32C is not authentication. Phase 10 needs an integration boundary without implementing crypto
or claiming that a raw epoll socket is secure. The accepted protocol has no bearer-token message,
and Linux-native handles must not leak through portable APIs.

## Decision

The baseline permits plaintext only when binding and accepting IPv4 loopback (`127/8`). A borrowed,
user-owned `ConnectionAuthenticator` may synchronously accept or reject a peer on the reactor owner
thread and attach a nonzero stable `principal_id`. Every dispatched request and cancellation carries
that identity to the shard. The default loopback development identity is anonymous zero; custom
authenticators cannot claim an authorized zero identity.

`TLS_REQUIRED` is explicit, but the epoll backend returns `NOT_SUPPORTED` at startup until a
maintained TLS record/handshake backend is integrated. It never falls back to plaintext. Future
providers may mark a peer transport-authenticated only after cryptographic verification.

Authentication rejection closes the descriptor before protocol allocation or handshake and
increments a metric. The authenticator must outlive the reactor, is called only by its owner thread,
and must implement any external synchronization itself.

## Alternatives considered

- **Custom TLS or cryptography:** rejected by project policy and security risk.
- **Allow plaintext on any bind:** rejected because it silently exposes data.
- **Add a token to CLIENT_HELLO:** rejected until credential transport, rotation, and replay
  semantics are accepted.
- **Claim TLS termination without authenticated metadata:** rejected as spoofable.

## Consequences

Loopback development works now. Remote serving remains blocked until a maintained TLS integration is
implemented and tested. Authorization above the stable principal remains an embedding responsibility.

## Validation

Portable tests cover loopback restriction, custom allow/deny and identity, and TLS fail-closed
behavior. Linux real-socket coverage proves the principal crosses the bounded shard queue.

## References

- [ADR 0009](0009-network-reactor-strategy.md)
- [ADR 0011](0011-dependency-and-build-versus-buy-policy.md)
- [Native Protocol v1](../protocol/native-v1.md)

