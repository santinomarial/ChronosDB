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

As completed by ADRs 0144 and 0145, `TLS_REQUIRED` uses the maintained OpenSSL carrier in the epoll
backend and never falls back to plaintext. A peer is marked transport-authenticated only after
chain and client-certificate verification; the certificate fingerprint is then mapped to the
stable principal by the application authenticator. The io_uring backend remains explicitly
unsupported for TLS until its separate record-completion design exists.

Authentication rejection closes the descriptor before native-protocol decoding and increments a
metric. Plaintext loopback authorization runs at accept; TLS authorization runs after the bounded
cryptographic handshake. The authenticator must outlive the reactor, is called only by its owner
thread, and must implement any external synchronization itself.

## Alternatives considered

- **Custom TLS or cryptography:** rejected by project policy and security risk.
- **Allow plaintext on any bind:** rejected because it silently exposes data.
- **Add a token to CLIENT_HELLO:** rejected until credential transport, rotation, and replay
  semantics are accepted.
- **Claim TLS termination without authenticated metadata:** rejected as spoofable.

## Consequences

Loopback development and mutually authenticated remote epoll serving are supported. Authorization
above the stable principal remains an embedding responsibility. io_uring TLS remains unavailable.

## Validation

Portable tests cover loopback restriction, custom allow/deny, certificate identity, mutual TLS,
and fail-closed behavior. Linux real-socket coverage proves verified TLS identity crosses the
bounded shard queue as the authorized principal.

## References

- [ADR 0009](0009-network-reactor-strategy.md)
- [ADR 0011](0011-dependency-and-build-versus-buy-policy.md)
- [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md)
- [ADR 0145](0145-bounded-epoll-mutual-tls-admission.md)
- [Native Protocol v1](../protocol/native-v1.md)
