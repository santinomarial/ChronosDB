# ADR 0144: Maintained mutual-TLS socket carrier

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** network and cluster-security subsystems
- **Supersedes:** the deferred carrier portion of ADR 0066

## Context

Remote cluster control traffic cannot rely on CRCs, source addresses, or an application principal
assertion for transport authentication. ADR 0066 therefore kept `TLS_REQUIRED` fail-closed until a
maintained TLS implementation existed. The reactor also needs nonblocking handshake and record I/O
whose ownership is compatible with its single-threaded socket owner.

## Decision

ChronosDB uses OpenSSL 3 `libssl` as its maintained TLS record and certificate-verification
implementation. `TlsServerContext` owns an immutable `SSL_CTX` behind a PIMPL. `TlsSocket` owns one
server `SSL` session over a borrowed nonblocking connected socket; the reactor continues to own and
close the descriptor. No OpenSSL type appears in a public ChronosDB header.

The server requires TLS 1.2 or newer, disables compression and renegotiation, loads an explicit
certificate chain, private key, and trust store, and requires a verified client certificate. After
OpenSSL reports a successful handshake and `X509_V_OK`, the carrier computes the peer certificate's
SHA-256 fingerprint through OpenSSL EVP. Only then may it expose authenticated plaintext or pass the
fingerprint to `ConnectionAuthenticator`, which maps the certificate identity to a stable nonzero
ChronosDB principal. Transport verification and application authorization remain separate checks.

Handshake, read, and write calls return explicit complete, want-read, want-write, or clean-close
states. They never block or own an event loop. A session and its borrowed descriptor have one owner
thread. Session creation through `SSL_new` retains OpenSSL's reference-counted `SSL_CTX`, so the
Chronos context factory must outlive synchronous session creation but may be replaced or destroyed
afterward. The carrier attaches OpenSSL to the borrowed descriptor through one process-wide custom
BIO method. Its writes suppress `SIGPIPE` per operation with `MSG_NOSIGNAL` where available or per
descriptor with `SO_NOSIGPIPE`; it never changes the embedding process's signal disposition. The
method is shared across session lifetimes so the finite OpenSSL custom-BIO type namespace is not
consumed per connection.

## Alternatives considered

- **Custom TLS or cryptography:** rejected because security-sensitive commodity protocols require a
  maintained implementation and review surface.
- **Treat a certificate subject string as the principal:** rejected because naming and collision
  policy belong to the application authenticator; the carrier supplies the verified certificate
  fingerprint.
- **Authorize by source address after TLS:** rejected because addresses are routing metadata, not
  durable node identity.
- **Make TLS calls blocking:** rejected because one slow or malicious handshake could stall all
  sockets owned by a reactor.
- **Permit optional client certificates:** rejected because remote database and cluster control
  connections require mutual identity.

## Consequences

`chronos_network` now privately links OpenSSL Crypto and SSL, while its public ABI remains free of
OpenSSL types. TLS credential paths are owning strings in the copied server configuration; ADR 0421
later adds a mutually exclusive shared owning PEM source for descriptor-qualified callers. The
application authenticator remains borrowed and must outlive every handshake that can call it; ADR
0422 later defines a safe generation-replacement boundary. The socket carrier is usable
independently and is the only accepted transport-authentication source. ADR 0145 integrates its
readiness states into epoll. Other backends may not claim TLS serving support until they provide
their own tested record scheduling. Abrupt peer closure is returned through the existing TLS I/O
status boundary and cannot terminate the host process with `SIGPIPE`.

## Affected invariants

Invariants 1, 5, 14, and 18 apply. Unverified bytes never become protocol frames, authentication
cannot downgrade to plaintext, allocation and socket ownership are explicit, and failures are
observable statuses rather than successful placeholders.

## Validation plan

Focused socket-pair tests use a test CA plus distinct server and client certificates. They cover
credential loading failure, successful nonblocking mutual handshake, verified fingerprint delivery
to the application authenticator, bidirectional plaintext records, and rejection without a client
certificate. A context-lifetime regression destroys both Chronos context factories before the
sessions complete their handshake and proves each session retained the parsed OpenSSL context. An
abrupt-close regression installs a temporary observing `SIGPIPE` handler and proves TLS writes do
not raise it. A repeated-session regression exceeds OpenSSL's finite custom-BIO type range and
proves one method is reused. Real mutual-TLS query execution drives the server after the client's
terminal local failure to cover hangup plus write readiness. Sanitizer and installed-consumer checks
cover the same OpenSSL-free public boundary.

## Migration or rollback considerations

This adds no durable or application wire format. Removing the carrier returns `TLS_REQUIRED` to
fail-closed unsupported behavior. Certificate rotation is performed by creating a new context and
moving new connections to it; existing sessions retain their original context and trust decision.

## Unresolved questions

io_uring record scheduling, reload orchestration outside the native epoll server, revocation
policy, response write deadlines, and disconnect retry ownership remain. ADR 0422 adds the bounded
native epoll and packaged-daemon reload owner.

ADR 0172 subsequently adds the symmetric maintained client context and connected-session creation
with required DNS or IP server-identity verification.

## References

- [ADR 0009](0009-network-reactor-strategy.md)
- [ADR 0064](0064-bounded-linux-epoll-reactor.md)
- [ADR 0066](0066-authentication-and-tls-integration-boundary.md)
- [ADR 0145](0145-bounded-epoll-mutual-tls-admission.md)
- [ADR 0172](0172-maintained-mutual-tls-client-socket.md)
- [OpenSSL dependency record](../dependencies/openssl.md)
- [Descriptor-bound in-memory TLS credentials](0421-descriptor-bound-in-memory-tls-credentials.md)
- [Transactional native TLS security reload](0422-transactional-native-tls-security-reload.md)
- [Architecture invariants](../architecture/invariants.md)
