# ADR 0145: Bounded epoll mutual-TLS admission

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** network and cluster-security subsystems
- **Supersedes:** the epoll deferral in ADR 0066 and ADR 0144

## Context

ADR 0144 provides nonblocking OpenSSL socket sessions, but transport security is useful only when
the reactor drives handshake and record readiness without violating bounded work, socket lifetime,
or the rule that unverified bytes cannot enter the native protocol decoder.

## Decision

When `TLS_REQUIRED` is selected, `EpollReactor::start` constructs one immutable
`TlsServerContext` before opening the listener. The context is declared above the connection table
and therefore outlives every session. Each accepted descriptor receives a `TlsSocket` but remains
unauthenticated and carries principal zero until the nonblocking mutual-TLS handshake succeeds.

The epoll owner translates OpenSSL `WANT_READ` and `WANT_WRITE` into descriptor interests. Cross-
direction retries preserve the same scratch or immutable pending-write buffer until the original
OpenSSL operation completes. The existing per-event I/O budget bounds plaintext record work, while
the fixed handshake deadline bounds TLS plus native-protocol admission time. Because OpenSSL can
retain decrypted plaintext after the kernel descriptor stops being readable, each connection tracks
that state explicitly. A bounded pre-poll scan drains at most the configured event budget and forces
a zero-duration kernel poll after doing internal work, preventing both stalls and unbounded draining.

After OpenSSL verifies the chain and client certificate, the owner passes the certificate SHA-256
fingerprint and peer address to `ConnectionAuthenticator`. Only an authorized nonzero principal
makes the connection accepted; only then may decrypted bytes reach `ConnectionBuffers` or shard
queues. TLS failures and authorization denials increment authentication rejections and close the
connection. Session destruction precedes descriptor close. Protocol responses use TLS record writes
and retain the existing immutable partial-write offset.

The optional io_uring backend continues to return `NOT_SUPPORTED` for `TLS_REQUIRED`; direct
`IORING_OP_RECV`/`SEND` cannot safely drive OpenSSL record state without a separately designed
memory-BIO completion boundary.

## Alternatives considered

- **Authenticate before TLS by address:** rejected because routing metadata is not node identity.
- **Decode `CLIENT_HELLO` before certificate verification:** rejected because hostile plaintext
  would cross the security boundary.
- **Temporarily switch sockets to blocking mode:** rejected because one peer could stall the owner.
- **Reuse direct io_uring socket operations for TLS records:** rejected because OpenSSL owns record
  framing and may require cross-direction progress.
- **Close the descriptor before destroying `SSL`:** rejected because the carrier borrows a live
  descriptor for its complete lifetime.

## Consequences

The epoll backend now supports remote certificate-authenticated serving without plaintext fallback.
Metrics count a TLS connection as accepted only after both certificate verification and application
authorization. `bytes_read` and `bytes_written` represent native-protocol plaintext bytes for TLS
connections and socket bytes for plaintext development connections; future wire-byte telemetry
requires an explicit carrier counter.

## Affected invariants

Invariants 1, 5, 10, 14, and 18 apply. Authentication precedes visibility, connection and I/O work
remain bounded, one owner controls readiness and buffers, and disconnect cleanup destroys every
borrowed-descriptor session before close.

## Validation plan

The Linux real-socket test drives TCP accept, nonblocking mutual TLS with distinct CA-signed server
and client certificates, fingerprint authorization, encrypted native hello/response exchange, and
principal-tagged query dispatch. The carrier suite separately rejects missing client certificates
and invalid credentials. Full network tests and sanitizer builds cover existing plaintext behavior.

## Migration or rollback considerations

Selecting loopback plaintext is unchanged. Rolling back this integration must restore explicit
`NOT_SUPPORTED` for epoll `TLS_REQUIRED`; it must not silently accept raw bytes. Credential rotation
creates a new reactor/context in the current library API, so production daemon reload orchestration
remains follow-up work.

## Unresolved questions

Credential reload, certificate revocation policy, dedicated wire-byte metrics, io_uring TLS memory-
BIO scheduling, and cluster-control request multiplexing remain.

## References

- [ADR 0064](0064-bounded-linux-epoll-reactor.md)
- [ADR 0066](0066-authentication-and-tls-integration-boundary.md)
- [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md)
- [Native server operations](../operations/native-server.md)
- [Architecture invariants](../architecture/invariants.md)
