# ADR 0172: Maintained mutual-TLS client socket

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** network and cluster-security subsystems
- **Extends:** [ADR 0144](0144-maintained-mutual-tls-socket-carrier.md)

## Context

ADR 0144 supplied a maintained server-side TLS context and nonblocking accepted session, but
ChronosDB still lacked an equivalent way to originate authenticated cluster connections. Tests and
embeddings had to manipulate `SSL_CTX` and `SSL` directly, so the public carrier could not yet drive
distributed-query requests over mutually authenticated sockets or enforce the intended server
identity itself.

## Decision

`TlsClientContext` owns an immutable OpenSSL client context plus one required expected server
identity behind the same OpenSSL-free PIMPL boundary. It loads an explicit trust store and client
certificate/private-key pair, requires TLS 1.2 or newer, disables compression and renegotiation, and
enables the same nonblocking partial-write modes as the server context.

ADR 0421 subsequently adds an exact in-memory PEM source, mutually exclusive with the retained path
source. Secure-file-owning services use it after reading already-qualified descriptors.

`TlsSocket::connect` creates a client session over a borrowed connected nonblocking descriptor. A
DNS identity is configured as both the certificate hostname check and SNI value; an IPv4 or IPv6
identity is configured through OpenSSL's IP SAN verifier and is not sent as SNI. Empty, overlong, or
embedded-NUL identities reject before session creation. The existing handshake state machine
drives both accepted and connected sessions and classifies certificate or identity verification
failure as unauthenticated. Plaintext and the peer certificate SHA-256 fingerprint remain
unavailable until the handshake and verification both succeed.

Session creation retains OpenSSL's reference-counted `SSL_CTX`, so the Chronos client-context
factory must outlive synchronous creation but may be replaced or destroyed afterward. The caller
retains descriptor ownership, and a single reactor owner thread serializes every context/session
use. Reconnection, address selection, deadlines, retry, and application-principal authorization
remain above this carrier.

## Consequences and validation

ChronosDB can now terminate and originate maintained mutual-TLS sessions without exposing OpenSSL
types in installed headers. The expected identity is an owning context string, so retry sessions
cannot silently change the certificate target. Server certificate rotation remains possible when
the replacement chain covers the configured identity and roots in the explicit trust store.

Socket-pair tests drive both maintained endpoints through partial readiness, verify distinct server
and client certificate fingerprints, exchange plaintext in both directions, reject a mismatched IP
SAN, reject missing credentials, retain the server-side missing-client-certificate check, and prove
sessions remain valid after both Chronos context factories are destroyed. The installed consumer
references both client context construction and connected-session creation.

Invariants 1, 5, 14, and 18 apply.

## Alternatives considered

- **Keep outbound OpenSSL ownership in each embedding:** rejected because verification and
  nonblocking-state rules would be duplicated outside the maintained carrier boundary.
- **Verify only the issuing CA:** rejected because a trusted certificate for another server is not
  authority for the configured destination.
- **Infer identity from the connected address:** rejected because routing and certificate identity
  selection are separate embedding decisions.
- **Send an IP literal as SNI:** rejected because SNI carries DNS server names; IP identity is
  verified directly against an IP SAN.

## Migration and rollback

This adds no durable or application wire format. Existing server sessions are unchanged. Embeddings
that hand-built OpenSSL client sessions can migrate by constructing `TlsClientContext` and handing
the already-connected descriptor to `TlsSocket::connect`. Removing the API restores the earlier
server-only carrier and therefore makes authenticated outbound cluster transport incomplete.

## References

- [Maintained mutual-TLS socket carrier](0144-maintained-mutual-tls-socket-carrier.md)
- [Bounded epoll mutual-TLS admission](0145-bounded-epoll-mutual-tls-admission.md)
- [Authenticated distributed query transport](0168-authenticated-distributed-query-transport.md)
- [OpenSSL dependency record](../dependencies/openssl.md)
- [Descriptor-bound in-memory TLS credentials](0421-descriptor-bound-in-memory-tls-credentials.md)
- [Architecture invariants](../architecture/invariants.md)
