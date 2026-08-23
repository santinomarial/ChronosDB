# ADR 0422: Transactional native TLS security reload

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** networking, native-server, security, and operations maintainers
- **Extends:** [ADR 0145](0145-bounded-epoll-mutual-tls-admission.md),
  [ADR 0420](0420-packaged-native-mutual-tls-server.md), and
  [ADR 0421](0421-descriptor-bound-in-memory-tls-credentials.md)

## Context

Packaged native mutual TLS previously required a process restart for certificate, trust-store, or
client-principal changes. The maintained carrier already permits established sessions to retain an
older immutable OpenSSL context, but the epoll reactor had no owner-thread operation for moving new
admissions to a replacement context. Rebuilding the complete reactor would also disconnect every
authenticated client and complicate its concurrent response-wakeup borrow.

## Decision

`EpollReactor::reload_tls_security` accepts one complete owning `TLS_REQUIRED` security generation
on the reactor owner thread. It validates the replacement against the unchanged bind address and
constructs the complete replacement `TlsServerContext` before mutating live admission state. An
invalid authority pointer, credential bundle, key/certificate pair, or trust store returns an
error, increments the reload-failure metric, and leaves the current generation and every
connection unchanged.

After successful construction, the owner closes every connection whose mutual-TLS handshake has
not completed. This prevents a handshake authenticated by one TLS context from being authorized by
another principal generation. It then moves the replacement authority pointer and context into
the admission state without allocation. Already authenticated sessions remain open with their
stored principal and their `SSL` object's retained reference to the original `SSL_CTX`; they never
call the replaced authority again. New accepts use only the replacement context and authority.

The portable `Reactor` forwards this operation only to epoll. Other backends return
`NOT_SUPPORTED`; plaintext-to-TLS, TLS-to-plaintext, bind-address, listener, and queue replacement
are not reload operations. Metrics count successful generations, failed attempts, and handshakes
closed by successful swaps in addition to the ordinary connection-close count.

When the packaged daemon receives `SIGHUP`, its signal handler sets only a `sig_atomic_t` request
flag. The main reactor owner consumes that flag, rereads the complete native principal and PEM
bundle through the descriptor-bound ADR 0421 path, and invokes the transactional replacement. It
publishes `native_security_reloaded` with a monotonic process-local generation only after success.
A failed read, parse, context construction, or swap logs `native_security_reload_failed` and keeps
the prior generation serving. `SIGHUP` without configured native mutual TLS is an explicit logged
no-op. `SIGINT` and `SIGTERM` retain their existing shutdown meaning.

The daemon transfers ownership of the replacement authority only after the reactor accepts its
borrow. The successful reactor operation has already closed every incomplete handshake, so the old
authority may then be destroyed while established sessions retain only their copied principal.
Operators must stage all four files before sending `SIGHUP`; cross-file filesystem snapshotting and
automatic file watching are not inferred.

## Consequences

Native admission credentials and coarse client principals can rotate without restarting the
database, Raft runtime, worker, listener, or established authenticated sessions. A revoked
principal can therefore retain an existing connection until it closes or reaches its idle
deadline; immediate established-session revocation would require an explicit connection policy and
is not implied by trust reload.

This operation does not reload Raft peer credentials, native-client route credentials, bind
addresses, protocol limits, or authorization above the stable principal. Certificate revocation
services, automatic polling, multi-file transaction formats, and io_uring TLS remain deferred.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): reload never grants node, group, placement, or
  leadership authority.
- [Invariant 11](../architecture/invariants.md): each established TLS session retains its original
  context, and each borrowed authenticator remains alive until no handshake can call it.
- [Invariant 14](../architecture/invariants.md): no native wire or durable format changes.
- [Invariant 18](../architecture/invariants.md): failed replacement cannot weaken or partially
  publish the active transport-authentication policy.

## Validation

The Linux reactor test establishes one authenticated session, admits an incomplete handshake,
rejects a malformed replacement without changing either connection, then installs a new authority.
It proves the incomplete handshake closes, the established session still dispatches with its old
principal, and a new session dispatches with the replacement principal while exact reload metrics
advance. The Linux packaged-process gate rotates server certificate/key, trust store, and principal
authority on `SIGHUP`, rejects the old client generation, and completes an exact matching retry
through the new generation. Portable builds retain the explicit non-epoll boundary.

## References

- [Native server principal configuration](../operations/native-server-principal-config.md)
- [Native server operations](../operations/native-server.md)
- [Packaged native daemon lifecycle](../learning/packaged-native-daemon.md)
