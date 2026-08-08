# Network security boundary

The Phase 10 security boundary is deliberately honest: loopback plaintext is a development mode,
not TLS. `TLS_REQUIRED` fails startup because no maintained TLS record backend is integrated. There
is no fallback.

`ConnectionAuthenticator` is a borrowed synchronous callback invoked by the reactor owner for a
new peer. It returns rejection or a stable nonzero principal. The reactor attaches that identity to
every request and cancellation, so authorization never infers identity from a reused descriptor.
Anonymous zero exists only for default loopback development.

The callback must outlive the reactor. Rejection happens before handshake or peer-sized allocation.
A future TLS provider may set `transport_authenticated` only after its maintained library verifies
the handshake; source address, CRC, and proxy assertions are not equivalent evidence.

Review questions: What proves transport authentication? Can the backend downgrade? Who owns the
authenticator? Does the principal survive cancellation? When can hostile bytes allocate memory?

