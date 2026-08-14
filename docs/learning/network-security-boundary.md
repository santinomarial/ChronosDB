# Network security boundary

Loopback plaintext is a development mode, not TLS. The maintained OpenSSL carrier requires TLS 1.2
or newer, an explicit server chain and key, an explicit trust store, and a verified client
certificate. There is no fallback or opportunistic downgrade. Backend event-loop integration must
be explicit: epoll provides it; io_uring currently returns `NOT_SUPPORTED` for TLS.

OpenSSL record I/O uses a shared custom BIO over a borrowed socket. The BIO never closes the
descriptor and its method owner outlives every session. Writes suppress `SIGPIPE` per call on Linux
or per socket on macOS without replacing the application's process-wide signal policy. Broken-pipe
and hangup races therefore become ordinary carrier failures that the socket owner observes and
removes. Reusing one BIO method is also required for long-lived processes because OpenSSL reserves a
finite type range for custom BIO implementations.

`ConnectionAuthenticator` is a borrowed synchronous callback invoked by the socket owner after
transport verification. For TLS it receives the verified certificate SHA-256 fingerprint and maps
that identity to rejection or a stable nonzero principal. The reactor attaches that identity to
every request and cancellation, so authorization never infers identity from a reused descriptor.
Anonymous zero exists only for default loopback development.

The callback must outlive the reactor. In plaintext mode rejection precedes protocol handshake. In
TLS mode untrusted peers consume only the explicitly bounded connection and handshake resources;
no application frame is decoded first. Only the maintained carrier may set
`transport_authenticated`, and only after chain verification and peer-certificate capture. Source
address, CRC, certificate subject text, and proxy assertions are not equivalent evidence.

Review questions: What proves transport authentication? Can the backend downgrade? Who owns the
authenticator? Does the principal survive cancellation? When can hostile bytes allocate memory?
