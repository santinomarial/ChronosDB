# Native server operations baseline

The Phase 10 transport is a library component, not yet a production daemon. Linux is authoritative.
Callers create bounded request/response rings, start one `EpollReactor`, and drive `poll_once` from
its single owner thread. Port zero is for ephemeral tests; deployments should bind explicitly.

Set finite connection, event, frame, buffered-byte, queued-frame, in-flight request, handshake, and
idle limits. Defaults are development bounds, not capacity guidance. Monitor accepted, rejected,
active, closed, and timed-out connections; decoded/dispatched frames; overloads; dropped responses;
protocol errors; and bytes. Sustained rejects, overloads, or drops indicate inadequate capacity or
shard latency. Accepted sockets use `TCP_NODELAY`; failure to set it rejects admission. Do not raise
a bound without measuring retained memory. Plaintext binds only to IPv4
loopback. Remote epoll serving requires `TLS_REQUIRED`, an explicit certificate chain and private
key, an explicit trust store, mandatory client certificates, and a borrowed authenticator that maps
each verified certificate SHA-256 fingerprint to a stable nonzero principal. Invalid credentials
fail startup; handshake or authorization failure closes the connection without protocol dispatch.
The io_uring backend returns `NOT_SUPPORTED` for TLS rather than downgrading.

Shutdown closes every socket, detaches active work, and clears buffers. Stop shard response
production before destroying queues. The embedding owns any configured authenticator and must keep
it alive until reactor shutdown. Credential rotation currently requires replacing the reactor so a
new immutable TLS context owns all new sessions.
