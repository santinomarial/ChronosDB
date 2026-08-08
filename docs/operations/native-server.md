# Native server operations baseline

The Phase 10 transport is a library component, not yet a production daemon. Linux is authoritative.
Callers create bounded request/response rings, start one `EpollReactor`, and drive `poll_once` from
its single owner thread. Port zero is for ephemeral tests; deployments should bind explicitly.

Set finite connection, event, frame, buffered-byte, queued-frame, in-flight request, handshake, and
idle limits. Defaults are development bounds, not capacity guidance. Monitor accepted, rejected,
active, closed, and timed-out connections; decoded/dispatched frames; overloads; dropped responses;
protocol errors; and bytes. Sustained rejects, overloads, or drops indicate inadequate capacity or
shard latency. Do not raise a bound without measuring retained memory. Plaintext binds only to IPv4
loopback. `TLS_REQUIRED` currently fails startup rather than downgrading, so remote serving is not an
implemented deployment mode.

Shutdown closes every socket, detaches active work, and clears buffers. Stop shard response
production before destroying queues. The embedding owns any configured authenticator and must keep
it alive until reactor shutdown.
