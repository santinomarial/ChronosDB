# Native client session

`NativeClientSession` makes the Protocol v1 client state executable without owning a socket. The
caller copies its `pending_write()` bytes to any transport, reports the exact consumed prefix, and
feeds arbitrary received fragments back through `receive()`.

The session owns bounded connection buffers and a pre-reserved active-request vector. IDs advance
only after a complete request enters bounded output, so allocation or backpressure cannot burn or
reuse identity. Decoded response frames own payload bytes; query-batch views created from them must
not outlive those bytes.

Handshake, query result, ingest durability, negotiated leader redirect, cancellation, errors, and
PONG each have explicit state checks. A redirect terminally releases an ingest or finite query only
before the first result; its node ID is a routing observation rather than an endpoint or lease.
`NativeLeaderRedirectRouter` can then join that observation to an explicit native endpoint and
borrowed TLS context while enforcing exact group, nondecreasing placement/term authority,
same-term leader consistency, and a finite redirect count. It remains separate from the session so
the framing owner never infers deployment routes or silently replays a request.
`NativeQuorumIngestRetry` is the explicit higher-level replay composition: it owns one append,
requires both Protocol 2 features, creates a fresh session/request ID per accepted redirect, and
publishes only a group/current-leader/nonregressing-term receipt. Its reconnect event tells a later
carrier exactly when the old transport must be replaced.
`NativeQuorumIngestTcpClient` is that carrier boundary. It owns one nonblocking descriptor and TLS
session at a time, authenticates the verified certificate fingerprint as a stable principal, and
requires a node authorizer to bind that principal to the router's current node before writing the
native handshake. Its poll interest and active deadline let an external event loop drive exactly
one connect, TLS, read, or write operation per call. Connect, handshake, and exchange deadlines
reset only at their defined phase transitions, including an accepted redirect.

The TCP owner is declared before the optional TLS session so reverse destruction always releases
TLS before closing its borrowed descriptor. A redirect explicitly performs that order before
opening the next route. Success closes transport and retains only the exact receipt; all failures
are sticky. In particular, EOF after sending an ingest is ambiguous and cannot cause replay because
only an authenticated leader redirect supplies retry authority. Memory is bounded by the route and
session configuration plus one fixed I/O chunk, and work is linear in bytes plus logarithmic route
selection. One event-loop thread owns the entire composition, so it needs no synchronization or
memory-ordering protocol.
Protocol errors fail closed and release all retained bytes. Complexity is linear in handled
bytes plus the finite active-request search; completed output-frame removal is linear in the finite
queued-frame bound.
