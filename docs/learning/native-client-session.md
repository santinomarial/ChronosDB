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
`parse_native_client_route_config` supplies the strict deployment boundary for that map. Each
node-sorted entry owns one usable IPv4 endpoint, expected TLS server identity, and unique leaf
certificate fingerprint. `NativeClientRouteAuthority` then binds a verified fingerprint and
connected address to the same stable node principal required by the carrier. The file deliberately
contains no group, leader, term, membership, or placement state; request-specific routing authority
still comes from the caller and authenticated protocol observations.
`NativeClientTlsRouteOwner` closes the deployment lifetime above that parser. It reads the exact
route file through a final-symlink-resistant descriptor, qualifies bounded credential files and
private-key permissions, owns the immutable authority, reserves one TLS client context per route,
and publishes context pointers only after every context succeeds. Its PIMPL keeps those pointers
stable across an outer move. OpenSSL still reopens each credential path while constructing a
context, so protected parent directories and atomic deployment remain required.
`NativeQuorumIngestRetry` is the explicit higher-level replay composition: it owns one append,
requires both Protocol 2 features, creates a fresh session/request ID per accepted redirect, and
publishes only a group/current-leader/nonregressing-term receipt. Its reconnect event tells a later
carrier exactly when the old transport must be replaced.
`NativeQueryRetry` applies the same lifecycle discipline to one finite query. It retains exact SQL,
requires Protocol 2 leader redirect, and buffers only canonical encoded result batches within
explicit cumulative row, batch, and byte limits. `result()` remains empty until `QUERY_END`; any
redirect is valid only before the first batch, and every failure erases the accumulated stream.
The owner deliberately cannot collapse multiple query-group leaders into one route.
`NativeQuorumIngestTcpClient` is that carrier boundary. It owns one nonblocking descriptor and TLS
session at a time, authenticates the verified certificate fingerprint as a stable principal, and
requires a node authorizer to bind that principal to the router's current node before writing the
native handshake. Its poll interest and active deadline let an external event loop drive exactly
one connect, TLS, read, or write operation per call. Connect, handshake, and exchange deadlines
reset only at their defined phase transitions, including an accepted redirect.

The TCP owner is declared before the optional TLS session so reverse destruction always releases
TLS before closing its borrowed descriptor. A redirect explicitly performs that order before
opening the next route. Success closes transport and exposes only the exact receipt; the bounded
request owner remains alive until its containing client is destroyed. All failures are sticky. In
particular, EOF after sending an ingest is ambiguous and cannot cause replay because only an
authenticated leader redirect supplies retry authority. Memory is bounded by the route and session
configuration plus one fixed I/O chunk, and work is linear in bytes plus logarithmic route
selection. One event-loop thread owns the entire composition, so it needs no synchronization or
memory-ordering protocol.
`NativeQuorumIngestTcpExecution` owns that event-loop step for one operation. It constructs one
client, translates the client's current read/write interest into a single `pollfd`, and caps every
kernel wait by the caller maximum, the current carrier phase deadline, and an optional absolute
operation deadline. It drives the client even after a zero-readiness timeout so exact phase expiry
cannot depend on socket activity. `POLLERR` and `POLLHUP` are applied only to the direction the
client requested; the client still classifies connect or TLS failure. Explicit cancellation and
whole-operation expiry destroy the client, preserve the first terminal status, and never expose a
partial result. Metrics retain attempts, redirects, waits, readiness, and the last exact route.
Protocol errors fail closed and release all retained bytes. Complexity is linear in handled
bytes plus the finite active-request search; completed output-frame removal is linear in the finite
queued-frame bound.
