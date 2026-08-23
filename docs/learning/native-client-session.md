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
Protocol errors fail closed and release all retained bytes. Complexity is linear in handled
bytes plus the finite active-request search; completed output-frame removal is linear in the finite
queued-frame bound.
