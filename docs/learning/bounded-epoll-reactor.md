# Bounded Linux epoll Reactor

The Linux reactor is the transport owner between Protocol v1 sockets and a shard worker. Its
portable `EpollReactor` interface hides file descriptors and `epoll_event`; non-Linux startup
returns `NOT_SUPPORTED`.

## Ownership and lifecycle

One thread owns a reactor. It accepts sockets, performs nonblocking I/O, mutates connection state,
changes readiness interest, drains responses, and expires deadlines. Each connection owns its
buffers, state, and optional OpenSSL session. The reactor-owned TLS context outlives the connection
table, and a session is destroyed before its borrowed descriptor closes. Decoded `Frame` ownership
crosses the request SPSC ring. A shard returns another owned frame; encoding copies it into bounded
connection output storage.
After publishing a response, the shard signals a coalescing `eventfd`, so an owner blocked in
`epoll_wait` does not wait for its timeout. The queue release/acquire pair publishes data; eventfd
publishes only readiness.

Connection IDs, not file descriptors, authenticate returned work. IDs increase without reuse.
Disconnect erases state and bytes, so later responses have no destination and are dropped.

```text
accept -> bounded connection -> optional mutual TLS -> principal authorization
EPOLLIN -> checked frames -> state transition -> request ring
response ring -> active identity/type/payload check -> bounded output -> EPOLLOUT
deadline/EOF/error -> detach -> clear -> close
```

Reads continue until would-block or a short read. Data accompanying half-close is read first; EOF
then performs disconnect cleanup. Writes retain immutable encoded bytes plus a monotonic offset.
Accepted sockets use `TCP_NODELAY`: adjacent result and terminal frames remain separately owned,
and the terminal must not wait for a delayed-ACK/Nagle interaction after the preceding short write.
TLS `WANT_READ`/`WANT_WRITE` states change epoll interest without blocking. The owner repeats
cross-direction operations with the same scratch or immutable pending-write buffer before other
record work. Decrypted bytes retained inside OpenSSL are marked explicitly and drained through a
bounded pre-poll pass, because they cannot rely on another kernel readiness edge. No decrypted frame
reaches the decoder before certificate verification and principal authorization.

## Bounds and failures

Connection count, readiness events, per-event I/O operations, read scratch, frame, buffered bytes,
output frames, in-flight requests, and both rings are finite. The I/O-operation budget makes an
always-ready listener or peer yield to the next poll. Full admission closes the new descriptor. A full request ring yields an
explicit overload error. Full output closes because no diagnostic can safely bypass that bound.
Startup allocation failure returns `RESOURCE_EXHAUSTED` after closing descriptors.

Disconnect cancellation publication is best effort because the request ring may be full.
Detachment is unconditional: identities are removed and late results dropped.

The Linux hostile suite uses real loopback sockets for slow partial hello, admission saturation,
explicit cancellation, simultaneous data/half-close, 128 descriptor-reuse cycles, a blocked poll
wakeup, an authenticated TLS hello/query exchange, and an 8 MiB result with a constrained receive
window. The last case proves partial writes retain exact bytes and preserve `QUERY_RESULT` before
`QUERY_END`.

## Complexity

I/O and queue work is linear in handled bytes/frames. Response routing and deadline scanning are
currently `O(configured connections)` per task/poll; profile this baseline before adding an index.

## Interview questions

- Why process readable bytes before simultaneous `EPOLLRDHUP`?
- Why is a file descriptor not a sufficient returned-work identity?
- Why does output saturation close instead of enqueueing another error?
- What remains guaranteed when disconnect cancellation cannot enter the request ring?
