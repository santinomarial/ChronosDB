# Packaged native daemon lifecycle

## Purpose and boundary

`chronosd` turns the existing Protocol v1 reactor into an installed process with bounded queues,
startup reporting, and signal-driven shutdown. It deliberately does not manufacture a database
behind that socket. Handshake and PING/PONG are implemented by the reactor; ingest, query, and
subscription requests receive a terminal `EXECUTION_FAILURE` until durable runtime composition is
configured.

## Ownership and lifetime

The main thread owns the reactor and calls `poll_once`. It is the sole request-ring producer and
response-ring consumer. One joined worker thread is the sole request-ring consumer and response-ring
producer. The queues outlive both threads and the reactor. Shutdown sets a signal-safe flag, stops
and joins the worker, and only then closes reactor descriptors.

The worker may retain one owned response when the response ring is full. It retries that same frame
without moving from it, blocking the data-plane consumer rather than allocating an unbounded side
queue. The SPSC release/acquire proof remains the one documented by ADR 0063.

## Failure behavior and limits

Queue capacity is finite and validated before allocation. Reactor admission, frame, connection,
handshake, idle, and buffering limits remain the `EpollServerConfig` bounds. Plaintext startup is
restricted to exact IPv4 loopback. Invalid options and unavailable reactor backends fail before the
startup banner. Worker publication or reactor failures terminate the process with a nonzero status.

The startup banner reports `data_plane=unconfigured` so process liveness cannot be confused with
database readiness. No ingest durability mode is acknowledged.

## Complexity and tradeoffs

Each dispatch and response handoff is amortized O(1); protocol parsing remains linear in frame size.
The worker uses a one-millisecond idle/backpressure poll, which is simple and bounded but is not a
measured production scheduling strategy. A future durable adapter should use an explicit wakeup and
retain the same finite ownership contract.

## Verification and likely review questions

The Linux subprocess test starts the actual binary on an ephemeral port, negotiates Protocol v1,
checks PING/PONG, verifies explicit query rejection, and sends `SIGTERM`. Install-layout validation
checks that the binary is packaged and its help path runs.

Reviewers should ask: Which thread owns each queue endpoint? Can saturation allocate elsewhere? Can
liveness imply data readiness? What happens to active socket work on `SIGTERM`? Which acknowledged
durability mode applies? Today the last answer is “none,” by design.
