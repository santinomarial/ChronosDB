# Packaged native daemon lifecycle

## Purpose and boundary

`chronosd` turns the existing Protocol v1 reactor into an installed process with bounded queues,
startup reporting, and signal-driven shutdown. With `--data-dir`, it owns the recoverable
single-node database and native service adapter behind that socket. Without the option it retains
the explicit unconfigured rejection mode. Handshake and PING/PONG remain implemented by the reactor.

## Ownership and lifetime

The main thread owns the reactor and calls `poll_once`. It is the sole request-ring producer and
response-ring consumer. One joined worker thread is the sole request-ring consumer and response-ring
producer. The queues outlive both threads and the reactor. Shutdown sets a signal-safe flag, stops
and joins the worker, and only then closes reactor descriptors.

The configured worker dispatches one task synchronously and may retain a bounded query response
sequence. It publishes that sequence in order and consumes no next request until completion. The
worker may retain one owned response when the response ring is full. It retries that same frame
without moving from it, blocking the data-plane consumer rather than allocating an unbounded side
queue. The SPSC release/acquire proof remains the one documented by ADR 0063.

Replicated mode keeps that same single queue consumer. Its adapter sends canonical ingest to the
nonblocking Raft coordinator and sends native SELECT to the synchronous query dispatcher over a
fresh owning applied read-barrier snapshot. Local one-voter startup commits a current-term no-op;
multi-voter mode lets the authenticated transport poll owner submit and exactly correlate the
barriers. A finite query sequence is emitted before another request or coordinator completion is
polled. Every table placement must be resident. The barrier result is a per-group vector rather
than a globally atomic cross-group instant, and remote fragments are not inferred from local state.

The subscription composition uses a stable committed-append router as the database's pre-open
observer address. After recovery and before socket admission, one per-plan runtime binds its fan-out
and borrows the database's exact snapshot storage context. The runtime owns neither the plan,
coordinator, resources, nor queues; all outlive it. A daemon registry must use separate bounded
internal SPSC queues for subscription requests and responses rather than letting two consumers race
on the reactor request ring.

With explicit SQL and MAC-key-file options, startup installs or reopens one immutable plan and its
coordinator below the database root. It compares the checkpoint vector with the recovered database
publication. Equality resumes normally; storage ahead of the checkpoint invalidates replay through
the current vector and checkpoints before admission, while storage behind or source drift fails.
The worker routes subscription tasks into the internal queues and continues polling the reactor
during signal shutdown until resumable terminal responses have left the worker ring.

## Failure behavior and limits

Queue capacity is finite and validated before allocation. Reactor admission, frame, connection,
handshake, idle, and buffering limits remain the `EpollServerConfig` bounds. Plaintext startup is
restricted to exact IPv4 loopback. Invalid options and unavailable reactor backends fail before the
startup banner. Worker publication or reactor failures terminate the process with a nonzero status.

The startup banner reports `data_plane=configured` or `data_plane=replicated` only after the
corresponding database path and reactor both start; otherwise the explicit unconfigured mode
remains distinguishable. Configured single-node ingest acknowledges
the exact requested/effective ASYNC or LOCAL_SYNC mode. Bootstrap and native DDL/DML identities use
the common nonnil system UUID source; deterministic service tests inject the same interface.

## Complexity and tradeoffs

Each dispatch and response handoff is amortized O(1); protocol parsing remains linear in frame size.
The worker uses a one-millisecond idle/backpressure poll, which is simple and bounded but is not a
measured production scheduling strategy. Configured execution is serial; a long request delays later
work. A future worker should use explicit wakeups and cancellable bounded execution while retaining
the same finite ownership contract.

## Verification and likely review questions

The Linux subprocess test starts the actual binary on an ephemeral port, negotiates Protocol v1,
checks PING/PONG and explicit unconfigured rejection, then starts a configured root, creates and
queries a table, sends `SIGTERM`, and verifies queryability after restart. Install-layout validation
checks that the binary is packaged and its help path runs. Its replicated case negotiates Protocol
2, applies QUORUM_SYNC, queries the applied rows, restarts, verifies an exact retry, and queries the
same recovered row count. A separate replicated gate provisions three retained roots and distinct
mutual-TLS identities, starts three actual daemon processes, obtains an applied quorum
acknowledgement, kills the acknowledged tablet leader, observes a higher-term replacement, and
requires an exact matching retry. Reopening each root then proves that all three applications
recover the same two visible rows and one retry entry. It deliberately does not claim native
multi-group query failover: SELECT still needs one daemon to lead every barrier group because client
leader routing and remote fragments are not packaged. On non-Linux hosts, daemon/service build and
durable-root initialization run, but the socket subprocess is not registered because the server
reactor is Linux-only.

Reviewers should ask: Which thread owns each queue endpoint? Can saturation allocate elsewhere? Can
liveness imply data readiness? What happens to active socket work on `SIGTERM`? Which acknowledged
durability mode applies? In configured mode, the last answer is the exact requested/effective ASYNC
or LOCAL_SYNC mode; in unconfigured mode there is no acknowledgement.
