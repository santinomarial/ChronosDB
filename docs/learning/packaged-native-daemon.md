# Packaged native daemon lifecycle

## Purpose and boundary

`chronosd` turns the native Protocol 1/2 reactor into an installed process with bounded queues,
startup reporting, optional mutual TLS, and signal-driven shutdown. With `--data-dir`, it owns the
recoverable single-node database and native service adapter behind that socket. Without the option
it retains the explicit unconfigured rejection mode. Handshake and PING/PONG remain implemented by
the reactor.

## Ownership and lifetime

The main thread owns the reactor and calls `poll_once`. It is the sole request-ring producer and
response-ring consumer. One joined worker thread is the sole request-ring consumer and response-ring
producer. The queues outlive both threads and the reactor. Shutdown sets a signal-safe flag, stops
and joins the worker, and only then closes reactor descriptors.

When the atomic native security bundle is configured, an immutable certificate-to-principal
authority is created before the reactor and therefore outlives the reactor's borrowed pointer. The
reactor owns its OpenSSL server context and per-connection sessions. The authority grants coarse
protocol admission only: it does not infer node, group, leader, placement, table, operation, or
source-IP authority from a client certificate. The daemon reads the certificate, private key, and
trust store to exact EOF through their already-qualified descriptors and passes one immutable PEM
bundle to context construction. Replacing a configured path after that read cannot change the
credentials OpenSSL parses. Replicated peer transport uses the same descriptor-to-memory boundary
for its shared inbound/outbound identity.

On `SIGHUP`, the signal handler writes only a `sig_atomic_t` request flag. The main thread consumes
it as the existing reactor owner, loads the complete next native bundle, and asks epoll to replace
TLS admission. Context construction completes before mutation. Failure retains the old authority,
context, and connections. Success closes incomplete handshakes, swaps admission generations, and
then releases the old authority owner; established sessions keep their original OpenSSL context and
stored principal and never borrow that authority again. The worker, queues, listener, database, and
Raft runtime remain live. Raft peer credentials are not part of this reload.

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
restricted to IPv4 loopback. Remote serving requires the complete native certificate, private-key,
trust-store, and client-principal bundle on epoll; empty or partial bundles, unsafe files, unknown
certificates, and TLS on io_uring fail closed. Invalid options and unavailable reactor backends fail
before the startup banner. Worker publication or reactor failures terminate the process with a
nonzero status. A native reload failure is observable but nonterminal because the complete previous
generation remains installed; its diagnostic names that `retained_generation`. A successful reload
reports its monotonic process-local generation.

The startup banner reports `data_plane=configured` or `data_plane=replicated` only after the
corresponding database path and reactor both start; otherwise the explicit unconfigured mode
remains distinguishable. Configured single-node ingest acknowledges
the exact requested/effective ASYNC or LOCAL_SYNC mode. Bootstrap and native DDL/DML identities use
the common nonnil system UUID source; deterministic service tests inject the same interface. If
initial WAL identity allocation fails after the final bootstrap becomes durable, the daemon reports
a contextual startup error and never emits a listening banner. A later ordinary start must reuse
that bootstrap rather than propose a replacement database. Failure while generating either proposed
bootstrap identity occurs earlier and must leave the supplied root empty.

An established `BOOTSTRAP` checksum failure is terminal for that start. `chronosd` reports the
database-start corruption before reactor admission and leaves the complete damaged descriptor
unchanged; it does not turn proposed startup identities into an implicit repair.

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
checks that the binary is packaged and its help path runs. A Linux-only fault child built from the
same daemon source wraps its exact 16-byte `getrandom` calls without changing the shipped binary.
After normal startup, its trigger fails the fifth CREATE identity candidate. The socket receives one
execution error; a normal restart observes no table, the next CREATE reports non-resumed completion,
and a second restart queries that cleanly installed table. A second trigger mode fails the third
candidate during initial WAL creation, after final bootstrap installation. That process reports the
entropy failure; the final bootstrap and subsystem directories remain, and two ordinary daemon
starts negotiate Protocol v1 and answer PING from the same root. Parameterized first- and
second-candidate cases prove database and metadata-group identity errors leave the root untouched;
the shipped daemon can then initialize it and answer PING. An ordinary-daemon corruption case flips
one covered bootstrap byte, requires the checksum diagnostic and nonzero exit, and compares the
complete damaged image after rejection. Its replicated case negotiates
Protocol 2, applies QUORUM_SYNC, queries the applied rows, restarts, verifies an exact retry, and
queries the same recovered row count. A separate replicated gate provisions three retained roots
and distinct mutual-TLS identities, starts three actual daemon processes, obtains an applied quorum
acknowledgement, kills the acknowledged tablet leader, observes a higher-term replacement, and
requires an exact matching retry. Reopening each root then proves that all three applications
recover the same two visible rows and one retry entry. A packaged-client gate starts `chronosd`
with mutual TLS and a strict client-principal allowlist, invokes the actual `chronosctl` binary, and
requires APPLIED followed by MATCHING_RETRY for the same canonical append. It then rotates the
server certificate/key, trust store, and allowlist through `SIGHUP`. The process matrix rejects a
malformed generation while the old client still receives MATCHING_RETRY, installs generation two,
then restores the original bundle as generation three. It rejects each superseded client and
requires each installed generation to receive the same MATCHING_RETRY. Reactor coverage separately
proves failed reload rollback, incomplete-handshake closure, and established-session continuity. It
deliberately does not claim native multi-group query failover: SELECT still needs one daemon to lead
every barrier group because client leader routing and remote fragments are not packaged. On non-
Linux hosts,
daemon/service build and durable-root initialization run, but the socket subprocess is not
registered because the server reactor is Linux-only.

Reviewers should ask: Which thread owns each queue endpoint? Which object outlives the reactor's
authenticator borrow? Can certificate admission be mistaken for node or placement authority? Can
saturation allocate elsewhere? Can liveness imply data readiness? What happens to active socket
work on `SIGTERM`? Which acknowledged durability mode applies? In configured single-node mode, the
last answer is the exact requested/effective ASYNC or LOCAL_SYNC mode; replicated mode may add the
exact Protocol 2 QUORUM_SYNC proof, and unconfigured mode has no acknowledgement.
