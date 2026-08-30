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
barriers. With the peer/TLS bundle, committed metadata supplies a distinct private data endpoint per
node. Self-led fragments use the direct production worker; remote fragments use a dedicated
mutual-TLS listener and one client context per authenticated peer. One complete coordinator merges
both subsets before global ordering, LIMIT, and Native encoding. The barrier result remains a
stable per-group vector rather than a globally atomic cross-group instant.

The private query listener has its own joined poll thread because worker execution is synchronous
and the authenticated shared query-control owner can also issue one remote Raft read authority.
It authenticates a peer before selecting authority framing by frozen magic; the shared mutable
magic is exact-decoded before plan mode selects row or grouped sufficient-state response framing.
Authority service calls may wait on this thread only because the distinct Raft transport thread
continues driving their durable and quorum completion; query-control work must not delay Raft
transport polling. The heap-owned query bundle keeps the peer authority, TLS contexts, local row and
grouped workers, listener, and borrowed Native config address-stable. Its release/acquire stop and failure flags have
the same publication argument as the existing worker threads: stop is visible before loop exit, and
failure is visible before the main thread reports termination.

The same bundle exposes its installed grouped reducer-job service to the Native provider for a
coordinator-owned reducer. This is an in-process endpoint, not a loopback peer: it accepts only an
exact local coordinator/target tuple, while all frozen control and result protocols continue to
reject self-routes. A service mutex serializes local query-thread control/source/result handoff with
the private listener's poll thread. Mutex unlock/lock supplies the cross-thread happens-before edge,
and every contained TLS, reducer, retry, and resource owner remains exclusively progressed.

The packaged owner constructs both mutable workers and the per-group replicated authority adapter
before their receivers, and constructs the shared listener last. Destruction therefore closes TLS
and TCP ownership before any borrowed receiver dependency disappears. The daemon passes its existing
replicated barrier into this owner. The Native coordinator uses the remote authority client against
peer instances of that inbound owner.

Eligible direct grouped SQL uses the grouped worker for self-led tablets and the same committed
private endpoint for remote tablets. The Native query owner merges every sufficient-state stream,
then applies the checked final projection, global order/limit, and all-or-nothing encoding. Queries
with computed pre-group semantics continue through complete row exchange.

For distributed Native SELECT, the query thread now observes every resident group before each whole
attempt. Locally led groups use the local barrier; follower observations select the current remote
leader, whose committed private endpoint and immutable TLS peer context construct the authority
request. All remote connections start before local waits. Only the complete sorted local/remote
proof vector can pin the publication set and bind fragments. Cancellation and retry share the same
absolute query deadline, and retry re-observes every leader. The proof set is still per-group, not a
globally atomic cross-group instant.

The replicated queue adapter separately admits one joined Native query thread. This keeps its queue
owner available for an exact later `CANCEL`: the matching connection/request publishes a sticky
cooperative token, destroys live remote clients at the next scheduler poll, and suppresses the whole
response. A second query is rejected while the slot is occupied. Shutdown publishes cancellation
and waits for the query thread before releasing service or database owners. Local worker calls are
bounded but remain interruptible only at fragment boundaries.

Retryable local or remote authority failure now restarts the whole distributed attempt under the
same absolute deadline. The query thread reacquires every group barrier, pins a new coherent
publication set, and accepts replacement fragments/routes only when the full logical query and
tablet/group vector remain exact. Old local and remote messages are discarded together; current
serving nodes may repartition work between the direct worker and private TLS carrier.

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

Authenticated multi-voter startup also requires the committed local query endpoint to be canonical
IPv4 and to share the local Raft peer's address. It binds that exact port before the public listening
banner and reports `distributed_query=configured`; any missing advertisement, inconsistent address,
TLS-context error, bind error, or query poll failure fails closed. Peer/query credentials and the
private listener are fixed until restart and are not changed by public Native `SIGHUP` reload.

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

An established WAL segment-header CRC32C failure has the same admission result but belongs to WAL
recovery after bootstrap and metadata startup. It is complete corruption, not an incomplete final
tail: `chronosd` preserves the entire damaged segment and exits before listening.

A checksum-invalid complete application record is equally terminal even at the active segment end.
The process matrix creates that record through native CREATE/SQL INSERT, damages its body, and
requires the full-record CRC32C failure without truncation or socket admission.

The packaged single-node configuration also does not authorize repair of a genuinely incomplete
final tail. A one-byte suffix after a valid active header produces the explicit-repair diagnostic;
the daemon exits before listening and preserves the suffix for an intentional recovery workflow.

A metadata-Raft segment-header checksum failure stops even earlier, after Bootstrap v1 validation
but before catalog projection or WAL recovery. The daemon reports the exact Raft corruption, exits
before listening, and preserves the complete damaged segment. The adjacent record case leaves the
multiplexed header valid, damages its payload, and requires the payload-checksum diagnostic with the
same segment preservation. The adjacent one-byte suffix case requires `Raft final record is
incomplete`; the packaged no-repair policy exits without truncation. An unrecognized synchronized
regular file in the Raft directory produces the exact unknown-entry failure; startup leaves that
file and the established segment unchanged. A symlink to the established segment produces the
non-regular-entry failure and remains an unchanged link after process rejection.
The anchor case uses the public persistence owner to checkpoint the daemon's exact recovered
metadata state, then proves the daemon accepts that authority once. After a covered-byte mutation,
startup reports the anchor-checksum failure and preserves both anchor and retained segment.

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
complete damaged image after rejection. The adjacent WAL case corrupts a covered active-header byte
and requires the exact CRC32C diagnostic plus complete-segment preservation. A complete-record case
corrupts a packaged SQL INSERT body and proves the active segment is not truncated. A one-byte
incomplete-tail case proves the packaged no-repair policy preserves the suffix. A metadata-Raft
header and complete-record pair prove the complete segment remains unchanged. A one-byte Raft-tail
case proves the no-repair policy preserves the suffix. An unknown-entry case proves the packaged
owner performs no speculative namespace cleanup; a symlink case proves it does not follow a
non-regular entry. A recovery-anchor case proves no fallback from a damaged highest authority. Its
replicated case negotiates Protocol 2,
applies QUORUM_SYNC, queries the applied rows, restarts, verifies an exact
retry, and queries the same recovered row count. A separate replicated gate provisions three
retained roots, committed query endpoints, and distinct mutual-TLS identities, starts three actual
daemon processes, obtains an applied quorum acknowledgement, executes a distributed SELECT from a
nonleader, kills the acknowledged tablet leader, observes a higher-term replacement, requires an
exact matching retry, and executes the same SELECT from the remaining nonleader through the new
leader. Reopening each root then proves that all three applications recover the same two visible
rows and one retry entry. A packaged-client gate starts
`chronosd` with mutual TLS and a strict client-principal allowlist, invokes the actual `chronosctl`
binary, and requires APPLIED followed by MATCHING_RETRY for the same canonical append. It then
rotates the server certificate/key, trust store, and allowlist through `SIGHUP`. The process matrix
rejects a malformed generation while the old client still receives MATCHING_RETRY, installs
generation two, then restores the original bundle as generation three. It rejects each superseded
client and requires each installed generation to receive the same MATCHING_RETRY. Reactor coverage
separately proves failed reload rollback, incomplete-handshake closure, and established-session
continuity. It qualifies one-tablet remote-fragment query routing and leader failover, not globally
atomic cross-group time, dynamic endpoint changes, aggregate distribution, or mid-query
cancellation. On non-Linux hosts,
daemon/service build and durable-root initialization run, but the socket subprocess is not
registered because the server reactor is Linux-only.

Reviewers should ask: Which thread owns each queue endpoint? Which object outlives the reactor's
authenticator borrow? Can certificate admission be mistaken for node or placement authority? Can
saturation allocate elsewhere? Can liveness imply data readiness? What happens to active socket
work on `SIGTERM`? Which acknowledged durability mode applies? In configured single-node mode, the
last answer is the exact requested/effective ASYNC or LOCAL_SYNC mode; replicated mode may add the
exact Protocol 2 QUORUM_SYNC proof, and unconfigured mode has no acknowledgement.
