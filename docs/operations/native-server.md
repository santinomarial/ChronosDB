# Native server operations baseline

The native transport remains an embeddable library component. `chronosd` packages its process,
bounded-queue, signal-shutdown, and native-socket lifecycle. Linux is authoritative. Embedders create
bounded request/response rings, start one
`EpollReactor`, and drive `poll_once` from its single owner thread. Port zero is for ephemeral tests;
deployments should bind explicitly.

Run `chronosd --help` for bounded startup options. The binary accepts plaintext only on `127.0.0.1`.
Diagnostics use the existing human-readable form by default. `--log-format json` emits one bounded,
flushed JSON object per startup or error event with an RFC 3339 UTC timestamp, severity, component,
stable event name, message, and event-specific string fields. JSON is written to the same stream as
the corresponding text event: startup on stdout and errors on stderr. Invalid UTF-8 is replaced so
one damaged diagnostic cannot corrupt the JSON stream. Option-parse failures honor a requested JSON
format and do not append human usage text; `--help` remains human-readable stdout.

Without `--data-dir` it reports `data_plane=unconfigured` and explicitly rejects data work. With
`--data-dir PATH` it initializes or reopens an existing directory as a durable single-node root and
reports `data_plane=configured`; native CREATE TABLE, single-local-tablet SQL INSERT VALUES,
canonical ingest, and supported vector SELECT execute. Without subscription options, subscriptions
fail explicitly. To serve one durable row-preserving plan, add both `--subscription-sql SQL` and
`--subscription-key-file PATH`. The table must already exist. The key file must contain exactly 32
nonzero bytes and be inaccessible to group/other; preserve the same secret across restarts or old
resume tokens will fail authentication. The daemon reports `subscriptions=configured` only after
the plan, coordinator, snapshot context, internal queues, and applied-append observer are ready. SQL
INSERT acknowledges only after `LOCAL_SYNC`, but its query envelope has no durable client retry key;
use canonical ingest when an ambiguous response must be retried without duplicating rows. `SIGINT`
and `SIGTERM` request orderly worker join, reactor shutdown, WAL drain, Raft close, and root-lock
release. Active configured subscriptions receive resumable server-shutdown termination while the
reactor is still draining responses.

For an already provisioned replicated root, add `--replicated-groups FILE`. The strict file format
is documented in [Replicated Group Configuration](replicated-group-config.md). This mode reports
`data_plane=replicated`, reconstructs resident tablet owners from committed metadata, automatically
elects only groups whose sole voter is the local node, and advertises Protocol 2 QUORUM_SYNC only
after all owners are running. It serves canonical replicated ingest and exact retries. Native query
SELECT confirms a current-term quorum read index for the metadata group and every resident tablet
group, requires each immutable application publication to cover its index, and then emits the normal
bounded result sequence. A partially resident table fails explicitly instead of returning a local
subset. The resulting component vector is not a globally atomic cross-group instant. Replicated
CREATE, SQL INSERT, ASOF, historical query, and subscription requests remain explicit errors.
Multi-voter deployments additionally require the authenticated Raft peer transport bundle below;
the group file itself contains no endpoints or credentials.

Replicated mode also advertises Protocol 2 leader redirect. A canonical ingest received by a stable
follower returns the exact committed tablet group, ordered observed leader/term, and current
placement epoch when that leader remains inside stable placement. Candidate/unknown leadership or
reconfiguration returns an error. The node ID is not a network address: clients need an explicit
authenticated native-endpoint map. The client library's bounded redirect router joins that map,
rejects regressing or contradictory authority, and caps retry selection. Its portable QUORUM_SYNC
owner retains the exact append and creates a fresh Protocol 2 session/request ID for each selected
route. A TCP/TLS carrier and deployment parser must still connect, drive readiness/deadlines, and
handle reconnect events. Multi-group SELECT is not redirected to one arbitrary group leader.

For multi-voter groups, configure the complete transport bundle:

```text
--replicated-peers /etc/chronosdb/peers.conf \
--raft-tls-cert /etc/chronosdb/node.pem \
--raft-tls-key /etc/chronosdb/node-key.pem \
--raft-tls-ca /etc/chronosdb/cluster-ca.pem
```

The [Replicated Peer Configuration](replicated-peer-config.md) must include the local node and every
voter of every resident group. The key file must not be accessible to group or other. All four
options are atomic at startup; partial configuration is rejected. `raft_transport=configured` in
the startup line means the authenticated poll owner is running, while `raft_transport=local` means
only exact local single-voter groups were accepted. Non-replicated modes report
`raft_transport=disabled`. Transport failure stops the daemon. Read barriers are handled by the
bounded query gate; Raft snapshot installation remains a fail-closed gap rather than a silently
discarded completion.

The repository's Linux process qualification uses one CA and a distinct certificate/key for each
of three loopback nodes. It proves authenticated election, QUORUM_SYNC application, abrupt tablet-
leader loss, higher-term retry deduplication, orderly survivor shutdown, and identical recovery from
all retained roots. It does not qualify deployment DNS, certificate rotation, packet faults,
failover latency, snapshot transfer, rolling upgrades, or native SELECT when metadata and tablet
groups have different leaders. Operators must not treat `raft_transport=configured` as evidence
that those broader gates have passed.

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
