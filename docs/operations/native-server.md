# Native server operations baseline

The native transport remains an embeddable library component. `chronosd` packages its process,
bounded-queue, signal-shutdown, and native-socket lifecycle. Linux is authoritative. Embedders create
bounded request/response rings, start one
`EpollReactor`, and drive `poll_once` from its single owner thread. Port zero is for ephemeral tests;
deployments should bind explicitly.

Run `chronosd --help` for bounded startup options. Without a native TLS bundle, the binary accepts
plaintext only on IPv4 loopback and reports `native_transport=plaintext`. With the atomic
[Native Server Principal Configuration](native-server-principal-config.md) and TLS bundle, epoll
serves mutual TLS on one canonical nonzero IPv4 bind address and reports `native_transport=tls`.
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
fail explicitly. CREATE obtains its complete nonnil, unique identity set before the first metadata
proposal. Operating-system entropy failure returns an execution error and leaves no partial durable
table prefix; retry is a new operation. During initial root startup, an entropy failure after the
final bootstrap is installed but before the WAL identity exists terminates startup with an error.
Leave the root intact: the next normal start reuses the checksummed bootstrap and completes the empty
WAL/Manifest initialization. If startup instead fails while proposing the database or metadata-group
identity, no bootstrap work has begun and the dedicated root remains empty; a normal retry may
initialize it. A checksum failure in an established `BOOTSTRAP` is not restartable creation:
startup preserves the damaged descriptor and fails before listening. Do not delete it or allow newly
proposed identities to replace it; diagnose the storage failure and recover the database root from a
trusted, identity-consistent backup. A WAL segment-header checksum failure is also terminal and is
never eligible for incomplete-tail repair. Startup preserves that segment; diagnose and restore an
identity-consistent root rather than editing a checksum or deleting WAL history. The same applies to
a complete final record with a bad header or full-record checksum: do not authorize tail repair or
truncate it merely because it is last. A genuinely incomplete final tail is repairable only through
an explicit recovery workflow; packaged `chronosd` does not authorize that mutation and preserves
the suffix while refusing startup. A metadata-Raft segment checksum failure also aborts before
listening and is never repaired automatically. Preserve the root and restore a complete
identity-consistent database rather than editing or deleting Raft history. The same applies to a
complete multiplexed record payload checksum failure; it is not a repairable tail. A genuinely
incomplete final Raft suffix is repairable only with explicit log-open authorization; packaged
`chronosd` does not opt in and preserves that suffix while refusing startup. Any unrecognized Raft
directory entry is corruption, not cleanup residue: preserve the root for diagnosis because
packaged startup fails before listening and does not remove the entry. This includes symlinks,
which recovery classifies without following. A damaged highest Raft recovery anchor is likewise
terminal, including when truncated: do not recreate it or restore an older anchor alone, because the
selected retained base and anchor are one authority. An anchor with a valid checksum but invalid
magic, version, layout, identity, checkpoint-range, count, or reserved fields is equally terminal.
A missing highest anchor after prefix reclamation is also terminal; do not adopt the lowest visible
segment as a new base. An intact anchor with a damaged retained segment is also terminal and must not
trigger fallback or repair. This includes a structurally incomplete record inside the anchored
checkpoint even when ordinary final-tail repair is explicitly authorized. Restore a root with
consistent identity. To serve one durable row-preserving plan, supply
the paired `--subscription-sql SQL` and `--subscription-key-file PATH` options. The table must
already exist.
The key file must contain exactly 32 nonzero bytes and be inaccessible to group/other; preserve the
same secret across restarts or old resume tokens will fail authentication. The daemon reports
`subscriptions=configured` only after the plan, coordinator, snapshot context, internal queues, and
applied-append observer are ready. SQL
INSERT acknowledges only after `LOCAL_SYNC`, but its query envelope has no durable client retry key;
use canonical ingest when an ambiguous response must be retried without duplicating rows. `SIGINT`
and `SIGTERM` request orderly worker join, reactor shutdown, WAL drain, Raft close, and root-lock
release. With native mutual TLS configured, `SIGHUP` transactionally reloads only its complete
certificate, key, trust store, and principal authority; without that bundle it is a logged no-op.
Successful reload diagnostics name the installed `generation`; failures name the unchanged
`retained_generation`.
Active configured subscriptions receive resumable server-shutdown termination while the reactor is
still draining responses.

## Single-node SQL quickstart

The following copy/paste sequence uses the development build, a fresh root, and the plaintext
loopback server. It creates a table, inserts two `LOCAL_SYNC` rows, reads them, stops and restarts the
daemon, and reads the recovered rows:

```sh
cmake --build build --target chronosd chronosctl
CHRONOS_DEMO_DIR=$(mktemp -d /tmp/chronosdb-sql-demo.XXXXXX)
CHRONOS_DEMO_PORT=7777
./build/chronosd --data-dir "$CHRONOS_DEMO_DIR" --port "$CHRONOS_DEMO_PORT" \
  >"$CHRONOS_DEMO_DIR/chronosd.log" 2>&1 &
CHRONOS_DEMO_PID=$!
until grep -q "chronosd listening" "$CHRONOS_DEMO_DIR/chronosd.log"; do sleep 0.1; done

./build/chronosctl sql --host 127.0.0.1 --port "$CHRONOS_DEMO_PORT" --execute \
  "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price DECIMAL(20, 8) NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY (symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL '7 days' ALLOWED LATENESS INTERVAL '0 seconds'"
./build/chronosctl sql --host 127.0.0.1 --port "$CHRONOS_DEMO_PORT" --execute \
  "INSERT INTO trades VALUES (TIMESTAMP '2026-08-24 12:00:00Z', CAST('AAPL' AS SYMBOL), CAST(227.16000000 AS DECIMAL(20,8)), 'opening row'), (TIMESTAMP '2026-08-24 12:00:01Z', CAST('MSFT' AS SYMBOL), CAST(504.26000000 AS DECIMAL(20,8)), NULL)"
./build/chronosctl sql --host 127.0.0.1 --port "$CHRONOS_DEMO_PORT" --execute \
  "SELECT count(*) AS rows FROM trades"

kill -TERM "$CHRONOS_DEMO_PID"
wait "$CHRONOS_DEMO_PID"
./build/chronosd --data-dir "$CHRONOS_DEMO_DIR" --port "$CHRONOS_DEMO_PORT" \
  >"$CHRONOS_DEMO_DIR/chronosd-restarted.log" 2>&1 &
CHRONOS_DEMO_PID=$!
until grep -q "chronosd listening" "$CHRONOS_DEMO_DIR/chronosd-restarted.log"; do sleep 0.1; done
./build/chronosctl sql --host 127.0.0.1 --port "$CHRONOS_DEMO_PORT" --execute \
  "SELECT count(*) AS rows FROM trades"
kill -TERM "$CHRONOS_DEMO_PID"
wait "$CHRONOS_DEMO_PID"
```

`chronosctl sql` executes exactly one statement per process through the existing Protocol v1 client
session and prints tab-separated column names and rows. Tabs, newlines, carriage returns,
backslashes, and other ASCII control bytes in text are escaped; binary values use lowercase hex.
The command is intentionally limited to the literal plaintext address `127.0.0.1`, uses a five
second timeout for each socket send or receive, and does not expose the mutual-TLS, distributed
routing, subscription, or retry-safe canonical-ingest surfaces. A SQL INSERT response lost after
the server commits remains ambiguous and must not be blindly retried.

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
route. The TCP client carrier now drives nonblocking connect, mutual TLS, principal-to-node
authorization, bounded partial I/O, per-phase deadlines, and redirect reconnect. Its execution
owner schedules the descriptor, caps kernel waits by phase and whole-operation deadlines, and
provides explicit cancellation and terminal metrics. The strict
[Native Client Route Configuration](native-client-route-config.md) parser and immutable authority
now build the endpoint/TLS-identity/certificate-principal map without borrowing Raft transport
configuration. Its address-stable TLS-route owner securely qualifies the file/credentials,
constructs one client context per route, and publishes the complete borrowed map. Embeddings must
still supply request-specific group/initial-node/placement authority. `chronosctl quorum-sync`
supplies that command composition and can target packaged `chronosd` when its mutual-TLS server
bundle authorizes the client's leaf certificate. Multi-group SELECT is not redirected to one
arbitrary group leader. A negotiated finite SELECT is redirectable only when its table has one
exact group/epoch/replica route and ordered observations show that every group in the packaged read
gate has the same stable remote leader. Split leadership and multi-group table placement fail
closed pending remote fragments.

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
protocol errors; TLS reload successes/failures and handshake closures; and bytes. Sustained rejects,
overloads, or drops indicate inadequate capacity or shard latency. Accepted sockets use
`TCP_NODELAY`; failure to set it rejects admission. Do not raise a bound without measuring retained
memory. Plaintext binds only to IPv4
loopback. Remote epoll serving requires `TLS_REQUIRED`, an explicit certificate chain and private
key, an explicit trust store, mandatory client certificates, and a borrowed authenticator that maps
each verified certificate SHA-256 fingerprint to a stable nonzero principal. Invalid credentials
fail startup; handshake or authorization failure closes the connection without protocol dispatch.
The io_uring backend returns `NOT_SUPPORTED` for TLS rather than downgrading.

Shutdown closes every socket, detaches active work, and clears buffers. Stop shard response
production before destroying queues. The embedding owns any configured authenticator and must keep
it alive until reactor shutdown or a successful owner-thread replacement has closed every handshake
that could still call it. `reload_tls_security` validates and constructs a complete replacement
before mutation. A failed attempt preserves every connection and the current generation; success
closes incomplete handshakes, keeps established principals/sessions, and applies the new immutable
context and authority only to new admissions. The packaged daemon exposes that operation through
`SIGHUP`. Raft peer and native-client route credential rotation remain separate restart work.
