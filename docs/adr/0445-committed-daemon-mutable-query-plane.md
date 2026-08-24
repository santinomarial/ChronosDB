# ADR 0445: Committed daemon mutable query plane

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB daemon, service, query, Raft, networking, and security maintainers
- **Extends:** [ADR 0290](0290-packaged-authenticated-raft-peer-transport.md),
  [ADR 0444](0444-proof-revalidated-local-and-remote-native-row-merge.md)

## Context

The Native service could merge local and remote proof-bound tablet fragments, but `chronosd` did
not own the private worker listener, per-node TLS client contexts, local worker, or their poll
lifecycle. Reusing the public Native client listener would conflate end-user principals with
cluster-node authority. Sharing the Raft transport thread would let a synchronous tablet scan delay
heartbeats and elections.

## Decision

A replicated daemon with the complete authenticated Raft peer bundle also composes the mutable row
query plane:

1. one startup snapshot must contain every configured peer's committed data endpoint;
2. each endpoint must be canonical IPv4 and use the same address as that authenticated Raft peer
   advertisement, while its port remains a distinct committed data-plane port;
3. a separate immutable `ReplicatedPeerAuthority` supplies exact certificate/IP/node
   authentication and authorization for query carriers;
4. the already loaded descriptor-bound Raft PEM bundle constructs one query client context per
   peer and the private mutual-TLS worker listener;
5. the database supplies both the direct local worker and every inbound request's fresh
   proof-revalidated context; and
6. a dedicated query-listener thread polls synchronous worker execution independently of the Raft
   transport and public Native reactor threads.

The public Native service borrows one stable config from a heap-owned daemon bundle. Its TLS-context
span and worker/authority addresses cannot move. Startup emits `distributed_query=configured` only
after the committed listener is bound and all client contexts exist. Replicated single-node mode
without a peer/TLS bundle retains the local complete-residency query path and reports
`distributed_query=local`; other modes report `disabled`.

Shutdown first drains and joins public client work, then stops and joins the private query listener
and closes it, then releases the service/config bundle, read barrier, Raft transport, and database.
Any private listener poll failure is terminal for the daemon. SIGHUP continues to rotate only the
public Native admission bundle; cluster peer/query credentials require restart.

## Consequences

One daemon no longer needs to lead every selected tablet. It obtains correlated all-group read
authority, executes self-led fragments locally, routes other fragments to the committed leader data
endpoints over mutual TLS, and publishes one globally finalized Native result. Missing local node
metadata, invalid or address-inconsistent endpoints, TLS-context failure, bind failure, incomplete
ownership, or poll-thread failure prevents or terminates serving rather than downgrading security.

The startup snapshot fixes the listener endpoint and peer credentials for the process lifetime.
Later committed node-endpoint changes can make fresh queries unavailable until an orderly restart;
dynamic listener/credential replacement is not inferred. No durable or network format changes.

The worker provider and query snapshot acquisition use the database's existing thread-safe durable
submission and immutable publication boundaries. Each server, client scheduler, and worker remains
owned by one thread; the only new cross-thread state is stop/failure flags using release stores and
acquire loads. A stop publication therefore happens-before loop termination, and a failure
publication happens-before the main thread's terminal observation.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): only the currently proved tablet leader serves a
  fragment.
- [Invariant 6](../architecture/invariants.md): committed node routing and correlated read authority
  remain pinned through query preparation.
- [Invariant 11](../architecture/invariants.md): the listener closes before its borrowed database,
  worker, authority, and TLS owners.
- [Invariant 14](../architecture/invariants.md): the distinct mutable query protocol remains
  mutually authenticated and independently checksummed.
- [Invariant 15](../architecture/invariants.md): listener admission, carriers, scans, responses,
  deadlines, and threads retain finite configured bounds.
- [Invariant 18](../architecture/invariants.md): no local shortcut, insecure fallback, or partial
  cross-tablet output is introduced.

## Validation

The Linux three-daemon process gate now provisions three committed query endpoints in addition to
three Raft endpoints. It requires each daemon to report the query plane configured, applies one
QUORUM_SYNC batch, queries from a nonleader through the remote worker, kills the tablet leader,
obtains the exact higher-term matching retry, and queries again from the remaining nonleader through
the replacement leader before retained-root recovery. On the current Darwin host the Linux target
is not generated; the complete process source is syntax-compiled, while the daemon, service tests,
installed consumer, formatting, and sanitizer-capable local suites remain executable.

## Migration and rollback

Every multi-voter deployment using the peer bundle must commit one reachable data endpoint per node
before restart. The existing peer certificate must authenticate both Raft and private query TLS
roles. Rollback removes the private query bundle/thread and restores the prior requirement that
packaged queries be locally executable; it does not change stored metadata or wire bytes.

## References

- [Packaged authenticated Raft peer transport](0290-packaged-authenticated-raft-peer-transport.md)
- [Proof-revalidated local and remote Native row merge](0444-proof-revalidated-local-and-remote-native-row-merge.md)
- [Packaged native daemon lifecycle](../learning/packaged-native-daemon.md)
- [Native server operations](../operations/native-server.md)
