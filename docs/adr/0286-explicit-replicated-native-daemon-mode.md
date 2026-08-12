# ADR 0286: Explicit replicated native-daemon mode

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, networking, runtime, and operations maintainers

## Context and decision

Protocol 2 QUORUM_SYNC must not be advertised by the WAL-backed single-node daemon or by an
unconfigured process. The packaged lifecycle now has all local owners needed to serve it, but mode
selection, configuration file ownership, reactor wakeups, and shutdown order must be explicit.

`chronosd --data-dir ROOT --replicated-groups FILE` selects replicated-ingest mode. It is mutually
exclusive with the single-node subscription options. The daemon opens `FILE` without following a
final symlink, requires one bounded nonempty regular file, detects growth while reading, and passes
the exact bytes to the strict v1 parser. The database root must already be provisioned; an absent
valid bootstrap or Raft history fails startup rather than creating a replicated cluster implicitly.

After committed-metadata database recovery succeeds, every configured group whose voter set is
exactly the local node is elected synchronously. Multi-voter groups are not auto-elected; production
transport/election timing remains external. Only then does the daemon construct the queue-facing
replicated service and configure the reactor to negotiate Protocol 2 with only the QUORUM_SYNC
feature. Protocol 1 clients remain compatible. Query and subscription requests in this mode fail
explicitly because a Raft-backed query snapshot adapter is not yet packaged.

The data-plane worker exclusively polls the replicated queue adapter. Actual response enqueue wakes
the reactor; cancellation remains response-less. On signal, admission closes and the worker polls
through queued requests, finite coordinator deadlines, retained response publication, and external
response-ring drain. The daemon then joins the worker, shuts down the reactor, destroys the service,
drains/closes the asynchronous Raft runtime, and finally releases the database-root lock.

## Consequences and validation

The feature bit now accurately means that a complete replicated ingest owner is running. The
existing single-node `data_plane=configured` status and Protocol 1 behavior remain unchanged;
replicated mode reports `data_plane=replicated`.

A Linux-only process test provisions an actual committed metadata/replicated root, starts
`chronosd`, negotiates Protocol 2 over loopback, obtains an APPLIED QUORUM_SYNC acknowledgement,
terminates the process cleanly, restarts it, and obtains MATCHING_RETRY for the identical command.
On the current macOS host the daemon and service suites build, the process test passes a standalone
syntax check, and actual epoll execution remains a Linux gate.

Authenticated remote client serving, Raft peer transport composition, leader redirection, snapshot
directory options, multi-process failover, shutdown under continuous request injection, TLS,
metrics export, and Linux sanitizer/TSan runs remain deferred. No durable or network bytes change.

## References

- [Native server operations](../operations/native-server.md)
- [ADR 0271](0271-native-protocol-v2-quorum-sync-negotiation.md)
- [ADR 0283](0283-bounded-reactor-facing-replicated-ingest-service.md)
- [ADR 0284](0284-committed-metadata-replicated-database-recovery.md)
- [ADR 0285](0285-strict-replicated-group-deployment-config.md)
