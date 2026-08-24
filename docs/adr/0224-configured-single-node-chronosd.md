# ADR 0224: Configured Single-Node chronosd

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB runtime, service, networking, and operations maintainers

## Context

`chronosd` packaged the Linux native reactor but deliberately rejected every data-plane request.
The recoverable database owner and native ingest/SELECT/CREATE adapter now provide a real durable
single-node boundary. Composing them must preserve the reactor's single-producer/single-consumer
queue ownership, bounded response buffering, secure identity creation, and ordered shutdown.

## Decision

`chronosd --data-dir PATH` opens an existing directory, proposes secure random database and
metadata-group UUIDs, and opens or creates `SingleNodeDatabase`. Existing Bootstrap v1 state remains
authoritative on restart. OpenSSL `RAND_bytes` supplies nonnil candidate UUIDs; the bootstrap pair
is required to differ, and the same generator is injected into native CREATE TABLE dispatch.

The configured worker synchronously dispatches ingest and query tasks through
`NativeProtocolService`. It retains the service's already bounded multi-frame query sequence and
publishes one frame at a time through the response SPSC queue. Queue saturation retains exactly one
owned pending frame. It consumes no later request until the sequence is fully published, preserving
per-request result order. Subscription requests return an explicit execution failure; CANCEL is
currently consumed as an idempotent lifecycle notification but cannot interrupt synchronous work.

Without `--data-dir`, the existing explicit unconfigured behavior remains available for transport
qualification. The startup banner reports `data_plane=configured` only after database and reactor
startup. Shutdown joins the worker, closes reactor sockets, then drains/closes WAL, metadata Raft,
and the database-root lock.

## Consequences

On Linux, the packaged daemon can create/reopen a durable database root and serve native CREATE TABLE,
canonical ingest, and supported vector SELECT requests on loopback. Default bootstrap limits are
finite development values, not measured capacity recommendations. Plaintext remains loopback-only;
the CLI still does not expose production TLS/authentication configuration.

The worker is single-threaded and synchronous. A long WAL wait or query delays later tasks and makes
queued CANCEL unable to interrupt it. Query responses are bounded but accumulated before queueing.
Configured single-plan live subscriptions were composed later by
[ADR 0242](0242-configured-chronosd-subscription-lifecycle.md). Metrics export and multi-node
ownership remain outside this step. Native SQL INSERT was composed by
[ADR 0226](0226-native-sql-insert-dispatch.md).

## Validation

The daemon and configured service build on macOS; a local launch creates a valid bootstrap, metadata
Raft log, and WAL before correctly failing because the repository's server reactor is Linux-only.
The Linux-only subprocess test negotiates a real socket, creates a table, queries it, shuts down,
restarts the same data directory, and queries the recovered catalog. A test-only daemon built from
the same `chronosd` source link-wraps only its 16-byte `getrandom` calls. A trigger armed after
startup fails the fifth CREATE identity read, returns an execution error over the real socket, and
then normal packaged-daemon restarts prove that no metadata prefix or table was installed before a
fresh non-resumed CREATE and another successful restart. The same child can fail the third qualified
read: the WAL identity allocation after durable Bootstrap v1 installation and metadata-Raft
startup. The process exits with its contextual startup error while the final bootstrap and
`wal/`/`raft/` directories remain; two starts of the shipped daemon then reopen the same root,
negotiate Protocol v1, and answer PING. Failures of the first or second qualified read cover the
proposed database and metadata-group identities: each exits before root mutation, after which the
shipped daemon initializes that untouched directory and answers PING. The shipped daemon has no
entropy-fault option. A separate shipped-daemon case first creates an established root, durably
damages one checksum-covered bootstrap byte, and then requires startup to report the exact checksum
corruption, exit before its listening banner, and preserve the complete damaged image byte-for-byte.
Another case damages the active WAL segment header, requires its exact CRC32C diagnostic and the
same pre-listen exit, and preserves the complete segment byte-for-byte. The existing unconfigured
PING/rejection subprocess case remains. A complete-record case first creates the record through
native SQL INSERT, then damages its body and requires the full-record CRC32C diagnostic, pre-listen
exit, and byte-for-byte segment preservation. The incomplete-tail case appends one byte at the clean
active end and requires the explicit-repair diagnostic, nonzero exit, and unchanged segment because
the packaged configuration does not authorize truncation. A metadata-Raft case damages the active
segment header, requires its exact checksum diagnostic and pre-listen exit, and preserves the
complete segment byte-for-byte. A complete metadata-record case damages the multiplexed payload,
requires its exact checksum diagnostic and the same pre-listen exit, and preserves the segment. A
one-byte Raft tail case requires the exact incomplete-record diagnostic and proves no truncation.
An unknown regular Raft-directory entry requires its exact namespace diagnostic and the same
pre-listen exit while preserving both that entry and the established segment. A symlink to the
segment requires the non-regular-entry diagnostic and proves no link following or cleanup. A real
recovery-anchor case requires its exact checksum diagnostic and preserves the damaged anchor plus
retained checkpoint segment.

## References

- [ADR 0213](0213-packaged-native-daemon-lifecycle.md)
- [ADR 0220](0220-native-protocol-ingest-service-adapter.md)
- [ADR 0222](0222-bounded-native-vector-query-results.md)
- [ADR 0223](0223-native-create-table-dispatch.md)
