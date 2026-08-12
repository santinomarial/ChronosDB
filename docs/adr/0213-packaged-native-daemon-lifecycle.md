# ADR 0213: Packaged Native Daemon Lifecycle

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB runtime and networking maintainers

## Context

The native Protocol v1 reactor was available only as an embedding library. This prevented process
lifecycle, packaging, signal shutdown, and real socket boundaries from being exercised without a
test-specific owner. The database data plane is not yet composed behind that reactor, so a daemon
must not imply that ingest, query, or subscription execution is available.

## Accepted decision

`chronosd` is the packaged owner of one bounded native reactor, one request SPSC queue, one response
SPSC queue, and one consumer worker. It exposes plaintext only on IPv4 loopback and defaults to the
reference epoll backend. The reactor owns handshake and PING/PONG. Until a durable data-plane owner
is configured, every admitted ingest, query, or subscription request receives one terminal
`EXECUTION_FAILURE` saying that the data plane is not configured. No request receives placeholder
success data.

The reactor thread is the sole request producer and response consumer. The worker thread is the sole
request consumer and response producer. On response-ring saturation it retains exactly one owned
response and retries without allocating a side queue. Release/acquire publication remains the SPSC
contract accepted by ADR 0063. `SIGINT` and `SIGTERM` set only a signal-safe stop flag; normal control
flow joins the worker and shuts down socket ownership.

## Detailed rationale

Packaging the accepted transport boundary makes process integration testable now without creating a
second protocol stack or fabricating storage behavior. An explicit terminal error keeps clients and
operators aware that liveness is not data-plane readiness.

## Alternatives considered

- A listener without a queue consumer would saturate and misrepresent readiness.
- Returning empty successful query or ingest results would be a fake implementation.
- Wiring unrelated in-memory smoke-test objects into the daemon would not provide a recoverable
  database owner and would create a misleading persistence contract.

## Consequences

The installed package contains a process suitable for protocol/lifecycle qualification, but it is
not yet a usable database service. Remote binding remains unavailable until immutable TLS and
authentication configuration is exposed. The next integration must replace the unconfigured worker
with a durable runtime implementing the existing request/response contracts.

## Affected invariants

This decision supports invariants 4, 14, and 15 through single-consumer dispatch, unchanged Protocol
v1 bytes, and bounded queue influence. It does not claim acknowledged-write durability.

## Validation plan

Linux subprocess tests start the installed-shape binary on an ephemeral loopback port, negotiate a
real Protocol v1 socket, verify PING/PONG, verify explicit query rejection, and stop it with
`SIGTERM`. Packaging tests execute the installed binary's help path.

## Deferred decisions

Durable database configuration, ingest/query/subscription dispatch, TLS credential configuration,
metrics export, privilege dropping, service-manager units, and multi-process cluster ownership.

## Migration or reversal implications

CLI defaults may change before release. Protocol bytes and reactor queue semantics remain governed
by their existing ADRs and require their normal compatibility process.

## References

- [ADR 0004](0004-thread-ownership-and-ingress-concurrency.md)
- [ADR 0063](0063-bounded-reactor-shard-spsc-routing.md)
- [ADR 0064](0064-bounded-linux-epoll-reactor.md)
- [Native Protocol v1](../protocol/native-v1.md)
- [Native server operations](../operations/native-server.md)
