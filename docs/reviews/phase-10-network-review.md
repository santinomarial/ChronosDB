# Phase 10 Native Network Exit Review

## Decision

**PASS WITH DOCUMENTED NONBLOCKING RISKS**

This decision advances development beyond Phase 10; it does not declare ChronosDB production ready.
The accepted boundary is an embeddable bounded networking library, not a packaged database daemon.

## Accepted implementation boundary

Protocol v1 has a fixed 40-byte checksummed header, independently checksummed bounded payload, and
strict version/type/flag/length validation before peer-controlled allocation. The implemented
message and connection state machines cover negotiation, ingest with explicit durability,
durability acknowledgement, query, bounded self-describing result batches, cancellation, errors,
and terminal completion. The portable client session owns partial reads and writes and validates the
same lifecycle from the peer side.

Connection buffers bound inbound bytes, outbound bytes, and queued frames independently. Complete
owned tasks cross a fixed-capacity reactor-to-shard SPSC ring with the documented release/acquire
publication and reuse edges. Saturation rejects explicitly; it does not create a hidden queue.

On Linux, one `EpollReactor` owner controls nonblocking descriptors, finite connection and event
admission, handshake and idle deadlines, bounded work per event, readable-before-half-close
dispatch, deterministic detach cancellation, and late-response rejection. Shard responses signal
an `eventfd`, waking a blocked reactor without polling. Large partial writes retain immutable frame
ownership and exact credit until completion. Accepted sockets require `TCP_NODELAY`; benchmark
diagnosis showed that leaving Nagle enabled delayed separately owned result and terminal frames.

Plaintext is restricted to IPv4 loopback. Authentication uses a borrowed, owner-lifetime
authenticator and propagates stable principal identity with request and cancel tasks. `TLS_REQUIRED`
fails startup because no maintained TLS record backend exists; there is no downgrade.

## Correctness and resource evidence

The accepted tests cover golden and corrupt packets, every truncation boundary, fragmented and
coalesced reads, short writes, request identity and cancellation, slow handshake expiry, bounded
connection admission, queue saturation and shard stall, response wakeup, half-close, 128-connection
churn, late results, and an 8 MiB response. Allocation-failure executables classify owned
construction/codec failures. A dedicated libFuzzer target covers framing, stream extraction,
payload decoders, server state, and client state.

The completed Ubuntu 24.04 gate uses GCC 13.3 with repository warnings as errors. The full build and
all 932 discovered tests pass; an explicit Linux networking run passes 31 tests, including all eight
real-socket epoll cases. The portability audit also removed copied structured bindings, made
small-unsigned shifts explicit, used the Zstandard 1.5.5-compatible public frame-header spelling,
fixed test aggregate construction, retained temporary result owners, and isolated API-owned
allocation injection from libstdc++ string argument construction. These repairs do not alter WAL,
CSEG, manifest, or protocol bytes.

Previously completed evidence remains applicable: the full local repository suite and
repository-wide clang-tidy passed before the portability-only resume; ASan/UBSan, focused TSan,
100,000-input network fuzzing, install/external-consumer checks, and the hostile Linux epoll matrix
passed. The resumed gate reran affected local targets/tests, formatting, `git diff --check`, the
complete Ubuntu build/tests, and the explicit Linux network suite rather than repeating unchanged
sanitizer, fuzz, and benchmark campaigns.

## Measurement evidence

The [Phase 10 native-network baseline](../benchmarks/native-network-phase-10.md) retains three raw
Ubuntu/LinuxKit aarch64 repetitions for frame/result codecs, fragmented receive, SPSC operation and
saturation, connection churn, and equal-work 1/8/32-connection request rounds. It reports allocation
counts, exact configuration, host noise, unfavorable outliers, and omitted metrics. It is a
microbenchmark baseline, not a throughput promise or regression threshold.

## Nonblocking risks and deferred work

- The server is library code without a production daemon, service configuration layer, or direct
  storage/query execution adapter.
- Remote serving is unavailable because plaintext is loopback-only and no maintained TLS backend is
  implemented.
- The published Linux evidence is an arm64 Docker/LinuxKit baseline on a non-isolated host; it does
  not establish production capacity, tail-latency objectives, or the full future support matrix.
- Protocol v1 has no compatibility history from deployed releases. New message semantics require
  the normal registry/version/ADR discipline.
- `io_uring`, distributed routing, subscriptions, materialized views, system-time history, Raft,
  and all later-phase functionality remain unimplemented.

None of these risks weakens the accepted Phase 10 bounded transport semantics. A future remote
deployment or production daemon must satisfy its own security, operations, integration, soak, and
support-matrix gates.
