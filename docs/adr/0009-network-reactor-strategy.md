# ADR 0009: Network Reactor Strategy

- **Status:** accepted
- **Date:** 2026-08-01
- **Owners:** ChronosDB networking and protocol maintainers

## Context

The server must handle many ingest, query, and subscription connections while bounding memory and preventing socket progress from mutating tablet state. Binary framing must remain fuzzable and portable even though the production server uses Linux-specific readiness APIs. Long-lived subscribers make partial writes and backpressure normal rather than exceptional.

## Accepted decision

ChronosDB implements a native, versioned, framed binary protocol with portable codecs independent of Linux APIs. Linux networking uses an event-driven reactor abstraction. The first production backend uses nonblocking sockets with `epoll`.

An `io_uring` backend may be added behind the same abstraction only after the epoll implementation is correct. It is accepted only on reproducible comparison with identical protocol, durability, batching, connection, query, and subscription workloads. Linux types from either backend do not leak into portable protocol or core storage interfaces.

Each connection has bounded inbound and outbound buffering, bounded frame sizes, and an explicit state machine:

- partial reads accumulate only up to the validated frame requirement and configured limit;
- framing validates version, type, flags, lengths, and integrity/limits before dependent allocation or decoding;
- partial writes retain immutable output ownership and resume from an explicit offset;
- cancellation is associated with a request/stream identity and is idempotent where specified;
- connection shutdown stops new work, resolves or detaches accepted work according to protocol state, and releases buffers only after no backend operation references them;
- backpressure pauses admission, returns overload, spills only under a future bounded policy, or disconnects according to an explicit rule; and
- subscription overflow cannot indefinitely block ingestion and produces a defined resumable or terminal outcome.

Resume tokens identify deterministic committed boundaries and are opaque, versioned, and integrity-protected. Reconnection validates token scope and retention before continuing; the token does not claim that an external sink applied prior results exactly once.

Thread-per-connection is rejected. TLS, when introduced, uses a proven maintained external library; ChronosDB does not implement cryptographic primitives.

## Detailed rationale

Epoll provides a mature, understandable baseline for correctness, fault injection, and measurement. A shared reactor contract prevents `io_uring` from becoming a second protocol/storage design and makes comparative evidence meaningful. Explicit connection state handles short I/O and cancellation without assuming one syscall equals one message.

Bounded buffers and overflow policies turn slow clients and malicious frames into observable admission decisions instead of unbounded memory growth. Keeping codecs portable enables host-independent fuzzing and macOS development of protocol logic.

## Alternatives considered

- **Thread per connection:** offers simple blocking code but consumes stack/scheduler resources per connection and permits slow clients to occupy threads indefinitely.
- **`io_uring` first:** may eventually reduce syscall or submission overhead, but it adds lifecycle and cancellation complexity before a correct baseline exists.
- **Text-only protocol:** is inspectable but adds parsing/encoding overhead and does not remove the need for versioning, bounds, or binary column batches; SQL text may be a payload inside the framed protocol.
- **Backend-specific codecs:** reduce abstraction work but couple wire compatibility and fuzzing to Linux I/O types.
- **Custom TLS/cryptography:** creates unacceptable security and maintenance risk outside the database's core value.

## Consequences

- The protocol and connection state machine are public versioned contracts from their first release.
- Every request path must tolerate short reads/writes, cancellation races, and disconnects.
- Bounded buffering can reject clients under pressure; errors and metrics must explain why.
- Epoll remains the reference backend even if `io_uring` is later retained.
- Subscription retention and resume behavior must coordinate with storage without allowing permanent pins.

## Affected invariants

This decision directly supports invariants [1, 9, 12, 14, 15, and 17](../architecture/invariants.md): mode-aware acknowledgment, retry identity, deterministic resume, protocol versioning, bounded subscribers, and snapshot-to-stream continuity. Buffer lifetime and cancellation also participate in invariant 11.

## Validation plan

- Fuzz frame codecs and state transitions with fragmentation, coalescing, hostile lengths, unknown versions/flags, and corrupted payloads.
- Test every request under partial read/write, disconnect, cancellation, half-close, timeout, and shutdown interleavings.
- Stall subscribers and saturate inbound/outbound limits while measuring bounded memory and continued ingestion.
- Round-trip, tamper, expire, and resume tokens across restart and schema/retention changes.
- Run identical epoll/`io_uring` workload manifests and compare full latency distributions, CPU, memory, errors, and fairness if the second backend is built.

## Deferred decisions

Frame byte layout, handshake and feature negotiation, maximum sizes, compression envelope, authentication, TLS library/configuration, reactor interface ABI, event-loop/shard counts, timeout policy, cancellation wire messages, overflow actions, and resume-token encoding remain deferred.

## Migration or reversal implications

Protocol evolution requires explicit version/feature negotiation and golden compatibility fixtures. A new reactor backend is non-breaking only when it preserves connection and protocol semantics. Replacing epoll as the reference or adopting thread-per-connection requires a superseding ADR with correctness and workload evidence.

## References

- [Architecture networking and live plane](../architecture/overview.md)
- [Roadmap phases 10–12](../roadmap.md)
- [Glossary: resume token](../glossary.md)
