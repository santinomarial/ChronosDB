# ADR 0107: Bounded io_uring socket reactor ownership

- **Status:** accepted
- **Date:** 2026-08-09
- **Owners:** ChronosDB networking maintainers

## Context

ADR 0009 keeps epoll as the reference Linux network backend and permits io_uring only behind the
same portable protocol and lifecycle abstraction. The first feature-pass experiment used io_uring
only to wait on the epoll descriptor. That established build and selection boundaries but was not a
socket-operation backend: accept, receive, and send still belonged to epoll and synchronous socket
calls. Phase 12 requires those operations to use liburing without leaking its types or weakening
partial-I/O, cancellation, bounded-buffer, or shutdown behavior.

## Accepted decision

Add a distinct `IoUringReactor` PIMPL selected through `ReactorBackend::kIoUring`. The backend owns
the listening socket, response `eventfd`, io_uring instance, bounded operation table, accepted
connections, protocol state, and connection buffers. It submits `ACCEPT`, `RECV`, `SEND`, and
response-wakeup `READ` operations directly through liburing; it does not create or wait on epoll.

The ring and operation table contain exactly `maximum_connections + 2` slots: at most one operation
for every admitted connection, one accept, and one response wakeup. Configuration above 32,766
connections is rejected by this backend rather than silently under-provisioning its completion
identity space. Every SQE names a stable table slot, and every connection has at most one receive or
send outstanding. A receive targets a connection-owned fixed read buffer. A send borrows only the
current immutable outbound frame until its CQE is consumed. Unordered-map rehash does not invalidate
node references, and nested frame-vector moves preserve their separately allocated byte storage.

Partial receive bytes continue through `ConnectionBuffers`; partial sends advance the existing
explicit frame offset only after a successful CQE. Pending output takes precedence over scheduling
the next receive for that connection, bounding write pressure without blocking other connections.
Protocol cancellation remains an ordinary validated frame routed to the shard queue. Closing a
connection marks it terminal, issues socket shutdown to resolve its outstanding operation, and
defers descriptor, state, and buffer reclamation until that operation completes. Reactor shutdown
destroys the ring before releasing any operation or connection-owned memory. As with epoll, the
single response producer must be joined before shutdown.

Startup probes the required io_uring opcodes. A missing liburing build, denied `io_uring_setup`, or
kernel without the required operations returns `NOT_SUPPORTED`; there is no automatic backend
fallback. Epoll remains available and remains the correctness/reference backend. This decision
makes no performance claim and does not adopt io_uring as the default.

## Consequences

- Protocol bytes, connection-state semantics, bounded SPSC routing, authentication, response
  validation, and public portable types are shared across both backends.
- The io_uring backend has bounded per-connection read storage and completion state. Its configured
  maximum is lower than the general epoll admission ceiling.
- One outstanding operation per connection simplifies buffer lifetime and cancellation reasoning;
  it may sacrifice full-duplex throughput and requires measurement before optimization.
- Backend behavior can be compiled independently on Linux. Runtime tests need a host policy that
  permits `io_uring_setup`; denial is an expected unsupported outcome.

## Alternatives considered

- **Continue polling epoll through io_uring:** preserves code reuse but does not implement the
  required socket-operation backend.
- **Share one mutable connection core between epoll and io_uring:** reduces duplicated orchestration
  but couples readiness and completion lifetimes and creates a larger correctness-sensitive
  refactor. Portable codecs and state machines are already shared at the correct boundary.
- **Multiple simultaneous operations per connection:** can improve full-duplex throughput but makes
  buffer mutation, close, cancellation, and completion ordering materially harder. It requires
  evidence before adoption.
- **Fallback silently to epoll:** obscures deployment intent and invalidates backend comparisons.

## Affected invariants

Invariants 11, 14, 15, and 18 apply: buffers survive every completion reference, network contracts
remain versioned, slow connections remain bounded, and the optional backend cannot weaken protocol
or lifecycle guarantees.

## Validation

The portable build exercises the explicit unsupported boundary. A focused Ubuntu 24.04/GCC build
with liburing 2.5 compiles `chronos_network` with warnings as errors. On Linux 6.12 with Docker
seccomp disabled, focused tests execute direct accept/receive/send/wakeup operations across
byte-fragmented input, shard routing, ordered query result/terminal output, and shutdown. Broader
parity, cancellation races, sanitizers, churn, and performance comparison remain in the Phase 18
ledger.

## Superseded detail

This ADR supersedes only ADR 0070's description of the io_uring path as a readiness pilot. ADR 0009
and the remainder of ADR 0070 remain accepted.
