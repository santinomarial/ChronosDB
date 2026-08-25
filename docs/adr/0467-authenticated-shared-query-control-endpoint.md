# ADR 0467: Authenticated shared query-control endpoint

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB cluster transport, networking, query, Raft, and security maintainers
- **Extends:** [ADR 0432](0432-bounded-mutable-vector-query-tcp-ownership.md) and
  [ADR 0463](0463-deadline-bound-raft-read-authority-tcp.md)

## Context

Committed node metadata has one private data endpoint. Adding an inferred adjacent port would put
route authority outside committed metadata, while separate listeners cannot bind the same address.
A shared endpoint must distinguish mutable-fragment and read-authority frames without parsing an
untrusted application prefix before certificate authentication.

## Decision

`DistributedMutableQueryControlTlsServer` completes mutual TLS, maps the verified client certificate
to a stable nonzero principal, and only then reads exactly eight plaintext bytes. The bytes must be
the frozen `CHDMREQ1` mutable-query magic or `CHRRAUQ1` read-authority magic. Unknown values fail
with `NOT_SUPPORTED`; no receiver is invoked.

The selected canonical streaming reader receives that authenticated magic prefix and consumes the
rest of exactly one bounded request. Mutable requests invoke the existing authenticated receiver and
retain its complete schema-validated response stream. Authority requests invoke the existing
authenticated receiver and retain its one canonical response. Coalesced suffixes, invalid frames,
response correlation errors, count/byte exhaustion, TLS closure, and exact handshake/exchange
deadlines fail the connection without partial output.

`DistributedMutableQueryControlTcpServer` owns the nonblocking listener, long-lived TLS context,
bounded stable connection records, fixed poll storage, finite accepts per poll, saturated metrics,
and TLS-before-descriptor teardown. Metrics distinguish completed mutable requests, completed
authority requests, failures, rejections, accept errors, and active ownership.

Both frozen request magics are now public compile-time format constants used by their codecs and the
dispatcher. This does not change either byte format. The original protocol-specific TCP servers
remain valid dedicated endpoints; packaged daemon integration will choose the shared owner.

## Consequences

Both cluster query-control operations can use the one committed node endpoint and existing peer TLS
identity without port derivation or a new metadata format. Authentication happens once before
application classification, while each existing receiver repeats source-node authorization and
target correlation.

One listener thread serializes all calls. A synchronous authority service may wait for quorum on
this thread because the distinct Raft transport thread continues polling. A slow request consumes
one bounded admitted connection and can delay other synchronous requests on this listener; broader
parallel listener sharding requires measurement rather than an unbounded worker pool.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): routing does not alter either mutable publication
  proof or quorum-confirmed authority proof.
- [Invariant 6](../architecture/invariants.md): one authenticated connection selects exactly one
  protocol and publishes only its complete correlated response.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 14](../architecture/invariants.md): existing checksummed/versioned formats remain
  distinct and their frozen magic values are the only dispatch keys.
- [Invariant 11](../architecture/invariants.md) and
  [Invariant 15](../architecture/invariants.md): buffers, frames, bytes, deadlines, admission,
  descriptors, poll storage, and destruction order are bounded and explicit.
- [Invariant 18](../architecture/invariants.md): sharing a listener changes ownership only and does
  not weaken TLS, principal, route, consistency, or result semantics.

## Validation

A real loopback gate uses the unchanged mutable TCP client and unchanged read-authority TCP client
against one shared mutual-TLS endpoint. It proves one exact worker call, one exact authority-service
call, complete results, two authenticated connections, protocol-specific completion metrics, and no
failures. A denied client principal is rejected after certificate verification but before protocol
selection or either receiver call. Before commit, all 222 normal cluster tests and all 28 cluster
allocation-failure tests passed with loopback socket permission. Both focused shared-endpoint tests
passed under ASan/UBSan with leak detection disabled because Apple's sanitizer runtime does not
support LeakSanitizer. All four changed production sources passed repository-pinned clang-tidy 18;
all changed C++ files passed clang-format 18; and the diff passed whitespace review.

## Migration or rollback considerations

No wire or durable migration. A daemon may replace its mutable-only private listener with this shared
owner while retaining the same committed address and clients. Rollback requires disabling remote
authority acquisition before restoring the mutable-only listener; mutable request bytes remain
compatible throughout.

## Unresolved questions

- Whether measured authority contention justifies multiple bounded listener shards behind one
  externally load-balanced committed endpoint.
- Whether future control protocols should use a versioned authenticated negotiation frame instead
  of adding more frozen eight-byte dispatch values.

## References

- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Raft Read Authority Transport v1](../formats/raft-read-authority-transport-v1.md)
- [Committed daemon mutable query plane](0445-committed-daemon-mutable-query-plane.md)
