# ADR 0492: Bounded mutable grouped sufficient-state mutual TLS

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster, query, networking, and security maintainers
- **Extends:** [ADR 0431](0431-mutual-tls-mutable-vector-query-carrier.md),
  [ADR 0479](0479-bounded-grouped-sufficient-state-mutual-tls.md), and
  [ADR 0491](0491-distinct-mutable-grouped-sufficient-state-transport.md)

## Context

The mutable grouped endpoint had authenticated receiver and finite sender policy, but no owner for
one connected encrypted session. The mutable row carrier cannot decode grouped sufficient state,
and the Manifest grouped carrier writes `CHDVREQ2` rather than the required applied-head
`CHDMREQ1` request.

## Decision

Add move-only, single-thread-affine mutable grouped TLS client and server owners over one already
connected nonblocking `TlsSocket`. They share the established grouped TLS limits, state, interest,
response reader, and write cursor, but retain the distinct mutable request cursor and reader.
Every readiness call performs at most one TLS handshake, read, or write operation. Positive
handshake and exchange deadlines are exact and terminal failure is sticky.

Both endpoints complete mutual TLS and authenticate the verified peer-certificate fingerprint
before application bytes. The client additionally authorizes the authenticated principal for the
immutable attempt target before writing any request byte. The server authenticates before reading
`CHDMREQ1`; the receiver then authorizes the claimed source and rebinds mutable grouped authority.

Client construction exact-decodes the attempt, validates grouped authority against the mutable
fragment, and transfers that authority plus query resources into the grouped response reader. It
publishes only one complete failure, one canonical empty terminal, or a contiguous bounded group
stream with exact reverse route/query/tablet/count/ordinal/sequence correlation. Any TLS,
integrity, authority, limit, allocation, or deadline failure clears the private prefix.

The server invokes `receive_bound`, revalidates returned authority against the decoded mutable
fragment, exact-decodes every response under temporary query memory, and constructs every response
write cursor before exposing the first byte. Fixed scratch arrays and independent frame, byte,
key, aggregate, nested-state, and query-memory bounds remain finite.

TLS contexts, descriptors, authenticators, authorizers, receiver, worker, and their dependencies
remain embedding-owned and outlive the carrier. TCP connection/listener ownership, route rotation,
retry scheduling, cancellation, and process lifecycle remain separate.

## Consequences

An applied-head grouped request now crosses a real mutually authenticated encrypted session without
ever being recoded as Fragment-v2. Response semantics remain shared because `CHDVGRP2` contains no
storage snapshot identity. Client work and memory are linear in one complete response stream; no
prefix is observable.

One event-loop thread serializes every mutation, so no synchronization or inter-thread
memory-ordering algorithm is introduced. Allocation injection required the server implementation
constructor to remain potentially throwing; owner creation catches exhaustion instead of allowing
a false `noexcept` boundary to terminate the process.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): TLS changes neither mutable authority nor wire bytes.
- [Invariant 6](../architecture/invariants.md): mutable proof and grouped response authority remain
  exact across both authenticated endpoints.
- [Invariant 10](../architecture/invariants.md) and
  [Invariant 14](../architecture/invariants.md): independently checksummed `CHDMREQ1` and
  `CHDVGRP2` frames retain exact type and correlation.
- [Invariant 15](../architecture/invariants.md): authentication precedes application I/O and all
  time, count, byte, nested payload, and allocation bounds fail closed.
- [Invariant 18](../architecture/invariants.md): borrowed TLS dependencies, query credit, response
  cursors, and unpublished prefix lifetimes are explicit.

## Validation

Five focused real socket-pair tests prove both certificate fingerprints, target authorization,
exact mutable worker binding/execution, complete two-group and empty-terminal publication, sticky
deadline expiry, invalid authority/bounds rejection, server-principal rejection before request
write, and prefix clearing above the client byte limit. Deterministic allocation injection walks
client and server owner construction until success and requires every prior failure to classify as
resource exhaustion.

The complete cluster and cluster-allocation suites pass 255 and 34 tests respectively with
loopback permission. The five session cases and construction-allocation case pass under ASan/UBSan
with leak detection disabled. Warning-as-error builds, changed-file LLVM 18 formatting, and
whitespace validation pass. LLVM 18 static analysis reports no project-local finding but cannot
complete because the installed macOS 26 libc++ requires compiler builtins unavailable to LLVM 18.
The repository-wide format check retains only the pre-existing violation in the unchanged grouped
TLS v2 header self-containment test.

## Migration and rollback

This is additive and changes no durable or wire bytes. Rollback removes the connected-session
owners while retaining the mutable grouped transport policy and in-process worker.

## Unresolved questions

- Deadline-bound outbound TCP connection ownership and bounded inbound admission.
- All-tablet mutable grouped scheduling, cancellation, and Native finalization.
- Partitioned shuffle/skew policy and computed pre-group programs.

## References

- [Distributed Mutable Vector Query Transport v1](../formats/distributed-mutable-vector-query-transport-v1.md)
- [Distributed Vector Grouped Aggregate Query Transport v2](../formats/distributed-vector-grouped-aggregate-query-transport-v2.md)
- [Transport security policy](../operations/transport-security.md)
