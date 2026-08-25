# ADR 0461: Authenticated remote Raft read authority

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB Raft, cluster transport, query, and security maintainers
- **Extends:** [ADR 0299](0299-correlated-replicated-read-authority.md),
  [ADR 0306](0306-authenticated-raft-observation-transport.md), and
  [ADR 0447](0447-fresh-all-group-native-query-rebinding.md)

## Context

Native mutable-query admission can acquire a correlated barrier and observation only from its local
multi-Raft runtime. This deliberately fails closed when required groups have different leaders.
Raft Observation Transport v1 cannot close that gap: it is explicitly non-mutating and an
observation alone cannot establish a linearizable read index. Adding query control messages to the
frozen consensus transport would mix lifecycle, retry, and compatibility domains.

## Decision

Introduce Raft Read Authority Transport v1 as a separate authenticated cluster protocol. One exact
request source asks one exact target to acquire authority for one exact group. A success returns the
nonzero barrier term, context, and read index plus a complete nested Raft Observation Response v1
success frame. The nested frame must repeat route, group, and correlation and prove that the target
is the current stable-membership leader in the exact barrier term with commit at or beyond the read
index.

The receiver authenticates before decode, authorizes the principal-to-source claim, validates the
target, and only then invokes an embedding-owned service. Service failures and exceptions become
correlated failure responses. A successful service value with the wrong group, node, role, term,
index ordering, or membership is rejected before encoding.

The canonical wire format is versioned, fixed-width, bounds checked, and protected by header,
payload, nested-frame, and whole-frame CRC32C. Status numbering reuses the frozen observation status
mapping, while the distinct magics and header shape prevent protocol confusion. No consensus or
durable format changes.

This decision implements the value codec and authenticated receiver boundary only. A bounded
partial-I/O owner, maintained mutual-TLS carrier, daemon service implementation, finite sender, and
all-group query-attempt coordinator are required before split-leader process queries are claimed.

## Consequences

The query plane now has a precise security and compatibility boundary for remote barrier issuance
without weakening the existing observation protocol or adding application messages to Raft. The
nested observation adds bytes and duplicates correlation fields, but lets one mature canonical
observation codec remain the sole definition of observation payload semantics.

Authority is per group, not a globally atomic cross-group instant. A later coordinator must collect
every authority inside one bounded query attempt, revalidate the complete vector against committed
metadata and publications, and discard the whole attempt on any timeout, leadership change,
membership transition, or correlation failure.

All objects in this slice are caller-thread-affine. The receiver borrows its authorizer and service;
both must outlive it and provide their own synchronization. No shared-memory concurrency algorithm
or memory-ordering edge is introduced.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): success carries both a quorum-confirmed barrier and
  the exact leader observation; commit still must become applied/published before visibility.
- [Invariant 6](../architecture/invariants.md): route, group, correlation, leader, term, membership,
  and read index all fail closed on mismatch.
- [Invariant 14](../architecture/invariants.md): the distinct v1 frame is fixed-width, canonical,
  checksummed, bounded, and exact-versioned.
- [Invariant 18](../architecture/invariants.md): authority acquisition does not create a global
  cross-group snapshot guarantee or relax query consistency semantics.

## Validation

Codec tests round-trip canonical success and failure frames and reject damage, unsupported versions,
nested-correlation changes, term mismatch, reconfiguring membership, and missing success authority.
Receiver tests prove authentication precedes parsing/service access, principal/source and target
checks precede acquisition, successful route/correlation preservation, correlated service failure,
uncorrelated-success rejection, and exception containment. Broader suite and sanitizer evidence is
recorded with the implementing commit. Before commit, all 209 normal cluster tests and all 28
cluster allocation-failure tests passed with loopback socket permission. All three focused codec and
receiver tests passed under ASan/UBSan with leak detection disabled because Apple's sanitizer runtime
does not support LeakSanitizer. The new production source passed repository-pinned clang-tidy 18;
all changed C++ files passed clang-format 18; and the diff passed whitespace review.

## Migration or rollback considerations

No prior reader accepts the distinct magic, so there is no mixed-version negotiation. A carrier
must configure this protocol only when both peers support exact v1. Rollback removes an unused query
control boundary and changes no durable bytes; once a daemon consumes it, roll back the complete
query-control stack rather than routing v1 frames to another protocol.

## Unresolved questions

- Should the daemon multiplex this protocol on the authenticated Raft peer listener or dedicate a
  query-control endpoint after measurement of head-of-line and failure-domain effects?
- What bounded attempt and retry policy best prevents mixed-term authority vectors under churn?
- How should authority fan-out expose per-stage observability without leaking peer identities to
  untrusted Native clients?

## References

- [Raft Read Authority Transport v1](../formats/raft-read-authority-transport-v1.md)
- [Linearizable Raft read barriers](../learning/linearizable-raft-read-barrier.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
