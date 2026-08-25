# ADR 0469: Split-leader Native read-authority coordination

- **Status:** accepted
- **Date:** 2026-08-24
- **Owners:** ChronosDB service, Native protocol, query, Raft, networking, and security maintainers
- **Extends:** [ADR 0465](0465-all-or-nothing-raft-read-authority-fanout.md),
  [ADR 0466](0466-per-group-replicated-read-authority-service.md), and
  [ADR 0468](0468-packaged-shared-query-control-service.md)

## Context

The distributed Native path still called `await_authority()` for every resident group on the local
daemon. It therefore failed unless one node led metadata and every tablet group, even though the
authenticated remote authority endpoint and all-or-nothing batch client were implemented. A
committed placement `leader_hint` is advisory and can be stale after an election, so it cannot be
the sole target authority.

## Decision

`ReplicatedIngestDatabase::observe_query_groups()` obtains the canonical configured query-group
vector directly from the local durable runtime. It validates each exact group and local-node
identity, stable committed membership, current leader shape, and current committed placement before
returning the observations. A follower observation supplies the current Raft leader identifier;
the committed catalog supplies that node's private endpoint; the immutable daemon peer bundle
supplies its TLS context and certificate/node authorization.

For one Native distributed attempt, every locally led group uses
`ReplicatedReadBarrier::await_group_authority()`. Every follower group becomes one immutable remote
acquisition request, and all remote requests start in the existing all-or-nothing batch before the
local waits. The coordinator merges local and remote proofs only after every group succeeds, sorts
the result canonically, rejects duplicates or count drift, and then acquires the exact
barrier-covered metadata/tablet publication. No partial authority vector reaches fragment binding.

Authority acquisition, fragment preparation, mutable execution, and whole-attempt rebinding share
one absolute Native execution deadline. Cancellation tears down a running remote batch. Retryable
failure re-observes every group and reconstructs all local/remote targets rather than retaining a
stale leader. The exact logical query/tablet identity check remains required before installing the
replacement attempt.

When this distributed owner is configured, protocol-v2 SELECT no longer emits the older
single-common-leader redirect before execution: a split leader topology is now executable directly.
The non-distributed replicated service retains its authoritative redirect behavior.

## Consequences

One Native coordinator can read across independently led metadata and tablet groups using the same
authenticated endpoint as mutable fragments. Current Raft observations, rather than metadata
hints, choose remote authority targets. The returned vector remains a stable per-group proof set,
not a globally atomic cross-group instant.

The preliminary unbarriered snapshot is used only to resolve a bounded authenticated control route.
The later barrier-covered snapshot and fragment binder revalidate current catalog, membership,
placement, schema, publication, and serving-leader authority before any row becomes visible.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): every local or remote proof is a current-term quorum
  read barrier and every visible publication covers its exact read index.
- [Invariant 6](../architecture/invariants.md): partial mixed authority and partial fragment output
  are withheld; retry replaces the whole attempt.
- [Invariant 11](../architecture/invariants.md) and
  [Invariant 15](../architecture/invariants.md): group, route, attempt, descriptor, deadline, buffer,
  retry, and cancellation ownership is finite and explicit.
- [Invariant 18](../architecture/invariants.md): remote targets come from committed endpoints plus
  authenticated immutable peer TLS policy; source and target node authorization remains exact.

## Validation

A focused real-loopback service gate provisions an applied two-group database with the metadata
group led locally and the tablet group observed as led remotely. The ordinary common-leader redirect
path fails closed on that split, while the configured distributed Native service acquires exactly
one local authority and one authenticated remote authority, executes the remote mutable fragment
through the same shared endpoint, and returns a complete SQL result. Server metrics prove one
authority and one mutable request, and the fake remote worker is invoked exactly once. The complete
normal service suite passed 107 of 107 tests and the service allocation-failure suite passed 4 of 4
tests. The focused split-leader gate passed under ASan/UBSan with leak detection disabled because
Apple's sanitizer runtime does not support LeakSanitizer. Warning-as-error builds, clang-format 18,
and whitespace review passed. The Linux-only three-daemon process gate cannot be rebuilt on this
macOS host. Repository-pinned clang-tidy 18 remains unavailable against the installed macOS 26
libc++ headers because the older analyzer lacks the SDK's generic bit-count builtins.

## Migration or rollback considerations

No wire or durable migration. Rolling back requires restoring common-leader-only query behavior or
disabling distributed Native SELECT before removing the authority client dependency. Existing
authority and mutable frames remain byte-compatible.

## Unresolved questions

- Linux three-daemon qualification with deliberately independent group leaders and leader loss.
- Whether a future authority response should carry an authenticated redirect hint to reduce one
  whole-attempt re-observation after a target loses leadership.

## References

- [Raft Read Authority Transport v1](../formats/raft-read-authority-transport-v1.md)
- [Packaged Native Daemon](../learning/packaged-native-daemon.md)
- [Implementation Roadmap](../roadmap.md)
