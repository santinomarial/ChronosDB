# ADR 0289: Owning authenticated Raft transport runtime

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, Raft, cluster transport, and operations maintainers

## Context and decision

ChronosDB already has bounded inbound/outbound mutual-TLS Raft carriers, reconnect ownership, an
asynchronous durable runtime, generation-tagged timers, and one unified poll owner. Direct daemon
construction would nevertheless be unsafe if borrowed authority, receiver, TLS-context, or election
source addresses changed while carriers retained pointers, or if initial timer state came from
external configuration rather than recovered consensus state.

`ReplicatedRaftTransportRuntime` is an address-stable outer owner. It first creates the immutable
peer authority, then constructs its receiver and one immutable client TLS context per remote peer,
followed by inbound listener, outbound reconnect manager, randomized timer driver, and unified poll
runtime. Member declaration order destroys the poll runtime before every borrowed dependency. The
durable application runtime remains borrowed and must outlive this owner.

The local configured peer endpoint is the exact inbound bind authority. Every remote peer gets its
own expected TLS server identity while local certificate, key, and trust paths are shared. Receiver
validation, inbound framing, outbound framing, and peer-pool routing use one exact codec-limit set.
The configured leaf certificate must be suitable for both TLS server and client use. Before
returning,
the owner synchronously requests an ordered observation for every configured resident group from
the asynchronous durable owner, validates group and local-node identity, and arms the timer from
that recovered observation. Deployment data cannot assert role, term, leader, or log position.

Election deadlines use fresh operating-system entropy for an inclusive bounded interval. Timeout
bounds are positive, ordered, strictly greater than the heartbeat interval, and capped at sixty
seconds. Completed transport results remain
explicitly caller-owned because snapshot-install and read-barrier work cannot be silently discarded.

ADR 0421 subsequently added a mutually exclusive shared in-memory PEM source. Packaged startup
reads all three credential files from qualified descriptors and shares those exact bytes across the
inbound server and every remote-identity client context; the compatibility path source remains
available to direct embedders.

## Consequences and validation

The service layer now has a complete transport lifecycle boundary that can be started before client
admission and destroyed before database shutdown. Single-node deployments continue using the
existing local election path; this owner requires at least one remote peer because the underlying
reconnect manager intentionally represents a nonempty route set.

The focused gate constructs the owner over a real durable Multi-Raft runtime and TLS contexts,
binds the exact configured listener, installs an initial durable observation, polls once, and shuts
down in dependency order where loopback binding is permitted; the restricted macOS workspace skips
that socket case explicitly. Invalid single-node transport composition and duplicate groups fail
before socket ownership everywhere. Real peer handshake/vote coverage remains in the lower unified
transport gate; daemon CLI/loading, poll-thread composition, dual-purpose deployment certificates,
multi-process election/failover, and snapshot/read result handling remain subsequent work.

## References

- [ADR 0265](0265-unified-raft-transport-runtime.md)
- [ADR 0287](0287-strict-authenticated-raft-peer-config.md)
- [ADR 0288](0288-exact-raft-certificate-node-authority.md)
- [ADR 0421](0421-descriptor-bound-in-memory-tls-credentials.md)
