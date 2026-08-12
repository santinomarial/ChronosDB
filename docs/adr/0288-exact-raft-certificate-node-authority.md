# ADR 0288: Exact Raft certificate-to-node authority

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, cluster transport, and security maintainers

## Context and decision

Mutual TLS verifies possession of a trusted certificate but does not prove that a peer may claim an
arbitrary Raft node ID. The transport carriers intentionally borrow separate network-authenticator
and cluster-node-authorizer interfaces. Daemon composition needs one immutable implementation that
cannot disagree across inbound and outbound paths.

`ReplicatedPeerAuthority` owns the complete parsed peer deployment vector and local node identity.
It accepts only a transport-authenticated request with a verified leaf-certificate SHA-256
fingerprint. The fingerprint must select exactly one configured entry and the socket peer's IPv4
address must equal that entry's configured address. On success, the configured positive Raft node
ID is returned directly as the stable principal ID. Cluster authorization succeeds only when that
principal equals the claimed node ID and the node remains in the immutable configured set.

Creation revalidates nonzero local/node/port fields, strict node ordering, unique endpoints, unique
fingerprints, and exact local-node presence even when callers bypass the text parser. DNS/SAN
syntax remains parser/TLS-context responsibility; it is not reinterpreted by authorization. The
object has no mutation after creation and its concrete authentication and authorization reads are
safe for concurrent carrier callers.

## Consequences and validation

Inbound clients and outbound servers share one certificate/IP/node truth. A trusted but unlisted
certificate, a listed certificate arriving from a different address, or a principal claiming a
different node fails closed before Raft admission or output. Node IDs need no second alias registry.

Focused tests cover exact authentication/authorization, missing TLS proof, unknown fingerprints,
wrong addresses, cross-node claims, missing local identity, unsorted nodes, and duplicate endpoint
or certificate authority. Certificate rotation, dual-certificate overlap, secure config loading,
TLS-context creation, daemon ownership, and real multi-process failover remain subsequent work.

## References

- [ADR 0246](0246-authenticated-raft-transport-receiver.md)
- [ADR 0248](0248-persistent-outbound-raft-mtls-carrier.md)
- [ADR 0287](0287-strict-authenticated-raft-peer-config.md)
