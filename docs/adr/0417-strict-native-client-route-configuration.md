# ADR 0417: Strict native-client route configuration

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** ChronosDB service, native-client, networking, and security maintainers
- **Extends:** [ADR 0413](0413-bounded-native-leader-redirect-routing.md),
  [ADR 0416](0416-bounded-native-quorum-ingest-tcp-execution.md)

## Context

The redirected QUORUM_SYNC execution required callers to hand-construct an immutable node-to-native
endpoint map, one expected TLS identity per node, and certificate-to-node authentication policy.
Using the Raft peer file would cross transport trust domains, while permissive flags or a file that
also asserted current leaders, group membership, or placement epochs could let local deployment
text masquerade as consensus authority.

## Decision

ChronosDB defines a separate bounded deployment file parsed by
`parse_native_client_route_config`. Its first line is exactly
`CHRONOSDB_NATIVE_CLIENT_ROUTES_V1`. Each following line binds one positive canonical decimal node
ID to a usable canonical IPv4 endpoint, a lowercase DNS name or canonical IPv4 TLS server identity,
and one lowercase hexadecimal SHA-256 leaf-certificate fingerprint. Node lines are strictly
increasing. Endpoints and fingerprints are unique; whitespace, comments, carriage returns, blank
lines, zero ports, the unspecified IPv4 address, leading-zero decimal forms, wildcard DNS
identities, and extra fields fail closed. A final LF is optional.

The pure parser reads no file and is separately byte-, count-, identity-, and allocation-bounded.
The route file contains no group, initial node, leader, term, membership, or placement epoch. Those
request-specific values remain explicit inputs to `NativeLeaderRedirectRouter` and must come from
the caller's applicable placement/operation authority.

`NativeClientRouteAuthority` takes the parsed, node-sorted routes by value and revalidates usable
endpoints, canonical TLS identities, ordering, and endpoint/fingerprint uniqueness. A verified TLS
leaf fingerprint plus the connected IPv4 address maps to exactly one stable node principal. The
native client's claimed destination is authorized only when that principal equals the configured
node ID. Missing transport authentication or certificate identity is an authentication error;
unknown or address-mismatched certificates are unauthorized. The authority retains the expected
TLS server identities but does not itself perform SAN verification; the later TLS-context owner
must create one context with the corresponding expected identity for every selected route.

## Consequences

Native endpoint and certificate authority are now deterministic, bounded, auditable, and distinct
from Raft transport configuration. The route vector is the only retained variable storage;
authentication is linear in the finite node count and claimed-node authorization is logarithmic.
The concrete immutable authority may be called concurrently after construction and performs no
mutation, so it requires no inter-thread memory-ordering protocol.

The file contains no private key, client certificate, trust-store path, or DNS-resolved address.
ADR 0418 subsequently supplies secure route-file loading, TLS credential qualification, and
address-stable context ownership. Request-specific group/initial-node/placement inputs and a
packaged `chronosctl` command remain separate work. No durable or network bytes change.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): deployment routes cannot assert write leadership or
  replace committed placement and ordered Raft observations.
- [Invariant 6](../architecture/invariants.md): endpoint, expected TLS identity, fingerprint, and
  node principal remain one exact route record.
- [Invariant 11](../architecture/invariants.md): parsed strings and routes are value-owned, while
  later TLS-context borrowing remains explicit.
- [Invariant 14](../architecture/invariants.md): this local deployment format changes no Protocol 2
  or Raft bytes.
- [Invariant 18](../architecture/invariants.md): ambiguous, duplicate, noncanonical, and unbounded
  route authority fails closed.

## Validation

Focused parser tests cover canonical DNS/IPv4 identities, exact endpoint and fingerprint decoding,
wrong magic, node ordering, duplicate authority, hostile decimal/IPv4/DNS/fingerprint forms,
carriage returns, blank lines, extra fields, and byte/count/identity bounds. Authority tests prove
exact fingerprint/address authentication, exact principal-to-node authorization, lookup, and
revalidation of ordering, endpoints, identities, and uniqueness. Two concurrent readers exercise
authentication and authorization under ThreadSanitizer. Header self-containment, installed
consumption, formatting, warnings-as-errors, clang-tidy, ASan/UBSan, and the full serialized suite
are required before completion.

## Migration and rollback

Native-client embeddings may replace ad hoc endpoint and certificate maps with the parser and
authority while retaining their existing TLS context and operation ownership. Rollback restores
caller-owned mapping logic and changes no server, durable, or wire state.

## References

- [Native client route configuration](../operations/native-client-route-config.md)
- [Bounded native leader-redirect routing](0413-bounded-native-leader-redirect-routing.md)
- [Bounded native QUORUM_SYNC TCP execution](0416-bounded-native-quorum-ingest-tcp-execution.md)
- [Native Protocol v2](../protocol/native-v2.md)
