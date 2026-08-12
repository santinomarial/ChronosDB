# ADR 0287: Strict authenticated Raft peer deployment configuration

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, cluster transport, security, and operations maintainers

## Context and decision

The existing Raft transport runtime requires exact IPv4 routes, TLS server identities, and a stable
certificate-principal-to-node authorization policy. Replicated group membership does not provide
those deployment facts, and placing secret key paths or mutable routing inside consensus metadata
would conflate local deployment with replicated authority.

ChronosDB therefore defines a separate bounded text file parsed by
`parse_replicated_peer_config`. Its first line is exactly `CHRONOSDB_REPLICATED_PEERS_V1`. Each
following line binds one positive canonical decimal node ID to a canonical IPv4 endpoint, a
lowercase DNS name or canonical IPv4 TLS server identity, and one lowercase hexadecimal SHA-256
leaf-certificate fingerprint. Node lines are strictly increasing. Endpoints and fingerprints are
unique; whitespace, comments, CR, blank lines, zero ports, leading-zero decimal forms, wildcard DNS
identities, and extra fields fail closed.

The parser is pure and allocation/count/identity/byte bounded. It does not read files, resolve DNS,
load secrets, construct TLS contexts, or change consensus state. A later daemon composition owns
secure file loading, requires the local node entry, uses its endpoint for the inbound listener, and
builds both inbound and outbound certificate-to-node authorization from the same exact mapping.
Local certificate, private-key, and trust-store paths remain separate secret-bearing arguments.

## Consequences and validation

Transport startup gains auditable route and identity authority without modifying any durable or
network format. Certificate reuse across node IDs is deliberately rejected because it would make a
verified principal ambiguous. Different certificates may share one DNS identity, but endpoints
remain unique.

Focused tests cover canonical DNS and IPv4 identities, endpoint and fingerprint decoding, node
ordering, duplicate authority, hostile decimal/IPv4/DNS/fingerprint forms, CR/blank lines, and
count/identity bounds. Secure file loading, TLS construction, daemon integration, certificate
rotation, live reload, IPv6, DNS routing, and process failover remain subsequent work.

## References

- [Replicated peer configuration](../operations/replicated-peer-config.md)
- [ADR 0246](0246-authenticated-raft-transport-receiver.md)
- [ADR 0265](0265-unified-raft-transport-runtime.md)
- [ADR 0285](0285-strict-replicated-group-deployment-config.md)
