# ADR 0418: Owning native-client TLS routes

- **Status:** accepted
- **Date:** 2026-08-23
- **Owners:** ChronosDB service, native-client, networking, security, and operations maintainers
- **Extends:** [ADR 0417](0417-strict-native-client-route-configuration.md)

## Context

The strict native route parser and certificate-to-node authority still left embeddings to read a
security-sensitive file, qualify three TLS credential paths, construct one expected-identity client
context per route, and preserve every context address while the redirect router borrowed pointers.
Performing those steps independently could follow a final symlink, accept a group-readable private
key, race vector relocation, or publish only part of a route map after credential failure.

## Decision

`NativeClientTlsRouteOwner::load` is the startup-only, move-only owner for one complete native route
and TLS bundle. It opens the route file and all three TLS paths with `O_NOFOLLOW` on the final
component and requires bounded, nonempty regular files. The route file, certificate chain, and
trust store may be readable but cannot be writable by group or other. The private key cannot have
any group or other permission. Empty and embedded-NUL paths, invalid limits, directories, final
symlinks, truncation, growth beyond the initial route-file size, and short reads fail closed.

The route file is read completely from its already-open descriptor and parsed under the strict v1
limits. TLS files default to a 16 MiB bound and cannot be configured above 64 MiB; route text
defaults to 1 MiB and cannot be configured above 16 MiB. After all paths qualify, the owner creates
the immutable route authority, reserves exact context and published-route capacity, constructs one
`TlsClientContext` with the route's expected server identity for every node, and only then publishes
the complete node-sorted `NativeLeaderRoute` span. Any parser, authority, OpenSSL, or allocation
failure destroys the partial owner and returns no routes.

The PIMPL owns the authority before the TLS-context vector and published pointer vector. Exact
reservation prevents context relocation while pointers are formed, no vector mutates after
publication, reverse destruction removes published routes before their context targets, and moving
the outer owner does not change any pointee address. Existing TLS contexts own their loaded
credential state and do not borrow configured path strings. ADR 0421 subsequently made each TLS
qualification return the exact bytes read from that already-open descriptor and constructs all
contexts from one shared immutable PEM bundle. OpenSSL therefore never resolves those route-owner
credential paths again.

## Consequences

An embedding can now obtain one complete authenticated native route map from explicit files without
writing filesystem checks, authority construction, or pointer-lifetime glue. Startup cost is one
bounded route read plus one OpenSSL client-context construction per node. Retained memory is the
parsed route/authority vector, one context per node, and one fixed published route per node.

The owner opens no network connection and asserts no group, initial node, leader, term, membership,
or placement epoch. Request-specific authority and operation limits remain explicit. A packaged
`chronosctl` QUORUM_SYNC command and its CLI/file arguments remain separate work. No durable or
network bytes change.

## Affected invariants

- [Invariant 5](../architecture/invariants.md): local files still cannot create write-leader or
  placement authority.
- [Invariant 6](../architecture/invariants.md): each endpoint, expected SAN identity, fingerprint,
  node principal, and TLS context is built and published as one complete route.
- [Invariant 11](../architecture/invariants.md): the PIMPL owns every authority/context pointee and
  publishes stable borrowed pointers with explicit lifetime.
- [Invariant 14](../architecture/invariants.md): startup composition changes no Protocol 2 bytes.
- [Invariant 18](../architecture/invariants.md): filesystem qualification and all-or-nothing
  context construction cannot weaken TLS identity or route validation.

## Validation

A focused integration test securely copies a private key, loads a real route file and client
credentials, moves the owner without changing its context pointer, completes a real mutual-TLS
handshake, matches the fixture server's verified SHA-256 fingerprint, and authorizes its exact node.
Negative tests reject group-readable keys, group-writable route/trust files, final route/key
symlinks, route-byte and TLS-limit violations, invalid certificate contents, and incomplete path
bundles. Header self-containment, installed consumption, formatting, warnings-as-errors,
clang-tidy, ASan/UBSan, and the full serialized suite are required before completion.

## Migration and rollback

Native-client embeddings may replace manual file qualification, context vectors, and leader-route
pointer construction with this owner. Rollback restores caller-side ownership and changes no
server, durable, or wire state.

## References

- [Strict native-client route configuration](0417-strict-native-client-route-configuration.md)
- [Native client route configuration](../operations/native-client-route-config.md)
- [Deadline-bound native QUORUM_SYNC TCP client](0415-deadline-bound-native-quorum-ingest-tcp-client.md)
- [Bounded native QUORUM_SYNC TCP execution](0416-bounded-native-quorum-ingest-tcp-execution.md)
- [Descriptor-bound in-memory TLS credentials](0421-descriptor-bound-in-memory-tls-credentials.md)
