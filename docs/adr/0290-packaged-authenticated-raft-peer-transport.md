# ADR 0290: Packaged authenticated Raft peer transport

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB daemon, service, Raft, security, and operations maintainers

## Context and decision

The replicated daemon previously elected only local single-voter groups. The owning authenticated
transport runtime now provides the missing lifecycle boundary, but enabling it through partial CLI
state or letting it outlive the durable database would break identity and ownership guarantees.

`chronosd` accepts peer transport only as one complete bundle:
`--replicated-peers`, `--raft-tls-cert`, `--raft-tls-key`, and `--raft-tls-ca`, together with
`--replicated-groups` and `--data-dir`. The peer file uses the strict v1 parser. Every configuration
file and TLS path is opened without following its final symlink and qualified as a bounded regular
file; the private key must be inaccessible to group and other. TLS contexts subsequently load those
qualified paths, so atomic deployment/permission discipline is still required to prevent a path
replacement between qualification and OpenSSL loading.

After committed database recovery, startup requires the local node to be a voter in every resident
group and every configured voter to have an authenticated peer entry. Without peer transport, every
resident group must be exactly the existing local single-voter form. With transport, the daemon
creates the address-stable owner and starts one dedicated poll thread before client work.

The transport thread is the sole unified-runtime and durable-completion-descriptor consumer. It
drains ordinary election, heartbeat, append, and application transitions after outbound routing.
Snapshot-install and read-barrier completions are never discarded: encountering either currently
fails the daemon closed until their storage/query handlers are composed. Shutdown first closes and
drains client work, then joins/destroys transport, then closes the borrowed durable database.

## Consequences and validation

Multi-voter replicated daemon configurations now have real authenticated election/replication
transport rather than an external embedding gap. Startup remains fail-closed for partial TLS flags,
missing voter routes, insecure keys, invalid peer files, transport thread failure, and unsupported
snapshot/read completion work. The native client listener remains separate loopback plaintext.

`chronosd` and the service suite build with the new cluster dependency. CLI checks cover help and
incomplete transport bundles; the Linux replicated process regression continues proving local mode
and now asserts `raft_transport=local`. The owning transport's real listener/TLS/durable-observation
gate passed outside the restricted macOS socket sandbox. A real three-process certificate fixture,
election/failover ingest gate, secure in-memory PEM loading, credential rotation, snapshot install,
and read barriers remain subsequent work.

## References

- [ADR 0287](0287-strict-authenticated-raft-peer-config.md)
- [ADR 0289](0289-owning-authenticated-raft-transport-runtime.md)
- [Native server operations](../operations/native-server.md)
