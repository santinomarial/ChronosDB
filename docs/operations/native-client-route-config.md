# Native Client Route Configuration

Redirect-capable native clients use one external file containing the complete node-to-native-route
and server-certificate authority map. Example:

```text
CHRONOSDB_NATIVE_CLIENT_ROUTES_V1
1=10.0.0.11:7421,native-1.example.test,0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
2=10.0.0.12:7421,native-2.example.test,1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
3=10.0.0.13:7421,native-3.example.test,2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

Each line contains a positive canonical decimal node ID, a usable canonical IPv4 address and
nonzero port, a lowercase DNS name or canonical IPv4 certificate SAN identity, and the lowercase
SHA-256 fingerprint of that node's native-server leaf certificate. Node IDs must be strictly
increasing. Endpoints and certificate fingerprints must be unique. Lines contain no spaces or
comments. LF is the only line ending; the final LF is optional. The unspecified `0.0.0.0` address
is not a client destination.

This file is a native-client trust map, not a Raft peer file. A node's Raft transport endpoint must
never be inferred as its native endpoint. The file also cannot declare a group, current leader,
term, membership, or placement epoch. A QUORUM_SYNC caller must provide its exact group, initial
node, and minimum placement epoch separately; authenticated Protocol 2 redirects may then change
the destination only within the configured map and finite redirect budget.

During TLS connection, the route's expected identity must be used for certificate SAN verification.
After the handshake, the verified leaf fingerprint and connected IPv4 address select one stable
node principal. The server may claim the configured destination node only when that principal and
node ID match exactly. A certificate presented from another configured address is not authorized.

Client certificate, private-key, and trust-store paths are separate local secret configuration and
never appear in this shared route file. `NativeClientTlsRouteOwner::load` opens every final path
without following a symlink, requires bounded nonempty regular files, rejects group/other-writable
route, certificate, and trust files, and requires the private key to be inaccessible to group and
other. It creates one expected-identity TLS client context per route and publishes stable borrowed
route pointers only after the complete bundle succeeds.

The current OpenSSL path API reopens qualified credential paths during context creation. Protect
parent directories and replace credentials atomically so a path cannot be swapped between those
steps. Existing contexts retain loaded credential state; credential rotation, DNS endpoint
resolution, and live reload are not implemented.

## Packaged QUORUM_SYNC command

`chronosctl quorum-sync` sends one already-encoded canonical Columnar Append v1 payload:

```sh
chronosctl quorum-sync \
  --group 01234567-89ab-cdef-0123-456789abcdef \
  --initial-node 1 \
  --minimum-placement-epoch 4 \
  --routes /etc/chronosdb/native-client-routes \
  --tls-cert /etc/chronosdb/client.pem \
  --tls-key /etc/chronosdb/client-key.pem \
  --tls-ca /etc/chronosdb/cluster-ca.pem \
  --append-file ./columnar-append-v1.bin \
  --timeout-ms 30000 \
  --json
```

The destination must expose the native protocol through mutual TLS. Packaged `chronosd` is a
compatible target when configured with the complete server bundle documented in
[Native Server Principal Configuration](native-server-principal-config.md). Its server allowlist
must contain the fingerprint of the client certificate used here.

Every argument is explicit: the route file does not supply group or placement authority, and the
command does not infer native routes from Raft endpoints. It exact-validates the append before
network activity, follows at most eight authenticated redirects, and returns exit `0` only for a
validated `APPLIED` or `MATCHING_RETRY` quorum receipt. Option errors return `2`; file, TLS,
deadline, transport, protocol, and server failures return `1`. The command does not retry ambiguous
transport failures. Re-running the same canonical append is safe through its embedded client and
batch identity and request digest.
