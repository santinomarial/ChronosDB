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
never appear in this shared route file. Secure file loading, immutable TLS-context construction,
credential rotation, DNS endpoint resolution, live reload, and a packaged client command are not
yet implemented. Embeddings currently parse the text and compose the returned routes and immutable
authority with their own credential/context owner.
