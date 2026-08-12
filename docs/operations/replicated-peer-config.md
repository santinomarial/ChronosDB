# Replicated Peer Configuration

Authenticated Raft transport uses one external file containing the complete node-to-route and
certificate authority mapping. Example:

```text
CHRONOSDB_REPLICATED_PEERS_V1
1=10.0.0.11:7441,node-1.example.test,0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
2=10.0.0.12:7441,node-2.example.test,1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
3=10.0.0.13:7441,node-3.example.test,2123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

Each line contains a positive canonical decimal node ID, canonical IPv4 address and nonzero port,
lowercase DNS name or canonical IPv4 certificate SAN identity, and lowercase SHA-256 fingerprint of
that node's leaf certificate. Node IDs must be strictly increasing. Endpoints and certificate
fingerprints must be unique. Lines contain no spaces or comments. LF is the only line ending; the
final LF is optional.

The local node must have an entry. Its endpoint is the inbound Raft listener authority; every other
entry is an outbound route. The TLS identity is checked during the client handshake, while the leaf
fingerprint maps the authenticated connection to exactly one node ID before any claimed Raft source
identity is accepted. Certificate, private-key, and trust-store file paths are separate local secret
configuration and never appear in this shared routing file.

This is deployment configuration, not consensus state. It cannot change Raft membership or
committed tablet placement. The daemon reads it once during startup; live reload and DNS endpoint
resolution are not implemented.
