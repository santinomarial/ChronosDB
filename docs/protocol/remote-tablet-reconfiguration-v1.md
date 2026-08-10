# Remote Tablet Reconfiguration Request v1

## Scope and security precondition

This internal control-plane envelope routes one canonical Tablet Reconfiguration Action v1 from an
authenticated cluster node to the node believed to lead its destination Raft group. It is not a
native client message and has no plaintext remote mode. A remote carrier must authenticate and
integrity-protect the connection, then pass the network authentication result to the receiver. The
currently implemented network stack still lacks that TLS carrier and therefore cannot yet serve
this envelope remotely.

The embedding supplies a principal-to-node authorizer. An authorized nonzero connection principal
must map to the envelope's exact source node before the receiver may write its action ledger.

## Framing

All integers are unsigned little-endian. Complete request bytes are bounded by the configured
limit, never above 131,156 bytes (80-byte header, at most 128 KiB action, 4-byte trailer).

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic `CHRRTRQ\0` |
| 8 | 2 | major `1` |
| 10 | 2 | minor `0` |
| 12 | 4 | header size `80` |
| 16 | 8 | total size |
| 24 | 8 | nonzero source node ID |
| 32 | 8 | nonzero target node ID, distinct from source |
| 40 | 8 | nonzero required leader term |
| 48 | 8 | action size |
| 56 | 4 | action CRC32C |
| 60 | 4 | header CRC32C with this field zero |
| 64 | 16 | zero reserved |

Exactly one complete [Tablet Reconfiguration Action v1](../formats/tablet-reconfiguration-action-v1.md)
follows the header. The final 4 bytes are CRC32C over the complete header and action. The nested
action limit applies independently of the envelope limit.

## Receiver contract

The receiver rejects anonymous, unauthorized, or source-mismatched principals before durable
preparation. It then exact-matches the target node and tablet. Membership begin/finalize actions
must name that tablet's configured Raft group; placement publication must name the configured
metadata group.

An accepted action is canonicalized by the nested decoder and durably prepared in the tablet-bound
immutable action ledger. The receiver submits it nonblockingly to the bounded asynchronous
Multi-Raft owner with the envelope's exact required leader term. The exclusive owner checks role
and term immediately before dispatch. A stale term or follower returns `UNAVAILABLE` with no Raft
mutation. Queue admission failure can leave prepared ledger evidence and is exactly retryable.

A successful async completion proves only local persist-and-sync for any returned transition. It
does not prove quorum commit, application, or continuing leadership. Exact retained-action replay
suppresses duplicate appends in the same term and uses the documented current-term progress no-op
for an uncommitted prior-term match.

## Compatibility and rejection

Major 1, minor 0 is exact. Unknown versions are unsupported only after header integrity succeeds.
Reserved bytes must be zero. Invalid sizes, identities, checksums, nested action bytes, trailing
data, or noncanonical action semantics fail closed before ledger preparation. No native object
layout is serialized.
