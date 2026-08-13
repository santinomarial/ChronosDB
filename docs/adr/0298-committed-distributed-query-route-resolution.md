# ADR 0298: Committed distributed query route resolution

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB cluster, metadata, and networking maintainers
- **Extends:** [ADR 0175](0175-nonblocking-ipv4-tcp-descriptor-ownership.md),
  [ADR 0178](0178-pinned-multi-tablet-tcp-query-scheduling.md),
  [ADR 0297](0297-metadata-backed-distributed-query-authority.md)

## Context

The TCP scheduler accepted a manually assembled node-route vector after fragment authority was
bound. An embedding could therefore select an endpoint for the wrong node or mix endpoints from
uncommitted configuration with dispatches derived from committed cluster metadata. TLS contexts
also carry expected server identities and cannot safely be inferred from an endpoint string.

`ClusterNodeMetadata::endpoint` is an existing generic durable string. Historical and test metadata
may contain names that are not usable by the current IPv4-only carrier, so tightening metadata
decoding to an IPv4 grammar would be an incompatible durable semantic change.

## Decision

`resolve_distributed_query_node_routes` accepts one committed metadata snapshot, the immutable
bound dispatches, and a canonical node-to-`TlsClientContext` span. It selects the unique serving
node IDs named by the dispatches, resolves each exact node through the committed cluster-node map,
and returns a node-sorted route vector accepted by `DistributedQueryTcpExecution`.

The resolver requires canonical unique metadata and TLS node ordering, finite bounds, nonzero node
IDs, one nonnull explicit TLS context per selected node, and a bounded endpoint string. The current
carrier accepts only the shared `parse_ipv4_endpoint` grammar: four canonical dotted-decimal octets,
a nonzero canonical decimal port, and a nonzero address. A selected generic endpoint outside that
grammar is unavailable, not corrupt metadata. DNS and multi-address resolution remain unsupported.

[ADR 0305](0305-bounded-dns-multi-address-query-routing.md) subsequently extends this boundary with
fresh bounded DNS acquisition and ordered multi-address retry while preserving the same committed
node and explicit TLS-context authority.

The TLS context is never derived from the endpoint. It retains the caller-configured trust store,
client credentials, and expected server certificate identity, while the authenticated query
carrier continues to authorize the resulting principal for the exact target node.

The strict IPv4 parser is a public network utility and replaces the duplicate parser in replicated
peer configuration. This does not change wire bytes or metadata encoding.

## Consequences

Executable query routes now bind transport addresses to the same committed node identities named by
dispatch authority. Endpoint updates may be acquired independently from read proofs because a route
does not authorize a snapshot or mutate a dispatch target. Whole-query rebinding remains mandatory
when the serving node itself changes.

Resolution performs `O(cluster nodes + TLS contexts + fragments log unique targets + unique targets
log authority)` work and retains one bounded route per distinct serving node. It performs no I/O.
Allocation failures are explicit resource exhaustion.

## Alternatives considered

- **Store TLS context or certificate identity in metadata:** rejected because process-local OpenSSL
  owners and deployment trust policy are not durable catalog values.
- **Accept hostnames synchronously:** rejected because DNS blocking, answer bounds, caching,
  expiration, and address selection need a separate contract.
- **Tighten all node metadata endpoints to IPv4:** rejected because the durable field is already
  generic and existing values would become undecodable.
- **Resolve a route per tablet:** rejected because multiple tablets on one node share the same
  immutable node route and would create redundant configuration.

## Failure modes and operations

Malformed committed catalog ordering is corruption. Missing selected nodes, unsupported selected
endpoint forms, and missing TLS contexts are unavailable. Invalid caller limits, dispatches, or TLS
context ordering are invalid arguments. No socket opens during resolution; scheduler creation still
revalidates every returned route before an attempt begins.

## Validation

Focused tests parse canonical endpoint boundaries and reject zero, overflow, leading-zero, name,
whitespace, and malformed forms. Existing replicated-peer configuration tests prove the shared
parser preserves its strict deployment contract. Route tests resolve two selected nodes with exact
TLS owners and reject a generic DNS endpoint, missing TLS authority, and noncanonical catalog order.
An unselected generic endpoint is ignored, and a selected-node route limit is enforced. The full
project build covers all public-header consumers.

Invariants 5, 6, 10, 14, and 18 apply.

## Migration and rollback

Embeddings may replace manual route construction with the resolver and pass its owned vector into
the existing scheduler configuration. Deployments using hostnames must continue resolving them
through an explicitly bounded external mechanism until a DNS contract is accepted. Rolling back is
wire- and durable-format compatible but restores manual node/endpoint correlation.

## References

- [Pinned multi-tablet TCP query scheduling](0178-pinned-multi-tablet-tcp-query-scheduling.md)
- [Explicit whole-query authority rebinding](0180-explicit-whole-query-authority-rebinding.md)
- [Metadata-backed distributed query authority](0297-metadata-backed-distributed-query-authority.md)
- [Distributed aggregate exchange](../learning/distributed-aggregate-exchange.md)
- [Phase 16 roadmap](../roadmap.md#phase-16--distributed-query-execution-and-rebalancing)
