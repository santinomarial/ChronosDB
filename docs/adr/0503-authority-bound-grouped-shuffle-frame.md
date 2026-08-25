# ADR 0503: Authority-bound grouped shuffle frame

- **Status:** accepted
- **Date:** 2026-08-25
- **Owners:** ChronosDB cluster and distributed-query maintainers
- **Extends:** [ADR 0501](0501-canonical-bounded-grouped-partition-splitting.md) and
  [ADR 0502](0502-complete-node-bound-grouped-shuffle-authority.md)

## Context

The source partitioner emits canonical `CHDVGEX1` streams and the whole-query shuffle authority
binds every source/destination edge, but the nested frame deliberately has no destination identity.
A remote destination must reject route drift before retaining an allocation-sized frame, and it
must not trust a source merely because the nested bytes are well formed. Local and remote delivery
also need an explicit separation so a co-located partition cannot masquerade as a network peer.

## Decision

Adopt the distinct `CHDVGSF1` 1.0 carrier specified by
[Distributed Vector Grouped Aggregate Shuffle Frame v1](../formats/distributed-vector-grouped-aggregate-shuffle-frame-v1.md).
Its fixed 128-byte header binds query ID, source tablet/node, destination partition/node, partition
count, hash version, and exact nested length. Header, nested payload, and complete-frame CRC32C
protect allocation-driving and interpreted bytes independently. Unknown versions, nonzero flags or
reserved bytes, and noncanonical lengths fail closed.

Encoding and decoding require the immutable shuffle authority. Nonempty nested keys are rehashed
after exact canonical decode and must route to the claimed partition. Empty terminal frames remain
valid for an exact edge. The carrier rejects source-equals-destination even though the authority
permits such an edge; local delivery must remain in process rather than construct a self-network
route.

The reusable reader validates the complete fixed header, route, and lower deployment limits before
allocating one exact frame. It owns decode resources, borrows authority with an explicit outlives
contract, reports a consumed prefix, and fails sticky. The move-only write cursor owns complete
encoded bytes and supports exact short-write progress. All owners are single-thread-affine; no
synchronization or memory-ordering algorithm is introduced.

This decision freezes only the carrier and partial-I/O boundary. CRC32C is not authentication.
Mutual-TLS certificate/node authorization, address resolution, whole-stream retry and duplicate
arbitration, reducer closure, and packaged SQL selection remain separate owners.

## Detailed rationale

A distinct outer envelope preserves the already accepted, authority-agnostic grouped-state format
and avoids placing network topology in the query layer. Header-first route rejection prevents a
hostile declared length from influencing allocation before the receiver knows the edge belongs to
the query. Recomputing the partition hash closes a semantic gap that checksums and peer identity
cannot close: an authorized source can still send a valid key to the wrong reducer.

## Alternatives considered

- **Add partition fields to `CHDVGEX1`.** Rejected because that would mix reusable group-state
  semantics with one topology and alter accepted bytes in place.
- **Infer destination from the connected socket.** Rejected because a connection does not prove
  query, tablet, partition, hash-version, or nested-message correlation.
- **Trust the source-side partitioner without rehashing.** Rejected because corrupted logic or
  checksum-valid misrouting would silently split equal keys across reducers.
- **Use the remote carrier for local edges.** Rejected because it would fabricate peer identity and
  adds needless authentication and socket failure modes to an in-process transfer.

## Consequences

One exact remote frame can now be validated independently at the destination without changing the
nested exchange format. The 132-byte outer overhead is paid per group or empty terminal, and keys
are hashed once more after decode. Those costs are accepted correctness costs until measurements
justify a stream-level envelope or batching revision. No claim is made that a complete stream has
been authenticated, retried, or reduced.

## Affected invariants

- [Invariant 6](../architecture/invariants.md): each frame is checked against one immutable complete
  query/source/destination/key/state authority.
- [Invariant 10](../architecture/invariants.md): allocation-driving header bytes, nested payload,
  and complete outer frame have explicit integrity coverage.
- [Invariant 11](../architecture/invariants.md): decoded query memory and buffered frame bytes have
  explicit owners and release on every failure.
- [Invariant 14](../architecture/invariants.md): the carrier has explicit 1.0 bytes and rejects
  unknown versions, while nested `CHDVGEX1` remains independently versioned.
- [Invariant 15](../architecture/invariants.md): hard and caller-supplied outer/nested byte and
  cardinality limits bound one slow or hostile input.
- [Invariant 18](../architecture/invariants.md): destination validation reuses canonical hash v1
  and exact key equality remains the future reducer authority.

## Validation plan

Focused tests cover variable-width multi-key round trip, explicit empty partitions, exact nested
query-memory release, wrong-partition and different-authority rejection, truncation, nested damage,
unknown version with recomputed integrity, every fragmented-read split, coalesced suffixes,
short-write progress, moved-from cursor completion, and sticky failure. Allocation injection covers
encode, exact decode, and reader retention with resource-exhausted classification and released
query credit. The warning-as-error build, 270 cluster tests, 41 cluster allocation-failure tests,
and focused ASan/UBSan cases pass. Changed-source clang-tidy 18 reaches only the known installed
macOS 26 libc++ builtin incompatibility after no ChronosDB-source diagnostic. Formatting and diff
checks are recorded with the implementing change.

## Migration or rollback considerations

This is an additive pre-alpha network format with no deployed compatibility population or durable
bytes. Rollback removes the carrier while retaining in-memory partition splitting and shuffle
authority. Once deployed, changing these 1.0 bytes requires version negotiation rather than an
in-place reinterpretation.

## Unresolved questions

- Bind the edge to authenticated TLS principals before any frame bytes are accepted.
- Define byte-identical whole-stream retry, duplicate arbitration, and terminal acknowledgment.
- Implement deterministic partition reducers and qualify skew, loss, cancellation, and exchange
  costs before packaged SQL selects the shuffle.

## References

- [Distributed Vector Grouped Aggregate Shuffle Frame v1](../formats/distributed-vector-grouped-aggregate-shuffle-frame-v1.md)
- [Distributed grouped SQL execution](../learning/distributed-grouped-sql-execution.md)
- [Implementation roadmap](../roadmap.md)
