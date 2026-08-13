# ADR 0358: Group-keyed distributed vector proof binding

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, metadata, and replicated-service maintainers
- **Extends:** [ADR 0300](0300-group-keyed-distributed-query-proof-binding.md),
  [ADR 0357](0357-metadata-backed-distributed-vector-snapshot.md)

## Context

The metadata-backed vector binder still required runtime proofs already reordered into fragment
order. Replicated leader barriers are produced in canonical Raft-group order and may include the
metadata group, so a caller-side reorder could pair valid authority with the wrong tablet.

## Decision

`bind_group_backed_distributed_vector_snapshot` accepts the same canonical unique group-sorted
barrier/observation authority used by aggregate queries. A shared internal resolver validates the
catalog and authority vector, resolves each planned tablet's committed immutable group binding,
binary-selects its exact proof, and produces a bounded plan-ordered proof vector. Unrelated groups
are ignored; a missing selected group fails closed.

The ordered proofs enter the metadata-backed vector binder, which retains every existing catalog,
membership, admission, Manifest, schema, projection, type, plan, and compatible-generation gate.
The join borrows inputs only for the call, performs no I/O, and changes no wire or durable format.

## Consequences and validation

The two-tablet focused test supplies an extra metadata-group authority, binds both vector dispatches
in tablet-plan order, and rejects an authority span missing one selected tablet group. Reversed
authority rejection and all metadata/proof-policy regressions remain covered by the shared aggregate
tests. Header self-containment and installed consumption cover the public vector entry point.

Correlated follower group authority, remote proof acquisition, vector worker execution, global
coordination, authenticated transport, and process integration remain incomplete. No Phase 16 exit
gate is claimed.

Invariants 4–6, 11, 14, 15, and 18 apply.

## References

- [Group-keyed distributed query proof binding](0300-group-keyed-distributed-query-proof-binding.md)
- [Metadata-backed distributed vector snapshot](0357-metadata-backed-distributed-vector-snapshot.md)
- [Authoritative tablet-to-Raft-group binding](0279-authoritative-tablet-group-binding.md)
