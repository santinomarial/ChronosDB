# ADR 0325: Distinct grouped FLOAT64 fragment intent

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0164](0164-snapshot-bound-distributed-aggregate-fragment-v1.md),
  [ADR 0324](0324-bounded-grouped-float64-coordinator.md)

## Context

The grouped exchange and coordinator can merge one nullable FLOAT64 key, but Fragment v1 names only
one aggregate input. Changing that frozen request would break exact ungrouped bytes. Duplicating its
database/table/tablet/schema/snapshot/route/read-proof and projection fields in a second format
would create two authority contracts that could drift.

## Decision

Distributed Grouped FLOAT64 Fragment Intent v1 is a distinct checksummed envelope around one exact
Distributed Aggregate Fragment v1 frame. Its fixed 40-byte header binds the outer version and exact
length, one projected group-key input index, the nested frame length, zero reserved bytes, and a
header CRC. A complete CRC covers that header, its stored CRC, and every nested byte.

The group-key index must be within the nested fragment's destination projection. It may equal the
aggregate input because `GROUP BY x` with an aggregate over `x` is valid. The nested fragment
continues to own query/snapshot/route/proof/projection/event-filter validation and caller decode
limits. Outer header integrity is checked before lengths control slicing, and complete integrity is
checked before nested decoding.

This format expresses structural grouped intent only. A later authority binder must prove the
selected destination key and aggregate columns have supported FLOAT64 types under the pinned schema
and retain the key's declared nullability. A later group-scoped dispatch must bind the Raft group
before a worker can execute it. The bare grouped intent is not executable.

## Consequences and validation

Existing Fragment v1 and Dispatch v1 bytes remain unchanged. The maximum grouped intent is 16,648
bytes and owns its nested encoding. Exact decoding returns owned values and makes only the bounded
allocations already required by the nested projection plus its returned owners.

Two focused cases freeze the outer layout and nested magic, round-trip all authority and grouping
fields, and reject truncation, trailing bytes, outer-header damage, independently damaged nested
bytes, checksum-valid future versions/reserved fields/lengths/key bounds, caller projection-limit
excess, and invalid encoder input. The installed-consumer gate covers both public codec symbols.

Schema/type binding, group-scoped executable dispatch, real-CSEG grouped execution, authenticated
transport, multi-key/non-FLOAT64 state, ordering/top-N/LIMIT, and broad failure evidence remain
incomplete. No Phase 16 exit gate is claimed.

Invariants 4–6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Fragment Intent
  v1](../formats/distributed-grouped-float64-fragment-intent-v1.md)
- [Distributed Aggregate Fragment v1](../formats/distributed-aggregate-fragment-v1.md)
- [Bounded grouped FLOAT64 coordinator](0324-bounded-grouped-float64-coordinator.md)
