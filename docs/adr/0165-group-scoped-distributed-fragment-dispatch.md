# ADR 0165: Group-scoped distributed fragment dispatch

- **Status:** accepted
- **Date:** 2026-08-11
- **Owners:** ChronosDB query, networking, and distributed-systems maintainers
- **Extends:** [ADR 0164](0164-snapshot-bound-distributed-aggregate-fragment-v1.md)

## Context

Fragment v1 binds a tablet and Raft indexes but deliberately has no Raft group UUID. ChronosDB's
tablet and group identities are distinct, and Raft indexes are meaningful only within one group.
Treating tablet UUID as group UUID would contradict the explicit commit-position model and allow a
valid barrier/index to be applied to the wrong logical log.

Changing accepted Fragment v1 bytes in place was rejected. The missing authority must be added
without silently reinterpreting that format.

## Decision

Distributed Aggregate Fragment Dispatch v1 is a small integrity-protected envelope containing one
nonzero Raft group UUID and one exact Fragment v1 frame. Its fixed 80-byte header carries exact
outer and inner lengths, zero reserved bytes, and a header CRC. A complete-frame CRC covers header,
stored header CRC, and every inner byte.

Workers accept executable requests only through this group-scoped envelope. After decoding, the
receiver exact-matches the group to its configured tablet group before evaluating the inner proof or
touching storage. The inner frame retains independent corruption/version diagnostics and remains
useful for structural tooling, but it is not independently executable.

The outer CRC is not authentication. An authenticated carrier prevents an untrusted peer from
recomputing checksums after substituting a group. Route, placement, current-term, and snapshot
freshness checks remain worker responsibilities.

## Consequences and validation

The nested frame adds 84 bytes and a second linear CRC pass while preserving the already accepted
inner format and giving every position an unambiguous group scope. Maximum dispatch size is 16,688
bytes. Both encoded layers own their bytes; exact decode returns owned group and fragment values.

Golden tests freeze lengths, group placement, and both outer checksums. Negative tests cover
truncation, group/header and inner corruption, checksum-valid unknown versions, and nil encoder
groups. Full query, focused sanitizer, and installed-consumer checks cover integration. Worker
group matching and execution follow separately.

Invariants 4–6, 10, 11, 14, and 18 apply.

## Migration and rollback

No bare Fragment v1 frame may cross the executable worker boundary. Rollback may stop dispatching
remote work but must not infer a group from tablet identity or unwrap and execute an inner frame.

## References

- [Distributed Aggregate Fragment Dispatch v1](../formats/distributed-aggregate-fragment-dispatch-v1.md)
- [Explicit WAL and Raft commit identities](0072-explicit-wal-and-raft-commit-identities.md)
- [Proof-bound distributed read admission](0115-proof-bound-distributed-read-admission.md)
