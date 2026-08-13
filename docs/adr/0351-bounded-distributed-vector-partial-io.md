# ADR 0351: Bounded distributed vector partial I/O

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and networking maintainers
- **Extends:** [ADR 0162](0162-bounded-distributed-exchange-partial-io.md),
  [ADR 0350](0350-canonical-distributed-vector-batch-exchange.md)

## Context

Distributed Vector Exchange v1 defines a variable-length frame up to 16,777,076 bytes. A stream
embedding still needs explicit ownership for fragmented reads, coalesced frames, peer-controlled
lengths, and short writes. Buffering an arbitrary suffix before checking the fixed header would
allow a peer to drive allocation outside the declared frame and nested-batch limits.

## Decision

`DistributedVectorExchangeReader` owns one 80-byte header and, only after validating its checksum,
version, canonical fields, complete-frame limit, and nested-batch byte limit, one exact-sized frame
vector. Each `consume` retains at most the current frame, reports the exact consumed prefix, and
leaves a coalesced successor with the caller. Complete bytes pass through the exact decoder. Wire
failure is sticky; invalid caller limits are rejected before consuming input and do not poison the
reader. The reader is neither copyable nor movable so partial connection state has one stable owner.

`DistributedVectorExchangeWriteCursor` owns one canonical encoded frame and exposes only its
unwritten suffix. Checked advancement rejects overrun without mutation. Moving transfers the sole
write obligation and leaves the source complete and empty; zero-byte progress is valid.

These types do not add sockets, queues, authentication, retry, sequencing, coordination, or
physical-plan requests, and they do not change Vector Exchange v1 bytes.

## Consequences and validation

Reader retention is bounded by one configured frame plus its fixed header, and allocation occurs
only after the integrity-covered lengths pass both outer and nested byte limits. Processing is
linear in accepted bytes. Focused tests enumerate every two-part split of a mixed-type frame,
consume coalesced frames by reported prefixes, preserve sticky corruption and unsupported-version
classification, reject an oversized nested batch before payload buffering, and verify exact short
writes, rollback, and moved-from cursor state. Header self-containment and installed-consumer
compilation cover the public API.

General vector fragment requests, sequencing/coordination, authenticated transport, execution,
multi-process integration, and broad fault/measurement evidence remain incomplete. No Phase 16
exit gate is claimed.

Invariants 6, 10, 11, 14, 15, and 18 apply.

## References

- [Distributed Vector Exchange v1](../formats/distributed-vector-exchange-v1.md)
- [Canonical distributed vector-batch exchange](0350-canonical-distributed-vector-batch-exchange.md)
- [Bounded distributed exchange partial I/O](0162-bounded-distributed-exchange-partial-io.md)
