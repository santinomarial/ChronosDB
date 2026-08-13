# ADR 0322: Distinct empty grouped-stream terminal

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query and distributed-systems maintainers
- **Extends:** [ADR 0320](0320-canonical-nullable-float64-grouped-exchange.md)

## Context

A nonempty tablet can mark its final grouped partial terminal, but a tablet with no selected rows
has no group partial to send. Encoding an empty partial under a NULL key would invent a real SQL
NULL group and corrupt the global result. Requiring coordinator timeout to imply completion would
make empty results indistinguishable from lost workers.

## Decision

Grouped Exchange Terminal v1 is a distinct fixed 64-byte frame with magic `CHDXGRT1`. It binds the
query UUID, tablet UUID, and nonzero per-tablet sequence, plus exact version/length, four zero
reserved bytes, and CRC32C. The frame carries no key, aggregate state, or optional payload; its
successful decode is the terminal event.

Separate magic prevents a terminal from being reinterpreted as either a NULL-key grouped partial or
the ungrouped exchange. Exact decoding rejects truncation, trailing bytes, damage, unknown versions,
nonzero reserved bytes, nil identities, and zero sequence. Integrity is checked before identities
control stream state. The encoder owns its fixed array and successful encoding/decoding allocates no
payload storage.

Nonempty streams may continue to close by setting the grouped partial's terminal flag. An empty
stream sends the terminal-only frame as sequence 1. A later coordinator must accept exactly one of
those closure forms under the same contiguous sequence and duplicate rules.

## Consequences and validation

Empty grouped tablets can terminate explicitly without creating a group or weakening failure
detection. Existing 136-byte grouped partial and 128-byte ungrouped bytes are unchanged.

A focused test freezes every terminal field, verifies exact round trip and CRC coverage, and rejects
truncation, trailing bytes, raw damage, unknown versions, checksum-valid reserved bytes, and invalid
input identity. The installed-consumer gate covers both terminal codec symbols.

Grouped stream multiplexing, coordinator state, fragment planning, ordering/top-N/LIMIT, and
broader transport/fault evidence remain incomplete. Bounded terminal partial-I/O ownership is the
accepted follow-up in [ADR 0323](0323-bounded-grouped-terminal-partial-io.md). No Phase 16 exit gate
is claimed.

Invariants 6, 10, 11, 14, and 18 apply.

## References

- [Distributed Grouped FLOAT64 Aggregate Exchange
  v1](../formats/distributed-grouped-float64-exchange-v1.md)
- [Canonical nullable-FLOAT64 grouped exchange](0320-canonical-nullable-float64-grouped-exchange.md)
