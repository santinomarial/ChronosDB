# Distributed Aggregate Exchange

## Purpose and public interfaces

`encode_exchange_message` converts one validated `ExchangeMessage` into an owned 128-byte worker/
coordinator frame. `decode_exchange_message_exact` validates one borrowed exact frame and returns
value-owned state. `BoundedExchange::push` and `DistributedAggregateCoordinator::accept` enforce
the same state invariants before retaining a message.

## Data, ownership, and invariants

The frame names both query and tablet and includes a nonzero per-tablet sequence. Its aggregate is
the mergeable Welford state: count, sum, extrema, mean, and M2. Presence flags are explicit because
zero is a legitimate extremum. Empty state has exactly one representation: absent extrema and
positive-zero numeric state. This also rejects negative-zero alternatives that would decode to the
same arithmetic value but produce different bytes.

`EncodedExchangeMessage` owns a `std::array<std::byte, 128>`. Its byte view remains valid only while
that owner lives. Exact decoding does not retain the input view. The current in-memory exchange is
mutex-protected MPMC state; the codec itself has no shared state and needs no synchronization.

## Failure behavior and complexity

Length, magic, and CRC are rejected before payload interpretation. Unknown checksum-valid versions
return `NOT_SUPPORTED`; damaged or contradictory bytes return `CORRUPTION`; invalid encoder input
returns `INVALID_ARGUMENT`. Transport authentication is separate from CRC integrity.

Encoding and decoding are `O(1)` because the frame is fixed at 128 bytes, use constant storage, and
perform no successful-path heap allocation. The bounded exchange still charges its in-memory
`ExchangeMessage` representation, not the wire length.

## Tradeoffs and deferred work

A fixed ungrouped-aggregate frame gives partial-I/O carriers an unambiguous payload without
prematurely defining a general physical-fragment language. The cost is a specialized first exchange
type. Grouping state, physical plans, ordering/top-N, cancellation, retry, duplicate sequencing,
and multi-tablet snapshot compatibility require their own bounded contracts.

## Verification and review questions

The golden-layout test reads every field independently and freezes one whole-frame CRC. Corruption
tests rewrite CRC where needed so reserved, version, and canonical-state validation are exercised
beyond the checksum gate. Sanitizer and installed-consumer checks cover runtime safety and public
header/link visibility.

**Why require positive zero for empty state?** Arithmetic equality is too weak for canonical bytes:
positive and negative zero compare equal but have different IEEE-754 representations.

**Why preserve Welford state instead of rows?** Workers reduce data before transfer, while merging
count/mean/M2 retains population variance semantics without replaying individual values.

**Why is sequence not enforced by the codec?** The codec proves that a sequence exists and is
nonzero. Ordering, deduplication, and retry windows depend on connection/coordinator lifecycle and
belong to the carrier protocol.
