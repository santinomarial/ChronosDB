# Bounded External Sort

## Purpose and interface

`SpillSortOperator` is the finite external counterpart to `SortOperator`. It accepts the same
physical key semantics plus an opened descriptor-relative spill directory, an exclusive filename
prefix, and explicit limits for rows, runs, disk bytes, record bytes, configuration, run sorting,
and merge output. It is a storage-independent physical primitive; SQL still supplies complete
hidden identity keys and removes them after ordering.

## Run formation and stable identity

The operator groups whole input chunks into contiguous runs. A chunk that cannot fit the current
run begins the next one; a chunk larger than a run is rejected explicitly. Each run is sorted by the
audited in-memory stable merge and encoded row by row.

During k-way merge, configured keys use exactly the existing physical comparator, including
ascending/descending, explicit NULL placement, bytewise variable values, decimals, UUIDs, and NaN.
If all keys compare equal, the lower run ordinal wins. Because run ordinals are contiguous source
intervals and each run preserves ties, this reconstructs original logical input order without
serializing scan arrival as a fake SQL identity.

## Ephemeral run bytes

A run begins with a fixed versioned header containing magic, version, row/column counts, a shape
checksum, and header CRC32C. Each row is a bounded length-prefixed payload plus CRC32C. The payload
stores a NULL marker per column, one byte for Boolean, canonical bytes for fixed-width values, and a
32-bit length plus bytes for variable values.

The format exists only during one operator lifetime. It is neither a durable format nor a recovery
artifact, so it is never synced and is not installed as a public codec. Even so, readers distrust
file contents: every length, checksum, shape, row count, canonical cell, file boundary, and trailing
byte is checked before output.

## Ownership and resource behavior

The operator owns its input, directory descriptor, exact exclusively created run files, bounded
metadata, and two reusable record buffers. One configuration reservation is acquired before those
containers reserve capacity. Run sorts keep their ordinary state and chunk reservations; output
uses a first disk pass to calculate exact canonical sizes, reserves output credit, and then copies
in a second pass. Returned chunks own only output credit and no file bytes.

Normal end closes and removes every file before returning sticky end. Any child, I/O, corruption,
limit, allocation, or cancellation failure requests query cancellation and destroys state. Early
destruction—such as a LIMIT above the sort—removes exact owned names best effort. It never scans or
deletes unrelated directory entries.

## Complexity and tradeoffs

For `R` rows, run width `M`, `N = ceil(R/M)` runs, and `K` keys, run sorting costs
`O(R log M * K)`. The baseline head scan costs `O(R * N * K)` comparisons and deliberately rereads
records rather than retaining one maximum-sized row per run. Memory is bounded by input run credit,
one in-memory run sort, two maximum-record scratch buffers, bounded metadata, one output batch, and
its exact reservation. Disk is bounded by the configured quota.

The repeated I/O is a transparent baseline, not a final optimizer claim. A tournament tree or
per-run cached head can reduce reads only after its aggregate memory is admitted and differential
tests prove identical ties, corruption handling, cancellation, and cleanup.

## Failure review questions

**Why not reuse Columnar Batch v1?** It requires durable table/schema/column identities that query
intermediates intentionally do not have. Fabricating them would confuse ephemeral execution shape
with catalog authority.

**Why are run files checksummed if they are temporary?** Storage and memory faults must become
deterministic errors rather than malformed canonical vectors or undefined behavior. Temporary does
not mean trusted.

**Why not split an oversized chunk?** That needs another exact selected-row materializer and credit
policy. The current API rejects the unsupported shape rather than hiding a weaker implementation.

**What makes tie order exact across runs?** Stable in-run sorting plus lower contiguous run ordinal
is mathematically the same as one global stable sort. SQL ordering still carries authoritative
identity keys, so physical stability is never presented as a logical identity contract.
