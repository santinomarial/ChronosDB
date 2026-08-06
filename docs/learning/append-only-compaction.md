# Append-Only CSEG Compaction

## Purpose and current boundary

The first Phase 7 compaction slice reduces immutable CSEG overlap without changing CSEG v1 or
Manifest v1 bytes. [ADR 0018](../adr/0018-append-only-cseg-compaction-and-manifest-replacement.md)
requires a deterministic complete-tuple merge, fresh output identities, an independent full-row
equivalence proof, atomic Manifest replacement, and retention of old input files while snapshots may
still pin them.

The implemented foundation has two deliberately separate authorities:

- `validate_manifest_v1_compaction_transition()` checks that one Manifest generation removes and
  adds exactly the caller-authorized identities while every unrelated durable fact remains exact.
- `validate_append_only_cseg_v1_equivalence()` proves that two borrowed CSEG image sets contain the
  same complete append-only rows.
- `merge_append_only_cseg_v1()` is the bounded reference output builder. It produces one fresh owned
  CSEG v1 part, but does not install files, publish a generation, or authorize deletion.
- `build_manifest_v1_for_append_only_compaction()` independently repeats full-row equivalence and
  builds the exact next full Manifest generation around one proven output.

## Public interface and ownership

`CompactionPartImage` pairs an independently expected `PartId` with borrowed immutable bytes. Both
input spans must be nonempty and strictly sorted by `PartId`; output identities must be fresh against
all inputs. The caller keeps every byte owner, schema, tablet identity, and WAL identity alive for
the call. The validator retains nothing.

`CompactionEquivalenceLimits` bounds part count, row count, CSEG decode/validation work, and the sum
of the largest uncompressed granule for every simultaneously active cursor. The last bound is
conservative for raw pages but prevents a wide many-part request from turning per-page limits into
unbounded aggregate memory.

## Algorithm and invariants

Every image is exact-decoded, checked against its supplied identity, fully CSEG-content validated,
and bound to the exact schema/tablet. Each side then owns one cursor per part and decodes only its
current granule. Because valid CSEG parts are internally strictly ordered, selecting the smallest
current row across cursors produces the global sorted stream without copying row values.

The ordering tuple is the frozen physical ordering key followed by `WAL_ID`, `RECORD_SEQUENCE`, and
`ROW_ORDINAL`. Equal current tuples from two parts on the same side are corruption: compaction may
not silently deduplicate them. Corresponding input/output rows must have the same ordering tuple and
then compare equal across every user column and all four system columns. Null shape, Boolean value,
fixed and floating-point bits, decimal bytes, variable bytes, WAL identity, sequence, ordinal, and
operation are all compared directly. Counts are only an early rejection; they never establish
equivalence.

The reference merger intentionally follows a different path. It fully materializes validated input
pages within an aggregate bound, creates compact row references, and performs an error-propagating
stable merge by the same frozen physical tuple. Equal adjacent tuples fail as corruption. It then
plans canonical CSEG granules under row/page limits, copies exact physical cells into new canonical
pages, derives WAL/sequence/event extrema, and encodes one caller-named fresh part. Before returning
owned bytes and a Manifest descriptor, it calls the independent streaming oracle on the inputs and
its output. A bug in sorting or materialization therefore cannot authorize itself merely because its
metadata is internally consistent.

## Failure behavior and complexity

Malformed or checksum-invalid CSEG bytes preserve the underlying corruption/unsupported/resource
classification. Bad caller shape, identity/schema/tablet/WAL context, reused output identity, or a
row disagreement is `kInvalidArgument`. Duplicate cross-part tuples are `kCorruption`; configured
or allocation limits are `kResourceExhausted`. No partial success is returned.

For `R` rows, `P` parts, and `K` ordering columns, the intentionally simple oracle selection is
`O(R * P * K)` plus full CSEG validation and complete-cell comparison. Resident decoded page memory
is bounded by the configured aggregate granule limit; cursor and descriptor state is `O(P)`. This is
an oracle, not the eventual optimized merger. The reference merger uses `O(R log R * K)` comparison
work, `O(R)` row-reference/sort storage, bounded decoded input pages, and one complete encoded output.

## Evidence and tradeoffs

Deterministic tests cover interleaved input partitions and reference merging, changed non-key values, stored NaN payload
bits, null versus empty variable values, missing rows, wrong WALs, reused identities, duplicate
tuples, corrupt bytes, and generated repartitionings. The microbenchmark measures four interleaved
inputs against one output at declared row counts and includes full decode, validation, and direct
cell proof; it makes no production throughput claim.

The repeated validation, full materialization, and linear fan-in oracle selection are expensive by design. A future heap-based
merger may be faster, but it must remain differentially identical to this path. Output construction,
durable install ordering, publication, crash testing, and pin-aware reclamation remain later Phase 7
tasks and cannot bypass this oracle. The current builder intentionally returns one output part;
requests that exceed its explicit row/page/file bounds require a later measured partitioning policy.

The existing `ManifestStorage::install_part()` installs that output through the ordinary immutable
part boundary. `install_manifest()` retains add-only authority by default; a caller must explicitly
provide the exact `ManifestCompactionReplacement` to select removal authority. In that mode storage
rereads every named input and output from final files, independently repeats complete equivalence,
revalidates every referenced part, and only then begins the existing Manifest write/readback/fsync/
no-replace-rename/directory-sync sequence. A crash before the Manifest directory sync selects the
old generation; a crash after it may select only the complete replacement generation. Input finals
are deliberately retained.

## Likely review and interview questions

- Why are counts or one digest insufficient? They do not prove multiplicity, exact cells, system
  identity, or floating/variable representations.
- Why reject equal ordering tuples? They identify duplicate physical row identities across inputs;
  silently choosing one would invent a deduplication policy.
- Why require an expected WAL when rows are compared on both sides? Otherwise two equally wrong
  sets could agree with each other while crossing the plan's WAL identity boundary.
- Why keep transition validation separate? Row equivalence cannot prove checkpoint, retry, tablet,
  or unrelated Manifest state, and structural agreement cannot prove row bytes.
