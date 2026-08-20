# Durable temporal history

## Purpose and boundary

ChronosDB preserves corrections rather than overwriting an earlier business fact. Each logical row
therefore has event time, receive time, and an ordered sequence of system-time versions. The current
subsystem supplies five concrete layers:

1. Temporal Mutation Command v1 stores schema-shaped values and row-aligned temporal metadata.
2. `apply_committed_temporal_command` converts one already durable WAL command into owned history.
3. `execute_temporal_command` performs validated live WAL admission and acknowledged publication.
4. `recover_temporal_wal` rebuilds fresh multi-table scalar providers from verified WAL order.
5. `recover_manifest_temporal_wal` composes selected CSEG history with verified checkpoint overlap
   and one applied WAL suffix.

Mixed command dispatch, application checkpoints, and Raft application are not hidden inside these
interfaces. CSEG v2 now has strict metadata/part codecs, semantic and projected reading, plus a
bounded single-lineage scalar resolver that provides a differential current/as-of winner oracle.
Manifest v2 discovery, generation-pinned part loading, and bounded multi-part scalar resolution are
implemented. Complete retained part sets can reconstruct fresh scalar providers. One global WAL
checkpoint can now recover every selected distinct-table WAL tablet: covered commands are routed and
verified against each tablet's durable boundary, and only later commands are applied. Resolved
scalar winners can now enter the vector pipeline through a bounded, query-accounted owned scan
source. Same-table multi-tablet routing, Raft/mixed-source composition, direct vector winner
resolution/lowering, and compaction integration remain.

## Public interfaces and data structures

`EncodedTemporalCommand` owns the complete WAL application payload. `DecodedTemporalCommandView`
borrows those bytes and exposes an embedded `DecodedColumnarBatchView`, mutation descriptors, and
the command's system commit timestamp. Logical identities are nonempty, bounded, and unique within
one command.

`TemporalSnapshotProvider` owns histories for one exact table schema. Its map key is the logical
identity; each value is a system-position-ordered vector containing the owned scalar row, mutation
kind, event/receive time, WAL identity/sequence, and system commit time. A second ordered map turns
an as-of system timestamp into the latest committed position visible at that time.

`restore_retained_history` is the atomic seed boundary for future CSEG/Manifest recovery. It accepts
rows in increasing source position and row-ordinal order, requires one source and one system time
per commit, validates the entire history in disposable state, and publishes only after success. A
first retained version may be a correction, replacement, or tombstone because compaction can remove
its expired predecessor. The caller must supply the proven table-wide retained-system-time boundary;
inferring it from the first physical row would be unsafe when identities have different histories.
Earlier as-of requests return `NOT_FOUND` rather than inventing an empty table.

`verify_retained_commit` is the read-only proof boundary for a WAL command already covered by a
selected durable tablet position. Its `TemporalCommitCoordinate` binds the authoritative position
and system time as one named input so recovery code cannot exchange the two integer fields. At or
after the explicit retained-system-time boundary, it
requires the complete row set at that source position to match the restored commit timestamp,
logical identities, source coordinates, operation metadata, and scalar storage exactly (including
floating-point bit patterns). Before that boundary, retained predecessor rows still match exactly,
while only physically absent command rows may be accepted as reclaimed. This exception is safe only
when a durable manifest independently proves that the command is covered.

`RecoveredTemporalState` owns one provider per configured table and the locked reopened
`WalWriter`. `release_writer()` transfers that writer exactly once to the later live coordinator.
Provider pointers remain valid for the lifetime of the recovered owner.

`RecoveredManifestTemporalState` is the complete WAL-temporal startup owner. It requires a global
checkpoint no later than every selected tablet's durable boundary, restores all pinned CSEG
histories, opens and preflights the WAL strictly after that coordinate, routes commands by table,
verifies covered commands through each target boundary, applies later suffixes, cleans recognized
temporaries, and returns table-keyed providers plus the locked writer. Manifest storage outlives WAL
ownership during destruction. Because Temporal Mutation Command v1 lacks tablet identity, a
same-table multi-tablet generation fails explicitly. A missing retention proof, a WAL that does not
reach the greatest tablet boundary, or any validation/replay failure destroys all fresh state.

`restore_manifest_v2_temporal_tablet_history` exact-opens and fully projects every supplied
generation-pinned CSEG v2 image after the owning tablet descriptor proves exact part count,
canonical identities, source/durable bounds, and total physical-version coverage. It copies user
cells plus all source/version/operation/identity/time metadata and sorts rows by source position and
row ordinal across parts. It then invokes the atomic provider seed with the caller-proven
retained-system-time boundary. Duplicate or impossible
cross-part mutation history is durable corruption; caller lineage/boundary mistakes remain explicit
input errors. Decoded parts are borrowed only during reconstruction and the returned provider owns
every scalar value.

`ScalarSnapshotScanOperator` bridges any resolved current/as-of scalar snapshot into the physical
pipeline. It plans a finite row slice, reserves conservative query credit before retaining output,
and copies every schema-order logical value into canonical validity/offset/value buffers. Each
identity-selected chunk owns its buffers, so releasing the provider, snapshot, or scan operator
cannot invalidate downstream work. Reservation failure leaves the row cursor unchanged;
cancellation and successful end are explicit and sticky.

In-memory retention advances two monotonic frontiers: the oldest commit position still observable
and the earliest system time still promised. Per-identity history keeps the newest predecessor
needed at the boundary. The global time index likewise retains its greatest entry before the time
frontier; removing that entry would make an exact-boundary query incorrectly appear empty. A
successful compaction reports prior/new frontiers and removed/retained version counts. These caller-
supplied frontiers are not durable file-deletion authority: Manifest/CSEG replacement still needs
the combined policy, reader, subscription, backup, and recovery proof.

## Application and recovery sequence

Committed application first binds the decoded batch to the retained catalog schema. It copies each
physical cell through `ScalarValue::from_column_cell`, attaches the enclosing WAL identity and
record sequence, and calls `TemporalSnapshotProvider::apply_committed` once for the complete batch.
The WAL record sequence is authoritative system order; wall-clock time never replaces it.
`verify_retained_temporal_command` reuses that canonical materialization path but compares the
result with restored history without publishing it.

Provider application validates every identity, row shape, source position, mutation transition,
capacity, and scalar schema before publication. It then stages copies of the complete histories and
time index. Only after every allocation and insertion succeeds does it swap the staged state under
the provider mutex. Readers therefore see the prior state or the complete next commit, including
under allocation failure.

WAL recovery follows the existing two-pass contract. Physical verification completes first. The
replay sink then decodes and schema-binds every record without publishing. Only after the complete
preflight succeeds does ordered replay apply records to fresh providers. A replay-time semantic
failure, such as a correction without an original, rejects recovery as corruption and destroys the
entire fresh owner. Successful reopen retains the process lock and continues at the next sequence.

## Invariants and failure behavior

- Application follows enclosing WAL record order; per-table positions may contain gaps when one
  WAL carries multiple configured tables.
- System commit times are nondecreasing per table, but equal timestamps are allowed and later log
  positions win.
- Originals create identities. Corrections, replacements, and tombstones require existing
  identities. Another original for an existing identity is invalid.
- Tombstones remain history versions and suppress only snapshots at or after their system boundary.
- A schema mismatch, unsupported command, unknown configured table, corrupt checksum, or impossible
  transition fails closed. No partially recovered owner is returned.
- The current recovery owner accepts Temporal Mutation Command records only. Mixed application-kind
  dispatch is an explicit subsequent database-runtime task.
- Retained-history restore is valid only on a fresh provider. Failed ordering, lineage, transition,
  schema, or capacity validation leaves that provider empty and usable.

## Complexity and tradeoffs

Encoding and decoding are linear in batch bytes plus identity metadata. Physical-to-scalar
application is linear in rows times columns. Snapshot resolution is linear in logical identities
with a binary search within each identity history. Current commit publication copies all retained
histories and the time index, so its cost is linear in retained versions and temporarily doubles
their memory. This conservative copy-on-commit design makes allocation failure atomic. Replacing it
requires measured evidence and a representation that preserves the same publication guarantee.

The live executor runs on the table/tablet's serialized writer. It materializes owned scalar values
and calls `validate_next_commit` before copying command bytes into the bounded WAL coordinator. A
successful coordinator completion is checked against the exact requested durability mode and WAL
position contract. Only then are the returned WAL identity and sequence attached and published.
Submission rejection leaves the provider usable because no WAL work started. Any error after
admission fails the provider closed; queries and later writes return `UNAVAILABLE` until whole-WAL
recovery reconciles whether the command exists. This prevents a later commit from skipping an
uncertain durable predecessor.

The v1 tombstone carries a complete schema-shaped row. This uses more space than an identity-only
tombstone but keeps canonical schema validation and row alignment uniform. CSEG history may later
use a different accepted format; it must not reinterpret these bytes in place.

## Validation and interview questions

Focused tests cover canonical command application, exact and disagreeing checkpoint-overlap WALs,
multi-tablet routed verification/application, retained and reclaimed pre-boundary rows,
original/correction replay, historical lookup, scalar-winner vector materialization,
impossible-history rejection, and continued WAL sequence ownership. Phase 18 retains golden bytes,
fuzzing, allocation-failure sweeps beyond the implemented command-specific recovery and canonical
empty/one-CSEG/two-CSEG one-tablet plus CSEG-backed/empty two-tablet Manifest startup paths, crash
points, many-tablet skew, mixed-command dispatch, direct vector differential/lowering, long-history
retention models, durable compaction, and performance measurement.

Useful design questions include:

- Why is the WAL sequence authoritative when the command also stores system time?
- Why does recovery preflight the whole log before publishing any row?
- How does copy-on-commit preserve atomic visibility under allocation failure?
- Why may one table's recovered positions have gaps?
- What state must an application checkpoint retain before older WAL segments can be reclaimed?
