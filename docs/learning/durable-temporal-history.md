# Durable temporal history

## Purpose and boundary

ChronosDB preserves corrections rather than overwriting an earlier business fact. Each logical row
therefore has event time, receive time, and an ordered sequence of system-time versions. The current
subsystem supplies three concrete layers:

1. Temporal Mutation Command v1 stores schema-shaped values and row-aligned temporal metadata.
2. `apply_committed_temporal_command` converts one already durable WAL command into owned history.
3. `execute_temporal_command` performs validated live WAL admission and acknowledged publication.
4. `recover_temporal_wal` rebuilds fresh multi-table scalar providers from verified WAL order.

Mixed command dispatch, application checkpoints, and Raft application are not hidden inside these
interfaces. CSEG v2 now has strict metadata/part codecs, semantic and projected reading, plus a
bounded single-lineage scalar resolver that provides a differential current/as-of winner oracle.
Manifest snapshot discovery, multi-part vector output, and compaction integration remain pending.

## Public interfaces and data structures

`EncodedTemporalCommand` owns the complete WAL application payload. `DecodedTemporalCommandView`
borrows those bytes and exposes an embedded `DecodedColumnarBatchView`, mutation descriptors, and
the command's system commit timestamp. Logical identities are nonempty, bounded, and unique within
one command.

`TemporalSnapshotProvider` owns histories for one exact table schema. Its map key is the logical
identity; each value is a system-position-ordered vector containing the owned scalar row, mutation
kind, event/receive time, WAL identity/sequence, and system commit time. A second ordered map turns
an as-of system timestamp into the latest committed position visible at that time.

`RecoveredTemporalState` owns one provider per configured table and the locked reopened
`WalWriter`. `release_writer()` transfers that writer exactly once to the later live coordinator.
Provider pointers remain valid for the lifetime of the recovered owner.

## Application and recovery sequence

Committed application first binds the decoded batch to the retained catalog schema. It copies each
physical cell through `ScalarValue::from_column_cell`, attaches the enclosing WAL identity and
record sequence, and calls `TemporalSnapshotProvider::apply_committed` once for the complete batch.
The WAL record sequence is authoritative system order; wall-clock time never replaces it.

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

Focused tests cover canonical command application, original/correction replay, historical lookup,
impossible-history rejection, and continued WAL sequence ownership. Phase 18 retains golden bytes,
fuzzing, allocation-failure sweeps, crash points, mixed-command dispatch, long histories, and
performance measurement.

Useful design questions include:

- Why is the WAL sequence authoritative when the command also stores system time?
- Why does recovery preflight the whole log before publishing any row?
- How does copy-on-commit preserve atomic visibility under allocation failure?
- Why may one table's recovered positions have gaps?
- What state must an application checkpoint retain before older WAL segments can be reclaimed?
