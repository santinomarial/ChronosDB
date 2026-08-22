# ADR 0071: Segmented Multi-Raft persistence and recovery

- **Status:** accepted
- **Date:** 2026-08-08
- **Owners:** ChronosDB distributed-systems maintainers

## Context

ADR 0010 requires many logical groups to share one physical persistence stream. ADR 0069 and the
Multiplexed Raft Persistent-State Record v1 established checksummed full-state records and a
persist-before-send transition, but did not own files, synchronization, rotation, or recovery.

## Accepted decision

One single-thread-affine `RaftPersistentLog` owns an existing dedicated directory, its `LOCK`, and
the active segment. Segments have independently checksummed version-1 headers and contain an exact
concatenation of Multiplexed Raft Persistent-State Record v1 values. Segment numbers and physical
record sequences are contiguous. Logical group indexes remain inside each record and never derive
from a file offset.

`append` establishes only a complete operating-system write boundary. `synchronize` calls the
platform data-synchronization primitive and advances the local durable physical sequence. Rotation
synchronizes and closes the predecessor, writes and fully synchronizes a temporary successor
header, atomically renames it without replacement, and synchronizes the directory before allowing a
record into the successor. No outbound Raft message covered by a transition may be released until
its required append/sync policy has completed.

Recovery holds the exclusive lock, rejects unknown/non-regular entries, requires contiguous segment
and record sequences, validates headers before length-driven allocation, validates every complete
record, and reconstructs the latest full state per group. A caller-selected repair mode may truncate
only a structurally incomplete suffix in the highest segment after the last fully verified record;
checksum-invalid complete headers or records are never repaired. Repair synchronizes the truncated
file and directory. Repeated recovery over unchanged bytes is idempotent.

The initial implementation uses full-state records and a bounded caller-provided batch.
`DurableMultiRaftRuntime` executes deterministic operations, appends every persistent transition,
performs one sync for the batch, and only then releases its transitions and outbound messages. It
also composes a complete all-group full-state checkpoint with the immutable recovery anchor from
ADR 0269 before reclaiming older whole segments. It does not yet expose `QUORUM_SYNC` through the
native client durability negotiation.

Physical sequence `UINT64_MAX` is a valid terminal record identity but has no successor. If a
runtime recovers or emits that identity, later transition admission fails closed before calling the
group core. This prevents an operation from changing volatile term, vote, log, commit, apply, or
snapshot state when no physical persistence identity remains for the resulting transition.
The node-local outbound bound is also validated against the maximum voter count at construction, so
every single core transition fits before the core may change state.
Before constructing any group, the durable runtime tightens the core's aggregate persistent-state
payload budget to the configured segment target less segment and record framing. This makes every
admitted full-state transition encodable in an otherwise empty segment; an oversized proposal or
replacement is rejected nonterminally before group mutation instead of failing during durable
append.
Before a durable batch dispatches its first operation, it reserves the configured worst-case
outbound fanout of the complete operation mix. An undersized aggregate batch bound returns a
nonterminal resource-exhaustion error without changing any group's volatile or persistent state.

## Consequences and alternatives

Full-state records increase write amplification but make the latest state of every group explicit
and independently decodable. One file per group was rejected because it destroys shared batching;
embedding Raft state into WAL v1 was rejected because it would reinterpret a frozen application
format and create ambiguous durability semantics. Automatic corruption truncation was rejected
because it could hide loss of a previously durable record.

The production-path `chronos-raftbench` harness measures declared batches of these full-state
records in separate APPEND_ONLY and LOCAL_SYNC modes. It retains raw batch latencies, exact log
images, and complete immediate-reopen validation artifacts. It publishes no baseline, does not
equate APPEND_ONLY reopen success with durability, and does not measure quorum commit latency.

## Affected invariants and validation

Invariants 1, 4, 5, 8, 10, 14, and 18 apply. Focused tests cover rotation, shared-group recovery,
latest-state reconstruction, sequence continuation, exclusive ownership, explicit incomplete-tail
repair, fail-closed complete-record corruption, batched persist-before-send release, durable
applied-index recovery, terminal-sequence pre-admission, one-transition outbound-bound validation,
aggregate durable-batch outbound pre-admission, exact segment-target state-budget admission, and
anchored all-group prefix reclamation. Injected close failures now cover every nonempty failure
combination across the owned physical handles: the active segment, advisory lock, and directory are
all invalidated, the first error is retained, repeated close is idempotent, and the durable state
reopens exactly. Ambiguous active-record `pwrite` and `fdatasync` failures now execute the real
operation before returning `EIO` through the asynchronous owner. Both withhold the current
transition, fan the retained root cause out to queued work, release the physical lock, and exactly
reopen the complete term/vote record that reached the real file. The predecessor synchronization
and close boundaries of rotation likewise execute the real operation before returning `EIO`; the
poisoned writer retains the root cause and reopen recovers the exact predecessor prefix before a
successful retry rotates. Successor segment and recovery-anchor installation, reclamation, and
recovery syscall failures, process crash points, sustained corruption campaigns, and Linux
power-loss qualification remain required. A focused CLI smoke test covers both benchmark modes and
exact artifact/recovery status; a clean controlled-host measurement campaign remains required
before making performance claims.
