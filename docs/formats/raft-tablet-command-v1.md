# Raft Tablet Command v1

> **Status: implemented for committed `COLUMNAR_APPEND` application.**
> Application commands retain these exact bytes inside the separate versioned
> [Raft Tablet Application Snapshot v1](raft-tablet-application-snapshot-v1.md).

One logical tablet Raft group stores application commands in the `LogEntry` values embedded by the
[Multiplexed Raft Persistent-State Record v1](multiplexed-raft-log-v1.md). Logical entry type `1`
means `COLUMNAR_APPEND v1`. Types `0` and all unassigned values are invalid or unsupported and must
not be interpreted as an append.

## Entry payload

The payload is the exact byte sequence returned by `encode_columnar_append_v1`: one complete WAL
application payload beginning with its `COLUMNAR_APPEND` envelope and containing one canonical
[Columnar Batch v1](columnar-batch-v1.md). WAL record framing, WAL ID, WAL record sequence, segment
position, and WAL record CRCs are absent. The surrounding multiplexed Raft record provides durable
physical integrity; the command request digest and embedded batch checks independently preserve
logical identity and nested bytes.

An implementation must use exact decoding. Incomplete input, trailing bytes, digest disagreement,
invalid nested batch bytes, tablet/schema mismatch, an unknown command type, and a command outside
configured limits fail closed. Length fields are validated before allocation.

## Application identity and ordering

Only an entry at or below the group's committed index may be applied. Its logical commit position is
`(RAFT, group_uuid, log_index)`. The log index is never derived from a physical segment or record
sequence. The state machine applies indexes in ascending order, atomically publishes all rows and
the exact retry outcome for one command, and only afterward persists advancement of Raft's applied
index.

The client ID and client batch ID retain their existing retry semantics. A repeated exact mutation
adds no rows but advances the tablet's outer applied position to the duplicate entry's index. Reuse
of the identity for different command bytes is a conflict and fails the state machine closed.

## Recovery and compatibility

Without an application snapshot, recovery starts with fresh unpublished tablet and retry owners and
replays the complete retained committed prefix. It does this even when the latest Raft persistent
state already records the same applied index, because process memory is not durable. A nonzero
`snapshot.last_included_index` is rejected until a versioned tablet application snapshot can restore
the omitted prefix.

Readers that do not implement entry type `1` must report it as unsupported. Changing the payload or
identity semantics requires a new entry type or compatible minor-version rule; existing type-1 bytes
must never be reinterpreted.
