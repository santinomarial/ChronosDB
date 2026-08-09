# Durable Metadata Raft State

## Purpose and interfaces

`DurableMetadataStateMachine` connects the dedicated Raft group to the deterministic catalog maps.
Entry type 2 is decoded under [Metadata Command v1](../formats/metadata-command-v1.md). The owner
provides read-only node, schema, tablet-placement, and retention lookups after committed application.

## Ordering and durability

```text
committed metadata entries
  -> exact checksum/version/limit decode of the complete batch
  -> apply variants in consecutive log-index order
  -> synchronize Raft applied_index
  -> optional leader quorum-sync/application proof
```

Pre-decoding prevents a corrupt later command in the current batch from being discovered only after
earlier catalog mutation. The underlying state machine enforces monotonic placement epochs, stable
tablet-to-table identity, canonical replica membership, leader-in-replica membership, stable schema
identity ownership, and bounded maps.

## Recovery and failure behavior

The applied index is not a catalog snapshot. Startup constructs empty state and replays every
retained committed entry, then leaves or advances the durable applied index as appropriate. A
compacted prefix is rejected because no metadata application snapshot exists yet. A failed live
application poisons the owner; restart revalidates authoritative log bytes.

Command size, endpoint bytes, replicas, nodes, schemas, and tablets are explicitly bounded. Decoding
validates the fixed header before length-driven work, owns endpoint and replica data, and never dumps
or loads a native struct.

## Complexity and likely interview questions

Decode and apply are linear in command bytes and replica count; map updates are `O(log N)`. Startup
is linear in retained committed history until snapshots exist.

- Why is metadata placement not safe as local last-writer-wins state?
- Why must the command batch preflight before mutation?
- Why does recovery replay commands when Raft says they were already applied?
- What must a metadata application snapshot bind before log reclamation is safe?
- Why does placement metadata not itself change Raft voting membership?
