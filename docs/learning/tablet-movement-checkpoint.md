# Tablet Movement Checkpoints

## Purpose and interface

`encode_tablet_movement_checkpoint_v1` captures a self-contained movement restart point.
`decode_tablet_movement_checkpoint_v1` verifies framing, integrity, bounds, and phase semantics.
`TabletMovement::recover` then reconstructs the owning state machine from the decoded record and
snapshot prefix.

`TabletMovementCheckpointStorage` owns a locked directory for one tablet. It installs and selects
versioned generation envelopes whose embedded coordinate prevents renaming valid old bytes into a
false latest recovery point.

`TabletMovementSnapshotChunk` separately binds one payload range to the full tablet/epoch/source/
target and snapshot boundary. This is the immutable piece format for removing full-prefix rewrites;
`TabletMovementSnapshotChunkStorage` owns those pieces under one session-bound directory lock. It
installs only the contiguous end, exact-retries immutable offsets, reconstructs progress by decoding
every piece after restart, and validates the complete content CRC. Checkpoint handoff remains the
next layer.

`TabletMovementCheckpointReference` is the compact handoff value: it stores the movement record and
the original chunk-session placement epoch without copying prefix bytes. Structural decode is
deliberately weaker than recovery authority. The future generation owner must exact-load the
derived chunk session and pass those bytes through full movement validation before adoption.

## Invariants and ownership

The record and prefix agree on received length. Replica/learner sets are sorted and unique. Before
promotion the source is a voter and target the sole learner; after promotion both vote; after
completion the source is absent and target votes. Caught-up and later phases retain all snapshot
bytes and verify the declared whole-content CRC.

Encoded and decoded buffers are owning vectors. Readers borrow only during decode. Recovery moves
the record and prefix into one `TabletMovement`, after which the caller must serialize mutations.
No native layout or unaligned typed load is used.

## Failure behavior and complexity

Header relationships and CRC are checked before payload allocation. Counts and lengths are bounded
before reserve/copy. A framed but semantically impossible checkpoint is corruption on decode and
invalid input on encode/recover. Encoding and decoding are linear in replica metadata plus received
snapshot bytes and require one output/owned-prefix allocation.

Storage exact-reads before file sync, renames without replacement, and treats the final directory
sync as the success boundary. A post-rename directory-sync failure poisons the live owner because it
cannot know whether a crash will retain the name. Reopen removes only canonical temporaries,
requires generations contiguous from one, and revalidates generation, tablet, nested checksums, and
semantic state before recovery.

## Tradeoffs and likely interview questions

Self-contained prefixes simplify audit and recovery but can rewrite large prefixes. Generation-
installed chunk files may reduce amplification later, provided their exact checksums and identity
remain bound to an atomic checkpoint.

- Why are header, payload, and whole-record CRCs separate?
- Why is a full content CRC required only once transfer is complete?
- Why must pre-promotion voters leave capacity for the learner?
- Why is movement progress not stored as a metadata Raft command?
- Why must the generation be inside the checksummed envelope rather than only in the filename?
- Why does a directory-sync failure poison the owner after rename?
