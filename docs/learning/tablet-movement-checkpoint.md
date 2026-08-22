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
every piece after restart, and validates the complete content CRC. It can also revalidate and load
through an exact earlier chunk boundary for checkpoint composition.

`TabletMovementCheckpointReference` is the compact handoff value: it stores the movement record and
the original chunk-session placement epoch without copying prefix bytes. Structural decode is
deliberately weaker than recovery authority. `install_verified_tablet_movement_reference` derives
and exact-matches the chunk session, validates the claimed durable boundary and full movement, and
only then installs the generation. Its
`CHRMVRG` generation envelope is intentionally distinct from the self-contained `CHRMOVG` envelope.
The generation storage exact-dispatches those magics within one contiguous sequence and exposes a
variant load; legacy typed loads fail closed rather than selecting an older generation.
`recover_tablet_movement_generation` restores self-contained generations directly and requires the
session-bound chunk owner for reference generations.

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

Chunks must become durable before the reference that claims them. A crash between those writes can
leave chunks ahead of the latest checkpoint; recovery deliberately reconstructs only the
checkpointed boundary and ignores the suffix. The opposite state is corruption. This makes the
checkpoint generation—not the longest observed chunk prefix—the movement progress authority.

## Tradeoffs and likely interview questions

Self-contained prefixes simplify audit and recovery but can rewrite large prefixes. Immutable chunk
files remove repeated prefix writes while the compact checkpoint remains the sole progress
authority. Final application-snapshot installation and safe chunk reclamation are separate layers.

The RTAS portion of final application-snapshot installation is now implemented by
`install_recovered_tablet_movement_snapshot`. It consumes the recovered checkpoint-owned bytes,
requires canonical RTAS identity and transfer-coordinate agreement, and crosses the existing RTAS
directory durability boundary. Physical Manifest/CSEG transfer and safe chunk reclamation remain
separate.

The matching Raft metadata completion is now composed by
`complete_recovered_tablet_movement_raft_snapshot`: only catching-up movement may enter it, the
pending leader request must equal the RTAS's full metadata and movement endpoints, and success is
returned only after the target Raft state is synchronized. Physical part transfer, response routing,
and reclamation remain separate orchestration. Subprocess coverage kills the composed handoff after
each RTAS durability operation, the Raft state write/sync, and success release; public reopen must
converge by resuming an incomplete handoff or answering an exact retry from persisted Raft
authority.

`checkpoint_recovered_tablet_movement_catch_up` closes the checkpoint-advancement crash window. It
requires the exact installed RTAS and target Raft boundary, advances a private candidate, installs
the next ready generation, and only then mutates the live recovered movement. Reference generations
revalidate the durable chunks; self-contained generations remain self-contained. Promotion,
response routing, and reclamation remain later steps. This is an application-only primitive: a
physical movement must use `checkpoint_tablet_physical_movement_readiness`, which additionally
projects the target tablet from one owned destination publication epoch and exact-matches its
canonical part-set checksum to the RTAS/Raft snapshot before permitting the ready checkpoint.

- Why are header, payload, and whole-record CRCs separate?
- Why is a full content CRC required only once transfer is complete?
- Why must pre-promotion voters leave capacity for the learner?
- Why is movement progress not stored as a metadata Raft command?
- Why must the generation be inside the checksummed envelope rather than only in the filename?
- Why does a directory-sync failure poison the owner after rename?
- Why may recovery accept chunks ahead of a checkpoint but never a checkpoint ahead of chunks?
