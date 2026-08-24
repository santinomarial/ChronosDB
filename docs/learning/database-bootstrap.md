# Durable database bootstrap

## Purpose and public boundary

`DatabaseBootstrap::open_or_create` turns an existing dedicated directory into, or reopens it as,
one identified ChronosDB database. It does not open WAL or Raft history. It returns only after a
checksummed descriptor and both subsystem directory names are durable, and it keeps the root writer
lock until explicit close or destruction.

The public codec is useful for inspection and hostile-input testing. The storage owner exposes the
validated descriptor plus derived WAL/Raft paths; subsystem-specific strong IDs are constructed by
the composed database owner.

## Ownership, ordering, and recovery

The owner holds one `PosixDirectory` and one `PosixAdvisoryLock`. All creation is descriptor-relative,
does not follow final-component symlinks, and uses exclusive file/directory creation. A new image is
written and file-synchronized as `BOOTSTRAP.tmp`; the root is synchronized before and after child
directory creation, and again after the no-replace final rename.

If creation stops after the intent boundary, the next opener reads and validates that intent and
finishes the exact same installation. It never substitutes the new proposal. A final descriptor
with a temporary peer or missing/wrong-type subsystem directory fails closed.

## Failure behavior, complexity, and tradeoffs

Parsing is constant-space and constant-time over 128 bytes. Root listing during incomplete creation
is linear in directory entry count and rejects unrelated state. An established database may contain
later subsystem directories/files; reopen validates only the bootstrap and required direct children.

The bootstrap intentionally stores a small set of operational limits because they affect replay and
admission reproducibility. It does not duplicate table policy or format ceilings. A failed creation
can leave a valid intent and partial directories, but those are resumable; no destructive cleanup is
performed. Candidate database and metadata-group identities are generated before this owner is
called; Linux process qualification fails each read independently, requires the root to remain
empty, and then initializes it with the ordinary daemon. Failure can also occur immediately after
the final bootstrap is ready: WAL identity allocation is a later boundary. The same qualification
injects that exact failure, observes the final descriptor plus both subsystem directories, and then
requires two ordinary daemon starts to reopen the same root and reach configured service. This
complements, but does not replace, the deferred crash matrix at each bootstrap synchronization
boundary.

## Review and measurement questions

Reviewers should ask which file name marks readiness, which synchronization proves each directory
entry, why regenerated UUIDs cannot replace an interrupted intent, and how a second process is
excluded. Benchmarking is limited to startup latency and root-directory scaling; no throughput claim
depends on this path.
