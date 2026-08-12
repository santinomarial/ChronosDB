# Database Bootstrap v1

> **Status: accepted and implemented.** This fixed image is the database-root authority used before
> the WAL, metadata Raft log, or data plane can open.

## File and installation

The final file is `BOOTSTRAP` in an existing dedicated database root. Creation first writes the
same image to `BOOTSTRAP.tmp` with exclusive creation, synchronizes the file, and synchronizes the
root directory. It then creates the `wal/` and `raft/` directories and synchronizes the root before
atomically renaming the intent to `BOOTSTRAP` without replacement and synchronizing the root again.

`BOOTSTRAP.tmp` without `BOOTSTRAP` is a restartable creation intent. Its validated identities and
limits are authoritative; startup completes the missing directories and final rename rather than
using newly proposed values. Both names together are corruption. A final file requires both
subsystem directories. The root `LOCK` advisory lock is held throughout bootstrap and the later
database-owner lifetime.

## Exact 128-byte image

All integers are unsigned little-endian. UUID byte arrays use canonical ChronosDB durable order.

| Offset | Width | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 8 | magic | ASCII `CDBROOT1` |
| 8 | 2 | major | `1` |
| 10 | 2 | minor | `0` |
| 12 | 4 | image size | `128` |
| 16 | 16 | database UUID | nonzero |
| 32 | 16 | metadata-group UUID | nonzero and distinct from database UUID |
| 48 | 8 | local node ID | nonzero |
| 56 | 4 | mutable-head row capacity | nonzero |
| 60 | 4 | maximum sealed generations | nonzero |
| 64 | 8 | bytes per variable-width column generation | nonzero, process-addressable |
| 72 | 8 | maximum retry-directory entries | nonzero, process-addressable |
| 80 | 8 | WAL target segment bytes | nonzero; subsystem validation applies later |
| 88 | 8 | Raft target segment bytes | nonzero; subsystem validation applies later |
| 96 | 28 | reserved | all zero |
| 124 | 4 | CRC32C | Castagnoli checksum over stored bytes `[0,124)` |

The decoder requires exactly 128 bytes, validates the checksum before interpreting variable
relationships, rejects nonzero reserved bytes, and rejects unknown versions. The image stores
operational limits that affect restart admission shape; format ceilings and deeper subsystem limits
remain enforced by their owning openers.

## Compatibility and failure policy

Major or minor changes require a new accepted format interpretation. Current code rejects every
unknown version rather than guessing defaults. A corrupt or incomplete final image prevents service
startup. Once `BOOTSTRAP` exists, caller-proposed new-database values are ignored so durable identity
cannot change on restart.
