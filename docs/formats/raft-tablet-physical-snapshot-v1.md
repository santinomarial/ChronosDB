# Raft tablet physical snapshot projection v1

> **Status:** accepted semantic projection; source builder and exact destination validator are
> implemented. Restartable CSEG transfer and destination publication remain separate.

This projection reuses an exact canonical [Manifest v2](manifest-v2.md) byte sequence; it does not
assign a new magic, version, reserved field, or filename. Its interpretation is selected by the
physical-snapshot API and the enclosing Raft movement protocol.

The Manifest v2 image must have:

- exactly one tablet descriptor;
- no database-global WAL reclaim checkpoint;
- a nonzero Raft commit source and source UUID equal to the expected Raft group;
- a Manifest generation equal to `SnapshotMetadata.manifest_generation`;
- a tablet durable position equal to `SnapshotMetadata.last_included_index`;
- `first_part_index = 0` and `part_count` equal to the complete part descriptor count;
- every part descriptor owned by that tablet and source, as required by Manifest v2; and
- only retry descriptors owned by that tablet and source.

The Raft `part_set_checksum` is SHA-256 over the exact byte range occupied by the canonical Manifest
v2 part descriptor table:

```text
parts_offset = 256 + 128
parts_length = part_count * 224
part_set_checksum = SHA256(projection[parts_offset .. parts_offset + parts_length])
```

For zero parts, the input is the empty byte sequence. The Manifest v2 header and file CRC32C still
protect framing and accidental corruption. Each part descriptor's content SHA-256 independently
binds the exact future CSEG object; the aggregate digest binds the ordered descriptor set. Neither
digest is proof of durable destination installation.

Validation exact-decodes the full input with no trailing bytes and applies caller-provided Manifest
limits before returning an owning-value report. Any mismatch with expected group, table, tablet,
generation, applied position, or aggregate checksum is corruption. Invalid caller identities are
invalid arguments. Unknown Manifest/CSEG versions retain Manifest v2's unsupported classification.
