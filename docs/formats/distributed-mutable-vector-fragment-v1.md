# Distributed Mutable Vector Fragment v1

> **Status: accepted and implemented.** These bytes name one exact committed/applied immutable
> `TabletState` publication. They are distinct from Distributed Vector Fragment v1/v2, whose
> applied position is also the exact durable Manifest boundary.

All integers are little-endian. The frame has a 248-byte header, `1..4096` unique projection
ordinals, one exact Distributed Vector Plan Intent v1, one exact Distributed Vector Result Schema
v1, and a four-byte complete-frame CRC32C trailer. Its maximum length is 4,344,172 bytes.

The header contains, in order:

- magic `CHDMVFR1`, version `1.0`, header length, and exact frame length;
- exact nested plan and result-schema lengths;
- query, database, table, tablet, destination-schema, and Raft-group UUIDs;
- serving node, exact applied position, observed leader-commit position, and placement epoch;
- optional maximum staleness, optional read-barrier term/context/index, projection count, flags,
  consistency mode, and seven zero bytes;
- optional lower and upper event-time values;
- independent CRC32Cs for the nested plan and result schema;
- CRC32C of bytes `[0,240)`, followed by four zero bytes.

Flags are lower-present, lower-inclusive, upper-present, upper-inclusive,
maximum-staleness-present, and barrier-present in bits 0 through 5. Unknown bits reject. An absent
numeric option is encoded as zero; inclusive without present is noncanonical. The complete trailer
covers every preceding frame byte.

Decoding validates the hard/caller bounds, magic, header CRC, version, reserved bytes, lengths,
complete CRC, optional-field canonical form, and both nested CRCs before allocating projection or
decoding nested values. Exact decoding rejects truncation and trailing bytes. Successful decoding
returns owned identities, projection, plan, and result schema. CRCs provide integrity, not peer
authentication.

`bind_distributed_mutable_vector_fragment` constructs the value only when the selected immutable
tablet publication exact-matches the admitted Raft group and applied position, active schema,
committed placement, serving replica, consistency proof, projection, plan, and result schema. The
worker repeats local group/node/placement/barrier/schema/publication checks before scanning any
head. No Manifest generation or durable position is inferred.

Unknown versions return `NOT_SUPPORTED`; damaged bytes return `CORRUPTION`; invalid encoder input
returns `INVALID_ARGUMENT`; caller-bound exhaustion returns `RESOURCE_EXHAUSTED`.
