# Raft Transport Envelope

## Purpose and public interface

`encode_raft_transport_envelope_v1` and `decode_raft_transport_envelope_v1` convert one
`RaftTransportEnvelope` between owned in-memory values and canonical bytes. The envelope binds a
logical group UUID, source node, destination node, and one of the eight current deterministic Raft
messages. `RaftTransportCodecLimits` provides finite frame, entry, entry-byte, and voter bounds.

## Data and ownership

Encoding borrows the envelope for the call and returns one owned byte vector. Decoding borrows the
input bytes only while validating and copies all variable append payloads and snapshot voters into
an owned result. Neither operation stores pointers, opens sockets, or mutates Raft state. Allocation
failure returns `RESOURCE_EXHAUSTED` without a partial value.

The 96-byte header protects lengths and route fields before variable parsing. A payload checksum
localizes damage, and the trailer covers the complete header and payload. Integers are explicit
little-endian values; UUIDs use canonical byte order; native structs are never serialized.

## Invariants and failure behavior

- The group is nonnil; nodes are nonzero and distinct.
- Candidate/leader request identity equals the claimed source.
- Append indexes are consecutive and all entry state fits configured bounds.
- Snapshot voters are ascending, unique, nonzero, and bounded.
- Boolean and reserved bytes have one canonical representation.
- An unknown checksum-valid version/kind is unsupported, corruption never reaches the state machine,
  and every payload is exactly exhausted.

CRC32C establishes only accidental-damage integrity. The socket owner authenticates before decode,
authorizes the stable principal for the claimed source, and checks the local destination before
dispatch. Likewise, the codec does not satisfy persist-before-send: the durable Multi-Raft owner
must synchronize the transition before transport sees its outbound envelope.

## Complexity and tradeoffs

Fixed messages encode and decode in constant time and space. Append messages are linear in entry
count and payload bytes; snapshot messages are linear in voter count. Decoding deliberately copies
payloads because the runtime owns messages beyond a network read buffer's lifetime. A future
carrier may add fixed-storage partial framing outside this API, but cannot reinterpret v1 bytes.

The separate envelope keeps clocks, retry, TLS, and descriptors out of the consensus core. It costs
one outer CRC pass and can reject a core-produced append batch that exceeds its configured transport
bound; explicit batching is preferable to unbounded socket ownership.

## Verification and likely interview questions

Focused tests cover every variant, actual conflict repair, corruption, compatibility, identity, and
bounds. Phase 18 retains golden fixtures, hostile length matrices, fuzzing, allocation failure,
partial read/write ownership, authenticated routing, partitions/reordering/duplication, and mixed
version processes.

Useful questions include: why is CRC not authentication; why must persistence precede sending; why
does snapshot metadata travel separately from snapshot bytes; how does route identity prevent
cross-group application; and why can a failed append response legitimately carry a nonzero match
index?
