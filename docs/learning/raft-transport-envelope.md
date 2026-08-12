# Raft Transport Envelope

## Purpose and public interface

`encode_raft_transport_envelope_v1` and `decode_raft_transport_envelope_v1` convert one
`RaftTransportEnvelope` between owned in-memory values and canonical bytes. The envelope binds a
logical group UUID, source node, destination node, and one of the eight current deterministic Raft
messages. `RaftTransportCodecLimits` provides finite frame, entry, entry-byte, and voter bounds.
`RaftTransportFrameReader` and `RaftTransportFrameWriteCursor` retain one frame safely across
arbitrary short reads and writes. The cluster-level `RaftTransportReceiver` authenticates and
authorizes a complete frame before asynchronously dispatching it to the durable Multi-Raft owner.

## Data and ownership

Encoding borrows the envelope for the call and returns one owned byte vector. Decoding borrows the
input bytes only while validating and copies all variable append payloads and snapshot voters into
an owned result. Neither operation stores pointers, opens sockets, or mutates Raft state. Allocation
failure returns `RESOURCE_EXHAUSTED` without a partial value.

The move-only stream reader owns a fixed header until it can validate every allocation-relevant
field, then owns one exactly sized frame until full decode. A successful step reports its exact
consumed prefix and returns at most one envelope, leaving a coalesced suffix with the caller. Any
failure is sticky. The move-only write cursor validates and owns a complete encoded frame; its
pending span remains valid until the cursor advances or moves.

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

The implemented receiver enforces that sequence. It rejects absent principals before parsing,
delegates exact source authorization to the embedding, checks the local destination, and submits
one owning receive operation followed immediately by its observation through the bounded
asynchronous runtime. Its completion is the acquire boundary for the already synchronized result
and post-message state. Outbound encoding borrows that result so the caller retains every message
if a configured frame limit is too small.

The inbound TLS carrier gives one event-loop thread exclusive session ownership. It reads through a
fixed scratch buffer without crossing a frame boundary, pauses with one asynchronous durable
operation in flight, and exposes the complete result for embedding-owned routing and snapshot work.
Handshake and incomplete-frame reads expire; already admitted durable work does not, because it may
have crossed an irreversible local synchronization boundary. Taking the asynchronous completion is
the mutex acquire edge that publishes the result to the carrier thread.

The outbound TLS carrier uses one preallocated fixed-slot ring per peer. Queue admission validates
the canonical source/destination and both frame/byte limits before moving caller bytes. One event-
loop thread owns all offsets, so no atomics are needed. A failed connection can drain complete FIFO
frames for whole-message retry; the partially written front is deliberately restarted at byte zero.

The peer pool preallocates a bounded set of exact-peer carrier slots. It borrows one durable result,
preflights every destination and aggregate per-peer frame/byte demand, encodes the complete result,
and only then distributes frames. A missing or ordinarily full route therefore consumes nothing.
Failed peers leave the map only through an explicit handoff that returns their carrier and complete
retry frames; address lookup, TCP connect, backoff, and replacement timing remain caller-owned.

The one-attempt TCP connector now retains complete retry frames while connecting, creates TLS only
after `SO_ERROR` proves success, and transfers the descriptor plus borrowing carrier as one value.
The pool owns that pair in TLS-before-descriptor destruction order. Connect failure returns the
unchanged retry set; address selection, backoff, and replacement timing remain policy-owned.

The per-peer reconnect owner now supplies that timing for one immutable route. It permits one
connector at a time, doubles a positive delay to an inclusive cap, resets after successful handoff,
and retakes the pool's complete failed frame set. It intentionally has no fresh-result side queue;
the bounded upstream owner must retain work until the peer can accept it.

The outbound peer manager composes a fixed catalog of those policies with the carrier pool. It
exposes exact connecting/connected descriptor interests, installs completed pairs, recycles failed
pairs, and routes a durable transition only if every destination passes preflight. This completes
outbound ownership without adding a hidden unbounded queue; the embedding still owns the poll loop
and the bounded durable-result completion that retries admission.

The inbound TCP server supplies the symmetric listener and fixed connection/poll table. Accepted
sockets derive their authentication address from the kernel peer endpoint and retain result-ready
sessions without read interest until the embedding takes the exact post-sync transition.
Each authenticated message and its immediately following owning group observation execute in one
durable FIFO batch, so timer rearming uses the exact post-message role and term rather than a later
racy observation.

Every timer and transport layer exposes its exact next monotonic deadline without mutation or
allocation. Aggregate owners select the minimum child value; idle, durable-in-flight, and
result-ready states return no invented deadline because descriptor readiness or explicit pickup is
their progress source.

## Complexity and tradeoffs

Fixed messages encode and decode in constant time and space. Append messages are linear in entry
count and payload bytes; snapshot messages are linear in voter count. Decoding deliberately copies
payloads because the runtime owns messages beyond a network read buffer's lifetime. Partial reading
adds one exact frame allocation after fixed-header validation; the default 64 MiB limit therefore
requires a separate carrier-wide admission budget.

The separate envelope keeps clocks, retry, TLS, and descriptors out of the consensus core. It costs
one outer CRC pass and can reject a core-produced append batch that exceeds its configured transport
bound; explicit batching is preferable to unbounded socket ownership.

## Verification and likely interview questions

Focused tests cover every variant, actual conflict repair, corruption, compatibility, identity,
bounds, bytewise and coalesced reads, sticky failure, short-write ownership, exact-peer pool
preflight, and failed-carrier retry handoff. Phase 18 retains golden fixtures, hostile length
matrices, fuzzing, allocation failure, connection churn, partitions/reordering/duplication, and
mixed-version processes. Focused real mutual-TLS coverage exercises fragmented persistent input and
bounded authenticated FIFO output.

Useful questions include: why is CRC not authentication; why must persistence precede sending; why
does snapshot metadata travel separately from snapshot bytes; how does route identity prevent
cross-group application; and why can a failed append response legitimately carry a nonzero match
index?
