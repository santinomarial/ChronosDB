# Bounded Connection Buffers

`ConnectionBuffers` separates portable stream ownership from the Linux readiness backend. Reads
append only within a finite limit, validate a 40-byte header, extract every complete frame in order,
and retain an incomplete suffix. Writes queue canonical immutable frames and expose only the
unwritten suffix of the front frame.

Inbound capacity, outbound retained bytes, and outbound frame count are independently bounded.
Admission never blocks and never creates a fallback queue. `clear()` releases all peer-controlled
bytes on disconnect. Receive cost is linear in accepted bytes plus retained-suffix compaction;
partial write consumption is constant time.

Tests enumerate every two-part split, multiple coalesced frames with a partial successor, exact
short-write offsets, hostile limits, cleanup, and owned allocation failures. Later epoll tests apply
the same state through real nonblocking sockets.
