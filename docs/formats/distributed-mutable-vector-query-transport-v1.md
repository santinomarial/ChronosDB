# Distributed Mutable Vector Query Transport v1

> **Status: accepted and implemented.** This request frame carries one exact Distributed Mutable
> Vector Fragment v1. Successful and failure responses reuse the schema-bound Distributed Vector
> Query Response v2 frame and are decoded against the fragment's exact result schema.

All integers are little-endian. The request is one 80-byte header, one exact mutable-fragment
payload, and one four-byte complete-frame CRC32C trailer. Its maximum length is 4,344,256 bytes.

The request header contains, in order:

- magic `CHDMREQ1`, version `1.0`, header length, and exact total length;
- source and target node IDs;
- exact payload length and CRC32C;
- 24 zero reserved bytes; and
- CRC32C of bytes `[0,76)`.

The target node must equal the nested fragment's serving node. Source and target are nonzero and
distinct. The trailer covers every preceding byte. Exact decoding verifies hard lengths, header
CRC, version, reserved bytes, complete CRC, payload CRC, the complete nested mutable fragment, and
route correlation before returning owned state. Streaming reception validates the complete header
before allocating the bounded frame and leaves coalesced successor bytes with the caller.

The request CRCs provide integrity, not authentication. `DistributedMutableVectorQueryReceiver`
requires a previously authenticated peer result, authorizes its principal for the claimed source
node, validates the local target, and only then invokes an embedding-owned worker. A subsequent TLS
carrier is responsible for producing that peer result before exposing request bytes.

Responses use `CHDVRSP2` unchanged because the response already carries query/tablet correlation,
typed result-schema validation, optional leader hints, terminal sequencing, bounded payloads,
header/payload/frame CRCs, and no durable-Manifest authority. Mutable versus durable authority is
therefore distinguished entirely by the request protocol and remains unavailable to the legacy
request decoder.

Unknown request versions return `NOT_SUPPORTED`; damaged request bytes return `CORRUPTION`;
invalid encoder input returns `INVALID_ARGUMENT`; bounded-reader exhaustion returns
`RESOURCE_EXHAUSTED`.

