# COLUMNAR_APPEND v1 Command Codec

> **Status: implemented pure in-memory command layer.** This layer does not submit to the WAL,
> reserve retry identities, route rows, mutate tablet state, or publish logical success. The
> authoritative bytes remain [WAL v1](../formats/wal-v1.md),
> [Columnar Batch v1](../formats/columnar-batch-v1.md), and
> [ADR 0015](../adr/0015-columnar-batch-v1-and-wal-append-command.md).

## Purpose and interfaces

`chronos::wal::encode_application_payload` and `decode_application_payload` handle the generic
16-byte application envelope without interpreting a kind. `chronos::ingest` then provides nominal
`ClientId` and `ClientBatchId` values, streamed SHA-256, canonical mutation-digest computation,
`encode_columnar_append_v1`, prefix/exact decoders, an adapter for an already integrity-verified
`wal::DecodedRecord`, and catalog-dependent `validate_columnar_append_schema`.

The encoder accepts one existing immutable `EncodedColumnarBatch`. Its result is a move-only
`EncodedApplicationPayload` owning exactly the envelope, 160-byte command header, and batch—no WAL
record framing or trailing capacity. Encoding re-validates the supposedly canonical owned batch so
metadata and digest inputs are derived from validated bytes.

## Durable structure and digest

The constants and offsets live in `columnar_append_format.hpp`. The 176-byte prefix consists of the
frozen 16-byte application envelope followed by the frozen 160-byte command header. The only suffix
is one exact Columnar Batch v1 object. At the maximum batch length the payload is 16,777,168 bytes;
WAL v1 adds four padding bytes, its 40-byte header, and four-byte trailer to reach the exact
16,777,216-byte record maximum.

The request digest hashes the accepted domain separator followed by fixed little-endian format,
kind, and mutation values; table, tablet, and schema identities; schema version; batch length; and
the exact batch bytes. Client identities, outcome fields, WAL position, and durability mode are
intentionally absent. The implementation streams those fragments through OpenSSL 3 EVP without a
batch-sized preimage allocation. Published SHA-256 vectors and an independently generated command
golden fixture prevent provider behavior from defining the contract.

## Decoding, ownership, and failure behavior

Prefix decoding reports `kIncomplete` with the currently known required byte count: 16 bytes before
the envelope, 176 before the command header, then the exact length declared by the bounded header.
Exact decoding rejects a trailing byte. Unsupported envelope versions/kinds/required flags and
future command semantics return `kUnsupported`; malformed identities, reserved fields, nested-batch
damage, metadata disagreement, and digest disagreement return `kCorruption`; caller bounds return
`kResourceLimit`. Provider failures are separately classified as internal errors.

Successful `DecodedColumnarAppendView` values borrow the immutable input payload. Their nested
`DecodedColumnarBatchView`, column buffers, and cells share that lifetime. Descriptor vectors and
small metadata values are owned by the decoded values. No view may outlive or observe mutation of
the original bytes. The record adapter requires a `DecodedRecord` from the physical WAL codec, so
the physical record CRC has already succeeded; it still checks record format/type and exact payload
length.

Physical decoding is schema-independent. It proves both inner CRC32C scopes, canonical batch
layout, command/batch table-schema-version-row agreement, and the request digest. Schema binding is
an explicit second stage that checks exact catalog identity/version and column ordinal/type/
nullability through the batch codec. Active-ingest-schema policy and tablet routing belong to later
admission code.

## Complexity and tradeoffs

Digesting and decoding are `O(batch bytes)`; descriptor memory is `O(columns)`. Encoding is also
`O(batch bytes)` and currently performs batch validation plus digesting and one owned payload copy.
That deliberate revalidation makes the public trust boundary explicit. The codec does no per-row
allocation. A maintained crypto provider adds a dependency but avoids owning security-sensitive
SHA-256 code; its boundary is recorded under `docs/dependencies/openssl.md`.

The microbenchmarks separate digest, full command encoding, and full command decoding for 64,
1,024, and 65,536-row timestamp batches. They retain CRC and digest checks and label results as
local measurements; comparisons must follow the benchmark contract.

## Questions worth being able to answer

- Why is the request digest not a replacement for either WAL record CRC32C or batch CRC32C?
- Why can a larger corrupted batch length be classified as incomplete by a prefix decoder?
- Which fields may change across a retry without changing the canonical mutation digest?
- Why does schema binding happen after physical decoding?
- What storage must outlive a decoded command and all of its nested cell views?
