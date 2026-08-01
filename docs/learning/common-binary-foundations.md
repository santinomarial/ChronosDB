# Common Binary Foundations

Phase 1B supplies the small, portable primitives that later codecs can compose. It does not define
a WAL record, page, manifest, network frame, or any other durable format. Those formats must still
specify versions, field meanings, size limits, integrity boundaries, and compatibility rules.

## Interfaces and ownership

`Status` owns its message and represents either `kOk` or a stable non-OK `StatusCode`. Default
construction deliberately means success. Constructing an OK status with text clears the text, and
constructing a non-OK status with an empty message supplies its stable code name. `Result<T>` is the
project alias for `std::expected<T, Status>`; `Result<void>` and move-only values work without a
second expected implementation or exception-based ordinary error flow.

`ByteView` is `std::span<const std::byte>` and `MutableByteView` is
`std::span<std::byte>`. A span owns no memory. Its caller must keep the backing buffer alive for the
view, every slice, and every sub-reader. Mutable storage also requires exclusive access during a
write. Reader and writer objects are not internally synchronized. Helpers accept only explicitly
byte-oriented `uint8_t` buffers; there is deliberately no general "serialize this object" helper.

`std::byte` makes raw binary data distinct from characters and numbers. `std::span` carries a pointer
and length together, permits zero-copy slicing, and makes the bounds available to every operation.
Neither type validates lifetime, so the borrowing rule remains part of the API contract.

## Bounds, arithmetic, and failure atomicity

Checked addition, multiplication, and range-end calculations return `std::optional`; overflow has
no numeric result. Alignment returns `Result<T>` so it can distinguish invalid zero/non-power-of-two
alignment from arithmetic overflow. The helpers accept unsigned integer types other than `bool`,
avoiding signed-overflow undefined behavior.

`ByteReader` and `ByteWriter` keep an offset into a borrowed span. They compare a request with
`remaining()` before computing a new offset, so a hostile length cannot wrap an end position. A
failed read leaves the cursor unchanged. A failed primitive or bulk write leaves both the cursor and
destination unchanged. Successful operations allocate no heap memory; only an error message may
allocate.

Integers are assembled or stored one byte at a time using shifts on unsigned fixed-width values.
There are no unaligned typed loads or stores and no pointer type-punning. Signed values preserve
their object representation through `std::bit_cast` from the corresponding unsigned width. Floats
do the same after their integer bit pattern has been read, which preserves negative zero, infinity,
NaN payload bits, and subnormal representations on supported IEC 559 binary32/binary64 platforms.

## Little-endian policy

All multi-byte reader and writer methods are explicitly suffixed `_le`. Byte zero carries the least
significant eight bits regardless of host endianness. Returned integers and checksums are host
numeric values; a format must call an explicit writer method to serialize them.

Durable data must never be produced by dumping a native C++ struct. Native structs can contain
padding, indeterminate bytes, compiler-dependent layout, host-endian fields, alignment constraints,
and representations that change across ABI or architecture. A real durable format instead assigns
fixed-width fields and encodes each one explicitly, after an accepted format specification defines
the surrounding version and integrity rules.

## CRC32C contract

CRC32C uses the Castagnoli polynomial, not the IEEE CRC32 polynomial and not a general-purpose hash.
It detects common accidental transmission/storage corruption; it does not provide authentication,
collision resistance, or protection against a malicious editor.

`Crc32c` begins with an internal state of all ones and XORs the final value with all ones.
`crc32c(bytes)` is the one-shot form. `Crc32c::extend` accepts arbitrary successive chunks, while
`extend_crc32c(finalized_checksum, bytes)` unfinalizes, extends, and finalizes an existing checksum.
Every chunking of the same byte sequence therefore has the same result. The standard check value for
ASCII `123456789` is `0xe3069283`; empty input is zero.

The implementation uses an immutable compile-time-generated 256-entry lookup table and the reversed
Castagnoli polynomial `0x82f63b78`. A portable software path comes first because it is easy to test
on Linux and macOS and defines the reference behavior. Hardware CRC instructions require CPU
feature detection, architecture-specific code, runtime dispatch, and equivalence benchmarks; those
costs need measured justification in a later task.

## Examples

Encoding and decoding a fixed layout remains explicit:

```cpp
std::array<std::byte, 12> storage{};
chronos::common::ByteWriter writer{storage};
if (const auto status = writer.write_u32_le(7); !status.is_ok()) {
  return chronos::common::make_unexpected(status);
}
if (const auto status = writer.write_float64_le(1.5); !status.is_ok()) {
  return chronos::common::make_unexpected(status);
}

chronos::common::ByteReader reader{storage};
const auto identifier = reader.read_u32_le();
const auto measurement = reader.read_float64_le();
if (!identifier || !measurement || !reader.empty()) {
  return chronos::common::make_unexpected(
      chronos::common::Status{chronos::common::StatusCode::kCorruption,
                              "invalid example payload"});
}
```

Incremental CRC does not depend on input segmentation:

```cpp
chronos::common::Crc32c checksum;
checksum.extend(header_bytes);
checksum.extend(payload_bytes);
const std::uint32_t value = checksum.value(); // host integer, not encoded bytes
```

Unsafe approaches intentionally rejected include `reinterpret_cast<const Header*>(bytes.data())`,
unaligned typed dereferences, `memcpy` of a native struct as a wire image, offset-plus-length checks
performed after unchecked addition, and interpreting a CRC32C value as cryptographic integrity.

## Complexity and tradeoffs

Fixed-width reads/writes and checked arithmetic use constant time and constant auxiliary space.
Copying or zero-filling `n` bytes is `O(n)` time and `O(1)` auxiliary space. CRC32C is `O(n)` time,
uses a fixed 1 KiB table, and allocates nothing. Views and sub-readers are constant-size, zero-copy
objects. The byte-at-a-time codec and table-driven checksum prioritize portability, auditability,
and defined behavior over peak throughput; future optimization requires a benchmark and an
equivalence test against this path.

## Likely interview questions

- Why does a span improve bounds safety without solving lifetime safety?
- Why is unsigned assembly followed by `bit_cast` safer than an unaligned signed load?
- How can `offset + length <= size` overflow, and what comparison avoids that problem?
- What does failure atomicity promise for a reader and a writer?
- Why is native-struct serialization neither portable nor safely versioned?
- What distinguishes CRC32C from IEEE CRC32 and from a cryptographic hash?
- How can an already finalized CRC32C be extended without changing chunking semantics?
- What evidence is needed before adding a hardware-accelerated checksum backend?
