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

## Bounded structured diagnostics

`LogRecord` is a borrowed, synchronous description of one diagnostic. The caller owns every string
and field span until `encode_json_log` or `write_json_log` returns. Encoding produces one
newline-free JSON object with a millisecond RFC 3339 UTC timestamp and stable severity, component,
event, and message keys. At most 32 caller fields are accepted; built-in names cannot be shadowed,
caller names must be unique ASCII identifiers, and individual plus aggregate text limits fail
before unbounded log retention. Strings escape JSON controls, preserve valid UTF-8, and replace
invalid input bytes with U+FFFD so malformed diagnostics cannot damage downstream line parsing.

`write_json_log` owns no file. It serializes calls through one process-local mutex, writes exactly
one encoded object plus newline, and flushes before returning. The function therefore provides
atomic in-process log lines, not durable storage: successful return means the C stdio stream
accepted and flushed the bytes, not that a filesystem or collector persisted them. Logging is not a
data acknowledgment and cannot participate in WAL or Raft durability claims.

`RotatingJsonLogSink` adds an owning operational file boundary around the same encoder. Its factory
opens an append-only regular active file and a nonblocking exclusive advisory lock, rejecting
active/lock symlinks and competing cooperating processes. The owner serializes size accounting,
rotation, writes, and flushes with one mutex. Before a complete line would cross the configured
bound it removes the oldest archive, shifts suffixes from oldest to newest, renames the active file
to `.1`, and opens a fresh active file. A rotation or write error is terminal so later calls cannot
pretend the archive sequence is still healthy. Encoding/validation errors occur before mutation and
do not poison the sink. The mutex—not an atomic memory-ordering protocol—establishes the happens-
before relationship for file ownership and byte counts. Archive files are diagnostic evidence, not
database state; rotation does not directory-sync or claim crash-durable collection.

## Injectable time domains

`TimeSource` separates civil wall time from monotonic elapsed time. `wall_now()` is appropriate for
diagnostic timestamps but may move backward or forward and cannot define commit order, retention
authority, or timeouts. `monotonic_now()` is appropriate for in-process ages and deadlines but has
no portable civil meaning and is never serialized as authority. `SystemTimeSource` delegates to the
standard system and steady clocks; `system_time_source()` exposes one thread-safe process-lifetime
instance.

Injected implementations must make both `noexcept` calls safe for every calling thread. Consumers
borrow an injected source, so it must outlive all retained consumer state. This permits allocation-
free deterministic clocks while keeping ownership visible; it does not supply sleeps, event-loop
timers, time-zone conversion, or a mapping between the two clock domains.

## UUID entropy boundary

`SystemUuidGenerator` obtains fixed 16-byte candidates through `UuidEntropySource`. Its default
`SystemUuidEntropySource` reads the supported operating system, completing partial Linux
`getrandom` reads and retrying `EINTR`; macOS `arc4random_buf` fills the candidate. Entropy errors
propagate immediately. A nil candidate is retried at most eight times, so a broken source cannot
hang an identity-allocating operation indefinitely.

The Linux adapter delegates to one private getrandom-shaped completion loop. After each positive
partial result, the next call receives only the uninitialized suffix. `EINTR` retries that exact
suffix; zero progress, an invalid result larger than the request, and terminal errors fail before a
candidate is returned. Deterministic reader scripts exercise this production loop without adding a
syscall-injection surface to installed headers. The system reader captures `errno` immediately with
the failed syscall result, so diagnostics do not depend on later library calls.

Injected entropy sources are borrowed and must outlive the generator and overlapping calls; mutable
sources supply their own synchronization. Successful bytes stay opaque rather than acquiring
invented RFC version semantics. The owning subsystem remains responsible for the identity domain,
collision checks against its authority, no-reuse policy, and durable installation.

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
objects. A time-source read is allocation-free and constant-space; its latency is the underlying
platform clock's contract, not a database performance claim. The byte-at-a-time codec and
table-driven checksum prioritize portability, auditability, and defined behavior over peak
throughput; future optimization requires a benchmark and an equivalence test against this path.

## Likely interview questions

- Why does a span improve bounds safety without solving lifetime safety?
- Why is unsigned assembly followed by `bit_cast` safer than an unaligned signed load?
- How can `offset + length <= size` overflow, and what comparison avoids that problem?
- What does failure atomicity promise for a reader and a writer?
- Why is native-struct serialization neither portable nor safely versioned?
- What distinguishes CRC32C from IEEE CRC32 and from a cryptographic hash?
- How can an already finalized CRC32C be extended without changing chunking semantics?
- What evidence is needed before adding a hardware-accelerated checksum backend?
- Why must caller-supplied log fields be bounded and forbidden from shadowing built-in keys?
- What does flushing a structured log line guarantee, and what durability does it not guarantee?
- Why must elapsed-time decisions use a monotonic source instead of wall time?
- What lifetime and synchronization obligations accompany an injected `TimeSource`?
- Why is OS entropy acquisition separate from durable identity-domain and collision policy?
- Why does the UUID generator bound nil retries instead of promising global uniqueness?
- Why must a partial entropy read advance the destination before an interrupted retry?
