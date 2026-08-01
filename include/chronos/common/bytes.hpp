#ifndef CHRONOS_COMMON_BYTES_HPP_
#define CHRONOS_COMMON_BYTES_HPP_

#include <cstddef>
#include <cstdint>
#include <span>

namespace chronos::common {

// These views never own storage. The caller must keep the referenced buffer alive and, for a
// MutableByteView, must prevent conflicting access for the complete lifetime of the view.
using ByteView = std::span<const std::byte>;
using MutableByteView = std::span<std::byte>;

// These overloads intentionally accept only explicitly byte-oriented uint8_t buffers. They do not
// expose arbitrary C++ object representations as durable bytes.
[[nodiscard]] inline ByteView byte_view(const std::span<const std::uint8_t> bytes) noexcept {
  return std::as_bytes(bytes);
}

[[nodiscard]] inline MutableByteView
mutable_byte_view(const std::span<std::uint8_t> bytes) noexcept {
  return std::as_writable_bytes(bytes);
}

} // namespace chronos::common

#endif // CHRONOS_COMMON_BYTES_HPP_
