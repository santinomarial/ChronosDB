#ifndef CHRONOS_COMMON_CRC32C_HPP_
#define CHRONOS_COMMON_CRC32C_HPP_

#include "chronos/common/bytes.hpp"

#include <cstdint>

namespace chronos::common {

// CRC32C uses the Castagnoli polynomial. The internal state starts at all ones and the returned
// checksum is XORed with all ones. Returned values are host integers, not serialized byte strings.
class Crc32c {
public:
  Crc32c() noexcept = default;

  void extend(ByteView bytes) noexcept;
  void reset() noexcept;
  [[nodiscard]] std::uint32_t value() const noexcept;

private:
  std::uint32_t state_{0xffffffffU};
};

[[nodiscard]] std::uint32_t crc32c(ByteView bytes) noexcept;

// Extends an already finalized checksum. Passing crc32c({}) starts a new stream. This operation
// preserves the same initial/final XOR semantics as Crc32c and supports arbitrary chunking.
[[nodiscard]] std::uint32_t extend_crc32c(std::uint32_t checksum, ByteView bytes) noexcept;

} // namespace chronos::common

#endif // CHRONOS_COMMON_CRC32C_HPP_
