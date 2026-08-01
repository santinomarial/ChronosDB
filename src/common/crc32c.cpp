#include "chronos/common/crc32c.hpp"

#include <array>
#include <cstddef>

namespace chronos::common {
namespace {

constexpr std::uint32_t kReversedCastagnoliPolynomial = 0x82f63b78U;

[[nodiscard]] constexpr std::array<std::uint32_t, 256> make_crc32c_table() {
  std::array<std::uint32_t, 256> table{};
  for (std::uint32_t index = 0; index < table.size(); ++index) {
    std::uint32_t remainder = index;
    for (std::uint32_t bit = 0; bit < 8U; ++bit) {
      const std::uint32_t mask = static_cast<std::uint32_t>(0U - (remainder & 1U));
      remainder = (remainder >> 1U) ^ (kReversedCastagnoliPolynomial & mask);
    }
    table[index] = remainder;
  }
  return table;
}

inline constexpr auto kCrc32cTable = make_crc32c_table();

[[nodiscard]] std::uint32_t extend_state(std::uint32_t state, const ByteView bytes) noexcept {
  for (const std::byte byte : bytes) {
    const auto index = static_cast<std::uint8_t>(state ^ std::to_integer<std::uint8_t>(byte));
    state = kCrc32cTable[index] ^ (state >> 8U);
  }
  return state;
}

} // namespace

void Crc32c::extend(const ByteView bytes) noexcept {
  state_ = extend_state(state_, bytes);
}

void Crc32c::reset() noexcept {
  state_ = 0xffffffffU;
}

std::uint32_t Crc32c::value() const noexcept {
  return state_ ^ 0xffffffffU;
}

std::uint32_t crc32c(const ByteView bytes) noexcept {
  Crc32c checksum;
  checksum.extend(bytes);
  return checksum.value();
}

std::uint32_t extend_crc32c(const std::uint32_t checksum, const ByteView bytes) noexcept {
  const std::uint32_t state = checksum ^ 0xffffffffU;
  return extend_state(state, bytes) ^ 0xffffffffU;
}

} // namespace chronos::common
