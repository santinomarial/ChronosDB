#include "chronos/schema/utf8.hpp"

#include <cstddef>
#include <cstdint>
#include <span>

namespace chronos::schema {
namespace {

[[nodiscard]] constexpr std::uint8_t byte_at(const common::ByteView value,
                                             const std::size_t index) noexcept {
  return std::to_integer<std::uint8_t>(value[index]);
}

[[nodiscard]] constexpr bool is_continuation(const std::uint8_t value) noexcept {
  return value >= 0x80U && value <= 0xbfU;
}

} // namespace

bool is_valid_utf8(const common::ByteView value) noexcept {
  std::size_t index = 0;
  while (index < value.size()) {
    const std::uint8_t first = byte_at(value, index);
    if (first <= 0x7fU) {
      ++index;
      continue;
    }

    if (first >= 0xc2U && first <= 0xdfU) {
      if (index + 1U >= value.size() || !is_continuation(byte_at(value, index + 1U))) {
        return false;
      }
      index += 2U;
      continue;
    }

    if (first >= 0xe0U && first <= 0xefU) {
      if (index + 2U >= value.size()) {
        return false;
      }
      const std::uint8_t second = byte_at(value, index + 1U);
      const std::uint8_t third = byte_at(value, index + 2U);
      const bool valid_second = is_continuation(second) && (first != 0xe0U || second >= 0xa0U) &&
                                (first != 0xedU || second <= 0x9fU);
      if (!valid_second || !is_continuation(third)) {
        return false;
      }
      index += 3U;
      continue;
    }

    if (first >= 0xf0U && first <= 0xf4U) {
      if (index + 3U >= value.size()) {
        return false;
      }
      const std::uint8_t second = byte_at(value, index + 1U);
      const std::uint8_t third = byte_at(value, index + 2U);
      const std::uint8_t fourth = byte_at(value, index + 3U);
      const bool valid_second = is_continuation(second) && (first != 0xf0U || second >= 0x90U) &&
                                (first != 0xf4U || second <= 0x8fU);
      if (!valid_second || !is_continuation(third) || !is_continuation(fourth)) {
        return false;
      }
      index += 4U;
      continue;
    }

    return false;
  }
  return true;
}

bool is_valid_utf8(const std::string_view value) noexcept {
  return is_valid_utf8(std::as_bytes(std::span{value.data(), value.size()}));
}

} // namespace chronos::schema
