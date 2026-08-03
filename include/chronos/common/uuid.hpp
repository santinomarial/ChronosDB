#ifndef CHRONOS_COMMON_UUID_HPP_
#define CHRONOS_COMMON_UUID_HPP_

#include <array>
#include <compare>
#include <cstddef>

namespace chronos::common {

// A UUID is an uninterpreted 128-bit value. The array order is the durable/network UUID byte order;
// callers must not serialize a native integer or object representation in its place.
class Uuid {
public:
  static constexpr std::size_t kSize = 16;
  using Bytes = std::array<std::byte, kSize>;

  constexpr Uuid() noexcept = default;
  explicit constexpr Uuid(Bytes bytes) noexcept : bytes_(bytes) {}

  [[nodiscard]] constexpr const Bytes& bytes() const noexcept { return bytes_; }

  [[nodiscard]] constexpr bool is_nil() const noexcept {
    for (const std::byte value : bytes_) {
      if (value != std::byte{0}) {
        return false;
      }
    }
    return true;
  }

  friend constexpr auto operator<=>(const Uuid&, const Uuid&) = default;

private:
  Bytes bytes_{};
};

} // namespace chronos::common

#endif // CHRONOS_COMMON_UUID_HPP_
