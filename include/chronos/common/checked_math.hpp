#ifndef CHRONOS_COMMON_CHECKED_MATH_HPP_
#define CHRONOS_COMMON_CHECKED_MATH_HPP_

#include "chronos/common/result.hpp"

#include <concepts>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>

namespace chronos::common {

template <typename T>
concept UnsignedInteger =
    std::unsigned_integral<T> && (!std::same_as<std::remove_cv_t<T>, bool>);

template <UnsignedInteger T> [[nodiscard]] constexpr std::optional<T> checked_add(T left, T right) {
  if (right > std::numeric_limits<T>::max() - left) {
    return std::nullopt;
  }
  return static_cast<T>(left + right);
}

template <UnsignedInteger T>
[[nodiscard]] constexpr std::optional<T> checked_multiply(T left, T right) {
  if (left != 0 && right > std::numeric_limits<T>::max() / left) {
    return std::nullopt;
  }
  return static_cast<T>(left * right);
}

template <UnsignedInteger T>
[[nodiscard]] constexpr std::optional<T> checked_range_end(T offset, T length) {
  return checked_add(offset, length);
}

template <UnsignedInteger T> [[nodiscard]] Result<T> checked_align_up(T value, T alignment) {
  if (alignment == 0 || (alignment & static_cast<T>(alignment - 1)) != 0) {
    return make_unexpected(
        Status{StatusCode::kInvalidArgument, "alignment must be a nonzero power of two"});
  }

  const T mask = static_cast<T>(alignment - 1);
  const std::optional<T> adjusted = checked_add(value, mask);
  if (!adjusted.has_value()) {
    return make_unexpected(Status{StatusCode::kOutOfRange, "alignment calculation overflowed"});
  }
  return static_cast<T>(*adjusted & static_cast<T>(~mask));
}

} // namespace chronos::common

#endif // CHRONOS_COMMON_CHECKED_MATH_HPP_
