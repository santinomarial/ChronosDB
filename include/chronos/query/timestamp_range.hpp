#ifndef CHRONOS_QUERY_TIMESTAMP_RANGE_HPP_
#define CHRONOS_QUERY_TIMESTAMP_RANGE_HPP_

#include <cstdint>
#include <optional>

namespace chronos::query {

struct TimestampRangeBound {
  std::int64_t value{};
  bool inclusive{true};

  friend bool operator==(const TimestampRangeBound&, const TimestampRangeBound&) = default;
};

// A conjunction of optional lower and upper TIMESTAMP_NS bounds. Reversed bounds and an equal
// endpoint with either side open are valid empty predicates. Comparisons never adjust endpoints,
// so the complete signed 64-bit timestamp domain is safe.
struct TimestampRangePredicate {
  std::optional<TimestampRangeBound> lower;
  std::optional<TimestampRangeBound> upper;

  [[nodiscard]] constexpr bool matches(const std::int64_t timestamp_ns) const noexcept {
    if (lower.has_value() &&
        (timestamp_ns < lower->value || (timestamp_ns == lower->value && !lower->inclusive))) {
      return false;
    }
    return !upper.has_value() || timestamp_ns < upper->value ||
           (timestamp_ns == upper->value && upper->inclusive);
  }

  [[nodiscard]] constexpr bool is_empty() const noexcept {
    if (!lower.has_value() || !upper.has_value())
      return false;
    if (lower->value != upper->value)
      return lower->value > upper->value;
    return !lower->inclusive || !upper->inclusive;
  }

  friend bool operator==(const TimestampRangePredicate&, const TimestampRangePredicate&) = default;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_TIMESTAMP_RANGE_HPP_
