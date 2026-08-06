#include "chronos/query/literal.hpp"

#include "chronos/common/status.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <locale>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status out_of_range(const std::string_view message) {
  return common::Status{common::StatusCode::kOutOfRange, std::string{message}};
}

[[nodiscard]] bool ascii_digits(const std::string_view text) noexcept {
  for (const char value : text) {
    if (value < '0' || value > '9')
      return false;
  }
  return !text.empty();
}

template <typename Value>
[[nodiscard]] bool parse_decimal(const std::string_view text, Value& output) noexcept {
  if (!ascii_digits(text))
    return false;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), output, 10);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

[[nodiscard]] constexpr bool leap_year(const std::int32_t year) noexcept {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

[[nodiscard]] constexpr std::uint8_t days_in_month(const std::int32_t year,
                                                   const std::uint8_t month) noexcept {
  constexpr std::array<std::uint8_t, 12> kDays{31U, 28U, 31U, 30U, 31U, 30U,
                                               31U, 31U, 30U, 31U, 30U, 31U};
  return month == 2U && leap_year(year) ? 29U : kDays[month - 1U];
}

struct DateParts {
  std::int32_t year{};
  std::uint8_t month{};
  std::uint8_t day{};
};

// Proleptic Gregorian days relative to 1970-01-01. The accepted year range keeps all arithmetic
// well inside int64_t; this is the civil-date algorithm described by Howard Hinnant.
[[nodiscard]] constexpr std::int64_t days_from_civil(const DateParts& date) noexcept {
  std::int64_t year = date.year;
  const std::uint8_t month = date.month;
  year -= month <= 2U ? 1 : 0;
  const std::int64_t era = (year >= 0 ? year : year - 399) / 400;
  const std::uint32_t year_of_era = static_cast<std::uint32_t>(year - era * 400);
  const std::uint32_t adjusted_month =
      month > 2U ? static_cast<std::uint32_t>(month - 3U) : static_cast<std::uint32_t>(month + 9U);
  const std::uint32_t day_of_year =
      (153U * adjusted_month + 2U) / 5U + static_cast<std::uint32_t>(date.day) - 1U;
  const std::uint32_t day_of_era =
      year_of_era * 365U + year_of_era / 4U - year_of_era / 100U + day_of_year;
  return era * 146'097 + static_cast<std::int64_t>(day_of_era) - 719'468;
}

[[nodiscard]] common::Result<DateParts> parse_date_parts(const std::string_view text) {
  if (text.size() != 10U || text[4] != '-' || text[7] != '-') {
    return common::make_unexpected(invalid("DATE literal must use YYYY-MM-DD"));
  }
  std::int32_t year = 0;
  std::uint16_t month = 0U;
  std::uint16_t day = 0U;
  if (!parse_decimal(text.substr(0U, 4U), year) || !parse_decimal(text.substr(5U, 2U), month) ||
      !parse_decimal(text.substr(8U, 2U), day) || year == 0 || month == 0U || month > 12U ||
      day == 0U || day > days_in_month(year, static_cast<std::uint8_t>(month))) {
    return common::make_unexpected(invalid("DATE literal contains an invalid calendar date"));
  }
  return DateParts{.year = year,
                   .month = static_cast<std::uint8_t>(month),
                   .day = static_cast<std::uint8_t>(day)};
}

[[nodiscard]] std::optional<std::int64_t> interval_multiplier(const std::string_view unit) {
  if (unit == "nanosecond" || unit == "nanoseconds")
    return 1LL;
  if (unit == "microsecond" || unit == "microseconds")
    return 1'000LL;
  if (unit == "millisecond" || unit == "milliseconds")
    return 1'000'000LL;
  if (unit == "second" || unit == "seconds")
    return 1'000'000'000LL;
  if (unit == "minute" || unit == "minutes")
    return 60LL * 1'000'000'000LL;
  if (unit == "hour" || unit == "hours")
    return 3'600LL * 1'000'000'000LL;
  if (unit == "day" || unit == "days")
    return 86'400LL * 1'000'000'000LL;
  return std::nullopt;
}

[[nodiscard]] constexpr std::uint8_t hexadecimal(const char value) noexcept {
  if (value >= '0' && value <= '9')
    return static_cast<std::uint8_t>(value - '0');
  if (value >= 'a' && value <= 'f')
    return static_cast<std::uint8_t>(value - 'a' + 10);
  return std::numeric_limits<std::uint8_t>::max();
}

} // namespace

common::Result<std::int64_t> parse_sql_integer_literal(const std::string_view text) {
  std::int64_t value = 0;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value, 10);
  if (text.empty() || parsed.ec == std::errc::result_out_of_range) {
    return common::make_unexpected(out_of_range("SQL integer literal exceeds INT64"));
  }
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size()) {
    return common::make_unexpected(invalid("SQL integer literal is malformed"));
  }
  return value;
}

common::Result<double> parse_sql_float_literal(const std::string_view text) {
  double value = 0.0;
  std::istringstream parser{std::string{text}};
  parser.imbue(std::locale::classic());
  parser >> value;
  if (!std::isfinite(value)) {
    return common::make_unexpected(out_of_range("SQL floating literal exceeds FLOAT64"));
  }
  if (text.empty() || parser.fail() || parser.peek() != std::char_traits<char>::eof()) {
    return common::make_unexpected(invalid("SQL floating literal is malformed"));
  }
  return value;
}

common::Result<std::int32_t> parse_sql_date_literal(const std::string_view text) {
  const common::Result<DateParts> parts = parse_date_parts(text);
  if (!parts.has_value())
    return common::make_unexpected(parts.error());
  const std::int64_t days = days_from_civil(*parts);
  if (days < std::numeric_limits<std::int32_t>::min() ||
      days > std::numeric_limits<std::int32_t>::max()) {
    return common::make_unexpected(out_of_range("DATE literal exceeds signed day range"));
  }
  return static_cast<std::int32_t>(days);
}

common::Result<std::int64_t> parse_sql_timestamp_ns_literal(const std::string_view text) {
  if (text.size() < 20U || text[10] != ' ' || text[13] != ':' || text[16] != ':' ||
      text.back() != 'Z') {
    return common::make_unexpected(
        invalid("TIMESTAMP literal must use YYYY-MM-DD HH:MM:SS[.fffffffff]Z"));
  }
  const common::Result<DateParts> date = parse_date_parts(text.substr(0U, 10U));
  if (!date.has_value())
    return common::make_unexpected(date.error());
  std::uint16_t hour = 0U;
  std::uint16_t minute = 0U;
  std::uint16_t second = 0U;
  if (!parse_decimal(text.substr(11U, 2U), hour) || !parse_decimal(text.substr(14U, 2U), minute) ||
      !parse_decimal(text.substr(17U, 2U), second) || hour > 23U || minute > 59U || second > 59U) {
    return common::make_unexpected(invalid("TIMESTAMP literal contains an invalid UTC time"));
  }
  std::int64_t fractional_ns = 0;
  if (text.size() != 20U) {
    if (text[19] != '.' || text.size() < 22U || text.size() > 30U) {
      return common::make_unexpected(
          invalid("TIMESTAMP fractional seconds require one to nine digits"));
    }
    const std::string_view fraction = text.substr(20U, text.size() - 21U);
    if (!parse_decimal(fraction, fractional_ns)) {
      return common::make_unexpected(invalid("TIMESTAMP fraction is malformed"));
    }
    for (std::size_t digit = fraction.size(); digit < 9U; ++digit)
      fractional_ns *= 10;
  }

  const std::int64_t days = days_from_civil(*date);
  const std::int64_t seconds_from_days = days * 86'400;
  const std::int64_t seconds_of_day =
      static_cast<std::int64_t>(hour) * 3'600 + static_cast<std::int64_t>(minute) * 60 + second;
  const std::int64_t seconds_since_epoch = seconds_from_days + seconds_of_day;
  constexpr std::int64_t kNanosecondsPerSecond = 1'000'000'000;
  constexpr std::int64_t kMaximumWholeSeconds =
      std::numeric_limits<std::int64_t>::max() / kNanosecondsPerSecond;
  constexpr std::int64_t kMaximumFractionalNanoseconds =
      std::numeric_limits<std::int64_t>::max() % kNanosecondsPerSecond;
  constexpr std::int64_t kMinimumFloorSeconds =
      std::numeric_limits<std::int64_t>::min() / kNanosecondsPerSecond - 1;
  constexpr std::int64_t kMinimumFractionalNanoseconds =
      kNanosecondsPerSecond + std::numeric_limits<std::int64_t>::min() % kNanosecondsPerSecond;
  if (seconds_since_epoch > kMaximumWholeSeconds ||
      (seconds_since_epoch == kMaximumWholeSeconds &&
       fractional_ns > kMaximumFractionalNanoseconds) ||
      seconds_since_epoch < kMinimumFloorSeconds ||
      (seconds_since_epoch == kMinimumFloorSeconds &&
       fractional_ns < kMinimumFractionalNanoseconds)) {
    return common::make_unexpected(out_of_range("TIMESTAMP literal exceeds INT64 nanoseconds"));
  }
  if (seconds_since_epoch == kMinimumFloorSeconds) {
    return std::numeric_limits<std::int64_t>::min() +
           (fractional_ns - kMinimumFractionalNanoseconds);
  }
  const std::int64_t whole_ns = seconds_since_epoch * kNanosecondsPerSecond;
  return whole_ns + fractional_ns;
}

common::Result<std::int64_t> parse_sql_interval_ns_literal(const std::string_view text) {
  const std::size_t separator = text.find(' ');
  if (separator == std::string_view::npos || separator == 0U || separator + 1U >= text.size() ||
      text.find(' ', separator + 1U) != std::string_view::npos) {
    return common::make_unexpected(
        invalid("INTERVAL literal must contain one nonnegative integer and one unit"));
  }
  std::uint64_t quantity = 0U;
  if (!parse_decimal(text.substr(0U, separator), quantity)) {
    return common::make_unexpected(invalid("INTERVAL quantity is malformed"));
  }
  const std::optional<std::int64_t> multiplier = interval_multiplier(text.substr(separator + 1U));
  if (!multiplier.has_value()) {
    return common::make_unexpected(invalid("INTERVAL unit is not supported in SQL v1"));
  }
  if (quantity > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) /
                     static_cast<std::uint64_t>(*multiplier)) {
    return common::make_unexpected(out_of_range("INTERVAL literal exceeds INT64 nanoseconds"));
  }
  return static_cast<std::int64_t>(quantity) * *multiplier;
}

common::Result<common::Uuid> parse_sql_uuid_literal(const std::string_view text) {
  if (text.size() != 36U || text[8] != '-' || text[13] != '-' || text[18] != '-' ||
      text[23] != '-') {
    return common::make_unexpected(invalid("UUID literal is not canonical"));
  }
  common::Uuid::Bytes bytes{};
  std::size_t byte_index = 0U;
  for (std::size_t index = 0U; index < text.size();) {
    if (index == 8U || index == 13U || index == 18U || index == 23U) {
      ++index;
      continue;
    }
    const std::uint8_t high = hexadecimal(text[index]);
    const std::uint8_t low = hexadecimal(text[index + 1U]);
    if (high == std::numeric_limits<std::uint8_t>::max() ||
        low == std::numeric_limits<std::uint8_t>::max()) {
      return common::make_unexpected(
          invalid("UUID literal is not canonical lowercase hexadecimal"));
    }
    bytes[byte_index++] = static_cast<std::byte>((high << 4U) | low);
    index += 2U;
  }
  return common::Uuid{bytes};
}

} // namespace chronos::query
