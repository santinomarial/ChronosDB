#include "chronos/common/status.hpp"
#include "chronos/query/literal.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>

namespace chronos::query {
namespace {

TEST(SqlLiteralTest, ParsesExactUtcNanosecondsAndCalendarBoundaries) {
  EXPECT_EQ(parse_sql_timestamp_ns_literal("1970-01-01 00:00:00Z").value(), 0);
  EXPECT_EQ(parse_sql_timestamp_ns_literal("1970-01-01 00:00:00.000000001Z").value(), 1);
  EXPECT_EQ(parse_sql_timestamp_ns_literal("1969-12-31 23:59:59.999999999Z").value(), -1);
  EXPECT_EQ(parse_sql_timestamp_ns_literal("2000-02-29 12:34:56.123Z").value(),
            951'827'696'123'000'000LL);
  EXPECT_EQ(parse_sql_timestamp_ns_literal("1677-09-21 00:12:43.145224192Z").value(),
            std::numeric_limits<std::int64_t>::min());
  EXPECT_EQ(parse_sql_timestamp_ns_literal("1677-09-21 00:12:43.145224193Z").value(),
            std::numeric_limits<std::int64_t>::min() + 1);
  EXPECT_EQ(parse_sql_timestamp_ns_literal("2262-04-11 23:47:16.854775807Z").value(),
            std::numeric_limits<std::int64_t>::max());

  for (const char* invalid : {
           "2026-02-29 00:00:00Z",
           "2024-02-29T00:00:00Z",
           "2024-02-29 24:00:00Z",
           "2024-02-29 00:00:60Z",
           "2024-02-29 00:00:00.1234567890Z",
           "1677-09-21 00:12:43.145224191Z",
           "1677-09-21 00:12:42.999999999Z",
           "2262-04-11 23:47:16.854775808Z",
           "2024-02-29 00:00:00+00:00",
       }) {
    EXPECT_FALSE(parse_sql_timestamp_ns_literal(invalid).has_value()) << invalid;
  }
}

TEST(SqlLiteralTest, ParsesDatesIntervalsNumbersAndCanonicalUuids) {
  EXPECT_EQ(parse_sql_date_literal("1970-01-01").value(), 0);
  EXPECT_EQ(parse_sql_date_literal("1969-12-31").value(), -1);
  EXPECT_EQ(parse_sql_date_literal("2000-02-29").value(), 11'016);
  EXPECT_FALSE(parse_sql_date_literal("1900-02-29").has_value());

  EXPECT_EQ(parse_sql_interval_ns_literal("1 nanosecond").value(), 1);
  EXPECT_EQ(parse_sql_interval_ns_literal("2 minutes").value(), 120'000'000'000LL);
  EXPECT_EQ(parse_sql_interval_ns_literal("0 days").value(), 0);
  EXPECT_FALSE(parse_sql_interval_ns_literal("-1 second").has_value());
  EXPECT_FALSE(parse_sql_interval_ns_literal("1 month").has_value());
  EXPECT_FALSE(parse_sql_interval_ns_literal("9223372036854775807 days").has_value());

  EXPECT_EQ(parse_sql_integer_literal("9223372036854775807").value(),
            std::numeric_limits<std::int64_t>::max());
  EXPECT_EQ(parse_sql_integer_literal("9223372036854775808").error().code(),
            common::StatusCode::kOutOfRange);
  EXPECT_DOUBLE_EQ(parse_sql_float_literal("1.25e2").value(), 125.0);
  EXPECT_FALSE(parse_sql_float_literal("1e9999").has_value());

  const auto uuid = parse_sql_uuid_literal("00112233-4455-6677-8899-aabbccddeeff");
  ASSERT_TRUE(uuid.has_value());
  EXPECT_EQ(std::to_integer<std::uint8_t>(uuid->bytes().front()), 0x00U);
  EXPECT_EQ(std::to_integer<std::uint8_t>(uuid->bytes().back()), 0xffU);
  EXPECT_FALSE(parse_sql_uuid_literal("00112233-4455-6677-8899-AABBCCDDEEFF").has_value());
}

} // namespace
} // namespace chronos::query
