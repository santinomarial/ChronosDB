#include "chronos/common/checked_math.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <optional>

namespace chronos::common {
namespace {

static_assert(checked_add<std::uint32_t>(2U, 3U) == std::optional<std::uint32_t>{5U});
static_assert(!checked_add<std::uint8_t>(255U, 1U).has_value());
static_assert(checked_multiply<std::uint32_t>(6U, 7U) == std::optional<std::uint32_t>{42U});
static_assert(!checked_multiply<std::uint16_t>(65535U, 2U).has_value());

TEST(CheckedMathTest, AdditionCoversZeroLimitsAndOverflow) {
  constexpr auto kMaximum = std::numeric_limits<std::uint64_t>::max();
  EXPECT_EQ(checked_add<std::uint64_t>(0, 0), std::optional<std::uint64_t>{0});
  EXPECT_EQ(checked_add<std::uint64_t>(kMaximum, 0), std::optional<std::uint64_t>{kMaximum});
  EXPECT_EQ(checked_add<std::uint64_t>(kMaximum - 1U, 1U), std::optional<std::uint64_t>{kMaximum});
  EXPECT_FALSE(checked_add<std::uint64_t>(kMaximum, 1U).has_value());
  EXPECT_FALSE(checked_range_end<std::uint64_t>(kMaximum - 4U, 5U).has_value());
}

TEST(CheckedMathTest, MultiplicationCoversZeroLimitsAndOverflow) {
  constexpr auto kMaximum = std::numeric_limits<std::size_t>::max();
  EXPECT_EQ(checked_multiply<std::size_t>(0, kMaximum), std::optional<std::size_t>{0});
  EXPECT_EQ(checked_multiply<std::size_t>(1, kMaximum), std::optional<std::size_t>{kMaximum});
  EXPECT_EQ(checked_multiply<std::size_t>(kMaximum / 2U, 2U),
            std::optional<std::size_t>{kMaximum - 1U});
  EXPECT_FALSE(checked_multiply<std::size_t>((kMaximum / 2U) + 1U, 2U).has_value());
}

TEST(CheckedMathTest, AlignmentRejectsInvalidValues) {
  const Result<std::uint64_t> zero = checked_align_up<std::uint64_t>(8U, 0U);
  ASSERT_FALSE(zero.has_value());
  EXPECT_EQ(zero.error().code(), StatusCode::kInvalidArgument);

  const Result<std::uint64_t> non_power_of_two = checked_align_up<std::uint64_t>(8U, 3U);
  ASSERT_FALSE(non_power_of_two.has_value());
  EXPECT_EQ(non_power_of_two.error().code(), StatusCode::kInvalidArgument);
}

TEST(CheckedMathTest, AlignmentHandlesOrdinaryAndBoundaryValues) {
  EXPECT_EQ(checked_align_up<std::uint64_t>(0U, 8U), Result<std::uint64_t>{0U});
  EXPECT_EQ(checked_align_up<std::uint64_t>(16U, 8U), Result<std::uint64_t>{16U});
  EXPECT_EQ(checked_align_up<std::uint64_t>(17U, 8U), Result<std::uint64_t>{24U});

  constexpr auto kMaximum = std::numeric_limits<std::uint64_t>::max();
  const Result<std::uint64_t> overflow = checked_align_up<std::uint64_t>(kMaximum, 8U);
  ASSERT_FALSE(overflow.has_value());
  EXPECT_EQ(overflow.error().code(), StatusCode::kOutOfRange);

  const Result<std::uint64_t> near_limit = checked_align_up<std::uint64_t>(kMaximum - 7U, 8U);
  ASSERT_TRUE(near_limit.has_value());
  EXPECT_EQ(*near_limit, kMaximum - 7U);
}

} // namespace
} // namespace chronos::common
