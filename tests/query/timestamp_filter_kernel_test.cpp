#include "query/timestamp_filter_kernel.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <numeric>
#include <vector>

namespace chronos::query::detail {
namespace {

[[nodiscard]] std::vector<std::byte>
encode_timestamps(const std::vector<std::int64_t>& timestamps) {
  std::vector<std::byte> bytes(timestamps.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < timestamps.size(); ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(timestamps[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte)
      bytes[row * sizeof(bits) + byte] = static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
  }
  return bytes;
}

[[nodiscard]] std::vector<std::uint32_t> compact(const std::vector<std::int64_t>& timestamps,
                                                 const TimestampRangePredicate& predicate,
                                                 const TimestampFilterKernel kernel) {
  const std::vector<std::byte> bytes = encode_timestamps(timestamps);
  std::vector<std::uint32_t> indices(timestamps.size());
  std::iota(indices.begin(), indices.end(), 0U);
  indices.resize(compact_identity_timestamps(bytes, indices, predicate, kernel));
  return indices;
}

TEST(TimestampFilterKernelTest, EveryAvailableKernelMatchesExactScalarBounds) {
  constexpr std::array<std::size_t, 13U> sizes{1U, 2U, 3U,  4U,  5U,  6U,  7U,
                                               8U, 9U, 15U, 16U, 17U, 257U};
  const std::array<TimestampRangePredicate, 9U> predicates{
      TimestampRangePredicate{},
      TimestampRangePredicate{.lower = TimestampRangeBound{-10, true}, .upper = std::nullopt},
      TimestampRangePredicate{.lower = TimestampRangeBound{-10, false}, .upper = std::nullopt},
      TimestampRangePredicate{.lower = std::nullopt, .upper = TimestampRangeBound{10, true}},
      TimestampRangePredicate{.lower = std::nullopt, .upper = TimestampRangeBound{10, false}},
      TimestampRangePredicate{.lower = TimestampRangeBound{-20, true},
                              .upper = TimestampRangeBound{20, false}},
      TimestampRangePredicate{.lower = TimestampRangeBound{7, true},
                              .upper = TimestampRangeBound{7, true}},
      TimestampRangePredicate{.lower = TimestampRangeBound{7, false},
                              .upper = TimestampRangeBound{7, true}},
      TimestampRangePredicate{.lower = TimestampRangeBound{20, true},
                              .upper = TimestampRangeBound{-20, true}}};
  constexpr std::array<TimestampFilterKernel, 3U> kernels{
      TimestampFilterKernel::kScalar, TimestampFilterKernel::kAvx2, TimestampFilterKernel::kNeon};

  for (const std::size_t size : sizes) {
    std::vector<std::int64_t> timestamps(size);
    for (std::size_t row = 0U; row < size; ++row)
      timestamps[row] = static_cast<std::int64_t>((row * 37U + size * 11U) % 101U) - 50;
    timestamps.front() = std::numeric_limits<std::int64_t>::min();
    timestamps.back() = std::numeric_limits<std::int64_t>::max();
    for (const TimestampRangePredicate& predicate : predicates) {
      const std::vector<std::uint32_t> expected =
          compact(timestamps, predicate, TimestampFilterKernel::kScalar);
      EXPECT_EQ(compact(timestamps, predicate, preferred_timestamp_filter_kernel()), expected);
      for (const TimestampFilterKernel kernel : kernels) {
        if (timestamp_filter_kernel_available(kernel)) {
          EXPECT_EQ(compact(timestamps, predicate, kernel), expected);
        }
      }
    }
  }
}

TEST(TimestampFilterKernelTest, UnavailableForcedKernelFallsBackToScalar) {
  const std::vector<std::int64_t> timestamps{-1, 0, 1};
  const TimestampRangePredicate predicate{.lower = TimestampRangeBound{0, true},
                                          .upper = std::nullopt};
  const std::vector<std::uint32_t> expected{1U, 2U};
  for (const TimestampFilterKernel kernel :
       {TimestampFilterKernel::kAvx2, TimestampFilterKernel::kNeon}) {
    if (!timestamp_filter_kernel_available(kernel)) {
      EXPECT_EQ(compact(timestamps, predicate, kernel), expected);
    }
  }
}

} // namespace
} // namespace chronos::query::detail
