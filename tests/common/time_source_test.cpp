#include "chronos/common/time_source.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

namespace chronos::common {
namespace {

class ManualTimeSource final : public TimeSource {
public:
  [[nodiscard]] WallTimePoint wall_now() const noexcept override {
    return WallTimePoint{WallTimePoint::duration{wall_ticks.load(std::memory_order_acquire)}};
  }

  [[nodiscard]] MonotonicTimePoint monotonic_now() const noexcept override {
    return MonotonicTimePoint{
        MonotonicTimePoint::duration{monotonic_ticks.load(std::memory_order_acquire)}};
  }

  std::atomic<std::int64_t> wall_ticks;
  std::atomic<std::int64_t> monotonic_ticks;
};

TEST(TimeSourceTest, InjectedSourceKeepsWallAndMonotonicDomainsDistinct) {
  ManualTimeSource source;
  source.wall_ticks.store(-7, std::memory_order_release);
  source.monotonic_ticks.store(19, std::memory_order_release);

  EXPECT_EQ(source.wall_now().time_since_epoch(), WallTimePoint::duration{-7});
  EXPECT_EQ(source.monotonic_now().time_since_epoch(), MonotonicTimePoint::duration{19});
}

TEST(TimeSourceTest, SystemSourceHasStableProcessLifetimeAndAdvancingMonotonicTime) {
  const SystemTimeSource& first = system_time_source();
  const SystemTimeSource& second = system_time_source();
  EXPECT_EQ(&first, &second);
  EXPECT_NE(first.wall_now().time_since_epoch(), WallTimePoint::duration::zero());
  const MonotonicTimePoint before = first.monotonic_now();
  const MonotonicTimePoint after = first.monotonic_now();
  EXPECT_GE(after, before);
}

TEST(TimeSourceTest, SystemSourceSupportsConcurrentMonotonicReads) {
  constexpr std::size_t kThreadCount = 4U;
  constexpr std::size_t kReadsPerThread = 1'024U;
  std::atomic<bool> regressed{false};
  std::vector<std::thread> readers;
  readers.reserve(kThreadCount);
  for (std::size_t thread = 0U; thread < kThreadCount; ++thread) {
    readers.emplace_back([&regressed] {
      const TimeSource& source = system_time_source();
      MonotonicTimePoint prior = source.monotonic_now();
      for (std::size_t read = 0U; read < kReadsPerThread; ++read) {
        const MonotonicTimePoint current = source.monotonic_now();
        if (current < prior)
          regressed.store(true, std::memory_order_release);
        prior = current;
      }
    });
  }
  for (std::thread& reader : readers)
    reader.join();
  EXPECT_FALSE(regressed.load(std::memory_order_acquire));
}

} // namespace
} // namespace chronos::common
