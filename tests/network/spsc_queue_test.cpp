#include "chronos/network/spsc_queue.hpp"

#include <atomic>
#include <cstddef>
#include <gtest/gtest.h>
#include <thread>

namespace chronos::network {
namespace {

[[nodiscard]] NetworkTask task(const std::uint64_t id) {
  return {.connection_id = id, .frame = {.header = {.request_id = id}, .payload = {}}};
}

TEST(SpscNetworkTaskQueueTest, PreservesFifoAndMakesSaturationExplicit) {
  SpscNetworkTaskQueue queue = SpscNetworkTaskQueue::create(2U).value();
  EXPECT_TRUE(queue.try_push(task(1U)));
  EXPECT_TRUE(queue.try_push(task(2U)));
  EXPECT_FALSE(queue.try_push(task(3U)));
  auto value = queue.try_pop();
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value->connection_id, 1U); // NOLINT(bugprone-unchecked-optional-access)
  EXPECT_TRUE(queue.try_push(task(3U)));
  value = queue.try_pop();
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value->connection_id, 2U); // NOLINT(bugprone-unchecked-optional-access)
  value = queue.try_pop();
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value->connection_id, 3U); // NOLINT(bugprone-unchecked-optional-access)
  EXPECT_FALSE(queue.try_pop().has_value());
}

TEST(SpscNetworkTaskQueueTest, PreservingPushDoesNotConsumeAFullQueueRetry) {
  SpscNetworkTaskQueue queue = SpscNetworkTaskQueue::create(1U).value();
  EXPECT_TRUE(queue.try_push(task(1U)));
  NetworkTask retained = task(2U);
  retained.frame.payload = {std::byte{7}, std::byte{8}};
  EXPECT_FALSE(queue.try_push_preserving(retained));
  EXPECT_EQ(retained.connection_id, 2U);
  EXPECT_EQ(retained.frame.payload.size(), 2U);
  ASSERT_TRUE(queue.try_pop().has_value());
  EXPECT_TRUE(queue.try_push_preserving(retained));
  const auto delivered = queue.try_pop();
  ASSERT_TRUE(delivered.has_value());
  EXPECT_EQ(delivered->connection_id, 2U);
  EXPECT_EQ(delivered->frame.payload.size(), 2U);
}

TEST(SpscNetworkTaskQueueTest, ReleaseAcquirePublishesCompleteOwnedFrames) {
  constexpr std::uint64_t kTasks = 100'000U;
  SpscNetworkTaskQueue queue = SpscNetworkTaskQueue::create(64U).value();
  std::atomic<bool> start{false};
  std::thread producer([&] {
    while (!start.load(std::memory_order_acquire)) {
    }
    for (std::uint64_t id = 1U; id <= kTasks; ++id)
      while (!queue.try_push(task(id)))
        std::this_thread::yield();
  });
  start.store(true, std::memory_order_release);
  for (std::uint64_t expected = 1U; expected <= kTasks; ++expected) {
    std::optional<NetworkTask> value;
    while (!(value = queue.try_pop()).has_value())
      std::this_thread::yield();
    ASSERT_EQ(value->connection_id, expected);
    ASSERT_EQ(value->frame.header.request_id, expected);
  }
  producer.join();
  EXPECT_TRUE(queue.empty());
}

} // namespace
} // namespace chronos::network
