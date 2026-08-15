#include "chronos/network/spsc_queue.hpp"

#include <atomic>
#include <cstddef>
#include <gtest/gtest.h>
#include <thread>

namespace chronos::network {
namespace {

[[nodiscard]] NetworkTask task(const std::uint64_t id) {
  return {.connection_id = id,
          .protocol = {.protocol_major = kProtocolV2Major,
                       .protocol_minor = kProtocolV2LatestMinor,
                       .feature_bits = std::uint64_t{1U} << 9U,
                       .maximum_payload_size = 4096U},
          .frame = {.header = {.request_id = id}, .payload = {}}};
}

TEST(SpscNetworkTaskQueueTest, PreservesFifoAndMakesSaturationExplicit) {
  SpscNetworkTaskQueue queue = SpscNetworkTaskQueue::create(2U).value();
  EXPECT_TRUE(queue.try_push(task(1U)));
  EXPECT_TRUE(queue.try_push(task(2U)));
  EXPECT_FALSE(queue.try_push(task(3U)));
  auto value = queue.try_pop();
  if (!value.has_value()) {
    ADD_FAILURE() << "expected the first queued task";
    return;
  }
  EXPECT_EQ(value->connection_id, 1U);
  EXPECT_EQ(value->protocol.protocol_major, kProtocolV2Major);
  EXPECT_EQ(value->protocol.feature_bits, std::uint64_t{1U} << 9U);
  EXPECT_EQ(value->protocol.maximum_payload_size, 4096U);
  EXPECT_TRUE(queue.try_push(task(3U)));
  value = queue.try_pop();
  if (!value.has_value()) {
    ADD_FAILURE() << "expected the second queued task";
    return;
  }
  EXPECT_EQ(value->connection_id, 2U);
  value = queue.try_pop();
  if (!value.has_value()) {
    ADD_FAILURE() << "expected the wrapped queued task";
    return;
  }
  EXPECT_EQ(value->connection_id, 3U);
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
  if (!delivered.has_value()) {
    ADD_FAILURE() << "expected the preserved task";
    return;
  }
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
