#include "chronos/network/connection_buffers.hpp"
#include "chronos/network/connection_state.hpp"
#include "chronos/network/epoll_reactor.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>

namespace chronos::network {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

TEST(ProtocolAllocationFailureTest, EncodeClassifiesEveryOwnedAllocation) {
  const std::array<std::byte, 64> payload{};
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_frame({.message_type = MessageType::kQueryResult, .request_id = 9U}, payload);
    });
    if (result.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(ProtocolAllocationFailureTest, DecodeClassifiesEveryOwnedAllocation) {
  const std::array<std::byte, 64> payload{};
  const std::vector<std::byte> encoded =
      *encode_frame({.message_type = MessageType::kQueryResult, .request_id = 9U}, payload);
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result = run_failure(fail_after, [&] { return decode_frame(encoded); });
    if (result.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

template <typename Operation> void expect_owned_allocation_is_classified(Operation&& operation) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    auto result = run_failure(fail_after, operation);
    if (result.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(ProtocolAllocationFailureTest, MessageEncodersClassifyEveryOwnedAllocation) {
  const std::array<std::byte, 8> body{};
  expect_owned_allocation_is_classified([&] { return encode_client_hello({}); });
  expect_owned_allocation_is_classified([&] { return encode_server_hello({}); });
  expect_owned_allocation_is_classified(
      [&] { return encode_ingest_request(DurabilityMode::kAsync, body); });
  expect_owned_allocation_is_classified(
      [&] { return encode_ingest_acknowledgement({.outcome = IngestOutcome::kMatchingRetry}); });
  expect_owned_allocation_is_classified([&] { return encode_query_request("SELECT 1"); });
  expect_owned_allocation_is_classified(
      [&] { return encode_error_message(ProtocolErrorCode::kOverloaded, "full"); });
}

TEST(ProtocolAllocationFailureTest, ConnectionCreationClassifiesItsOwnedRequestStorage) {
  expect_owned_allocation_is_classified([&] { return ServerConnectionState::create(); });
}

TEST(ProtocolAllocationFailureTest, ConnectionBuffersClassifyCreationAndReceiveAllocations) {
  expect_owned_allocation_is_classified([&] { return ConnectionBuffers::create(); });
  const std::vector<std::byte> encoded =
      *encode_frame({.message_type = MessageType::kQueryRequest, .request_id = 1U},
                    *encode_query_request("SELECT 1"));
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    ConnectionBuffers buffers = ConnectionBuffers::create().value();
    auto result = run_failure(fail_after, [&] { return buffers.receive(encoded); });
    if (result.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(ProtocolAllocationFailureTest, SpscQueueClassifiesItsSingleOwnedAllocation) {
  expect_owned_allocation_is_classified([&] { return SpscNetworkTaskQueue::create(64U); });
}

#if defined(__linux__)
TEST(ProtocolAllocationFailureTest, EpollReactorClassifiesEveryStartupAllocation) {
  SpscNetworkTaskQueue requests = SpscNetworkTaskQueue::create(4U).value();
  SpscNetworkTaskQueue responses = SpscNetworkTaskQueue::create(4U).value();
  EpollServerConfig config;
  config.maximum_connections = 4U;
  config.maximum_events_per_poll = 4U;
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return EpollReactor::start(config, {.requests = &requests, .responses = &responses});
    });
    if (result.has_value()) {
      reached_success = true;
      EXPECT_TRUE(result->shutdown().is_ok());
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}
#endif

} // namespace
} // namespace chronos::network
