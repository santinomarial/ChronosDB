#include "chronos/network/protocol.hpp"
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

} // namespace
} // namespace chronos::network
