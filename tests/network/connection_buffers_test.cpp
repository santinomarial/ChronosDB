#include "chronos/network/connection_buffers.hpp"
#include "chronos/network/messages.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::network {
namespace {

[[nodiscard]] std::vector<std::byte> query_frame(const std::uint64_t id) {
  const auto payload = encode_query_request("SELECT 1").value();
  return encode_frame({.message_type = MessageType::kQueryRequest, .request_id = id}, payload)
      .value();
}

TEST(ConnectionBuffersTest, ReassemblesEveryTwoPartFragmentationBoundary) {
  const std::vector<std::byte> encoded = query_frame(1U);
  for (std::size_t split = 0U; split <= encoded.size(); ++split) {
    ConnectionBuffers buffers = ConnectionBuffers::create().value();
    auto first = buffers.receive(common::ByteView{encoded}.first(split));
    ASSERT_TRUE(first.has_value()) << split;
    EXPECT_EQ(first->size(), split == encoded.size() ? 1U : 0U) << split;
    auto second = buffers.receive(common::ByteView{encoded}.subspan(split));
    ASSERT_TRUE(second.has_value()) << split;
    ASSERT_EQ(second->size(), split == encoded.size() ? 0U : 1U) << split;
    if (!second->empty()) {
      EXPECT_EQ(second->front().header.request_id, 1U);
    }
    EXPECT_EQ(buffers.inbound_buffer_bytes(), 0U);
  }
}

TEST(ConnectionBuffersTest, SplitsCoalescedFramesAndRetainsPartialSuffix) {
  std::vector<std::byte> bytes = query_frame(1U);
  const std::vector<std::byte> second = query_frame(2U);
  bytes.insert(bytes.end(), second.begin(), second.end());
  const std::vector<std::byte> third = query_frame(3U);
  bytes.insert(bytes.end(), third.begin(), third.begin() + 17);
  ConnectionBuffers buffers = ConnectionBuffers::create().value();
  auto decoded = buffers.receive(bytes);
  ASSERT_TRUE(decoded.has_value());
  ASSERT_EQ(decoded->size(), 2U);
  EXPECT_EQ(buffers.inbound_buffer_bytes(), 17U);
  decoded = buffers.receive(common::ByteView{third}.subspan(17U));
  ASSERT_TRUE(decoded.has_value());
  ASSERT_EQ(decoded->size(), 1U);
  EXPECT_EQ(decoded->front().header.request_id, 3U);
}

TEST(ConnectionBuffersTest, PartialWritesRetainImmutableFrameAndExactCredit) {
  const std::vector<std::byte> first = query_frame(1U);
  const std::vector<std::byte> second = query_frame(2U);
  ConnectionBuffers buffers = ConnectionBuffers::create().value();
  ASSERT_TRUE(buffers.enqueue(first).is_ok());
  ASSERT_TRUE(buffers.enqueue(second).is_ok());
  EXPECT_EQ(buffers.outbound_buffer_bytes(), first.size() + second.size());
  EXPECT_EQ(buffers.pending_write().size(), first.size());
  ASSERT_TRUE(buffers.consume_written(7U).is_ok());
  EXPECT_TRUE(std::ranges::equal(buffers.pending_write(), common::ByteView{first}.subspan(7U)));
  ASSERT_TRUE(buffers.consume_written(first.size() - 7U).is_ok());
  EXPECT_EQ(buffers.pending_write().size(), second.size());
  EXPECT_FALSE(buffers.consume_written(second.size() + 1U).is_ok());
  buffers.clear();
  EXPECT_EQ(buffers.outbound_buffer_bytes(), 0U);
}

TEST(ConnectionBuffersTest, EnforcesInboundOutboundAndFrameCountLimits) {
  const std::vector<std::byte> encoded = query_frame(1U);
  ConnectionBuffers buffers =
      ConnectionBuffers::create({.protocol = {.maximum_payload_size = 64U},
                                 .maximum_inbound_buffer_bytes = kFrameHeaderSize + 64U,
                                 .maximum_outbound_buffer_bytes = encoded.size(),
                                 .maximum_outbound_frames = 1U})
          .value();
  ASSERT_TRUE(buffers.enqueue(encoded).is_ok());
  EXPECT_FALSE(buffers.enqueue(encoded).is_ok());
  const std::array<std::byte, 105> excess{};
  EXPECT_FALSE(buffers.receive(excess).has_value());
}

} // namespace
} // namespace chronos::network
