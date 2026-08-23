#include "chronos/network/native_quorum_ingest_retry.hpp"

#include "gtest/gtest.h"
#include <algorithm>
#include <array>
#include <cstdint>
#include <vector>

namespace chronos::network {
namespace {

[[nodiscard]] common::Uuid group(const std::uint8_t seed = 1U) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] NativeQuorumIngestRetryConfig config(const TlsClientContext& first,
                                                   const TlsClientContext& second) {
  return {.routing = {.group_id = group(),
                      .initial_node_id = 1U,
                      .minimum_placement_epoch = 4U,
                      .routes = {{1U, {{127U, 0U, 0U, 1U}, 7401U}, &first},
                                 {2U, {{127U, 0U, 0U, 2U}, 7402U}, &second}},
                      .limits = {.maximum_routes = 2U, .maximum_redirects = 2U}}};
}

[[nodiscard]] std::vector<std::byte> take_pending(NativeQuorumIngestRetry& retry) {
  const common::ByteView pending = retry.pending_write();
  std::vector<std::byte> bytes(pending.begin(), pending.end());
  EXPECT_TRUE(retry.consume_written(pending.size()).is_ok());
  return bytes;
}

[[nodiscard]] std::vector<std::byte> server_frame(const MessageType type, const std::uint64_t id,
                                                  const common::ByteView payload = {}) {
  return encode_frame({.protocol_major =
                           type == MessageType::kServerHello ? kProtocolMajor : kProtocolV2Major,
                       .message_type = type,
                       .request_id = id},
                      payload)
      .value();
}

void complete_handshake(NativeQuorumIngestRetry& retry) {
  const auto hello = decode_frame(take_pending(retry));
  ASSERT_TRUE(hello.has_value());
  ASSERT_EQ(hello->header.message_type, MessageType::kClientHello);
  const auto offer = decode_client_hello(hello->payload);
  ASSERT_TRUE(offer.has_value());
  EXPECT_EQ(offer->minimum_major, kProtocolV2Major);
  EXPECT_EQ(offer->feature_bits, kProtocolV2QuorumSyncFeature | kProtocolV2LeaderRedirectFeature);
  const auto accepted = retry.receive(
      server_frame(MessageType::kServerHello, 0U,
                   *encode_server_hello({.selected_major = kProtocolV2Major,
                                         .feature_bits = kProtocolV2QuorumSyncFeature |
                                                         kProtocolV2LeaderRedirectFeature})));
  ASSERT_TRUE(accepted.has_value()) << accepted.error().to_string();
}

[[nodiscard]] Frame take_request(NativeQuorumIngestRetry& retry) {
  auto request = decode_frame(take_pending(retry));
  EXPECT_TRUE(request.has_value());
  return request.value_or(Frame{});
}

TEST(NativeQuorumIngestRetryTest, ReplaysExactAppendThroughFreshRedirectedSession) {
  TlsClientContext first;
  TlsClientContext second;
  const std::vector command{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  auto retry = NativeQuorumIngestRetry::create(config(first, second), command);
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  EXPECT_EQ(retry->current_route().node_id, 1U);
  complete_handshake(*retry);
  const Frame first_request = take_request(*retry);
  ASSERT_EQ(first_request.header.request_id, 1U);
  auto first_ingest = decode_ingest_request(
      first_request.payload,
      {.protocol_major = kProtocolV2Major,
       .feature_bits = kProtocolV2QuorumSyncFeature | kProtocolV2LeaderRedirectFeature});
  ASSERT_TRUE(first_ingest.has_value());
  EXPECT_TRUE(std::ranges::equal(first_ingest->encoded_columnar_append, command));

  const auto redirect = encode_leader_redirect(
      {.group_id = group(), .leader_node_id = 2U, .leader_term = 7U, .placement_epoch = 4U});
  auto redirected = retry->receive(
      server_frame(MessageType::kLeaderRedirect, first_request.header.request_id, *redirect));
  ASSERT_TRUE(redirected.has_value()) << redirected.error().to_string();
  EXPECT_TRUE(redirected->reconnect_required);
  EXPECT_EQ(redirected->attempt_number, 2U);
  EXPECT_EQ(retry->current_route().node_id, 2U);

  complete_handshake(*retry);
  const Frame second_request = take_request(*retry);
  EXPECT_EQ(second_request.header.request_id, 1U);
  const auto second_ingest = decode_ingest_request(
      second_request.payload,
      {.protocol_major = kProtocolV2Major,
       .feature_bits = kProtocolV2QuorumSyncFeature | kProtocolV2LeaderRedirectFeature});
  ASSERT_TRUE(second_ingest.has_value());
  EXPECT_TRUE(std::ranges::equal(second_ingest->encoded_columnar_append, command));

  const QuorumSyncIngestAcknowledgement acknowledgement{.group_id = group(),
                                                        .leader_node_id = 2U,
                                                        .leader_term = 8U,
                                                        .log_index = 10U,
                                                        .entry_term = 8U,
                                                        .local_durable_physical_sequence = 12U};
  auto completed = retry->receive(
      server_frame(MessageType::kQuorumSyncIngestAcknowledgement, second_request.header.request_id,
                   *encode_quorum_sync_ingest_acknowledgement(acknowledgement)));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  EXPECT_EQ(completed->acknowledgement, acknowledgement);
  EXPECT_EQ(retry->state(), NativeQuorumIngestRetryState::kComplete);
  EXPECT_EQ(*retry->result(), acknowledgement);
  EXPECT_TRUE(retry->pending_write().empty());
}

TEST(NativeQuorumIngestRetryTest, FailsStickyOnStaleRedirectWithoutChangingRoute) {
  TlsClientContext first;
  TlsClientContext second;
  auto retry = NativeQuorumIngestRetry::create(config(first, second), {std::byte{1U}});
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  complete_handshake(*retry);
  const Frame request = take_request(*retry);
  const auto stale = encode_leader_redirect(
      {.group_id = group(), .leader_node_id = 2U, .leader_term = 1U, .placement_epoch = 3U});
  auto rejected =
      retry->receive(server_frame(MessageType::kLeaderRedirect, request.header.request_id, *stale));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(retry->state(), NativeQuorumIngestRetryState::kFailed);
  EXPECT_EQ(retry->current_route().node_id, 1U);
  EXPECT_EQ(retry->attempts_started(), 1U);
  EXPECT_EQ(retry->failure(), rejected.error());
  EXPECT_FALSE(retry->result().has_value());
}

TEST(NativeQuorumIngestRetryTest, RequiresEveryProtocolFeatureAndExactReceiptAuthority) {
  TlsClientContext first;
  TlsClientContext second;
  auto missing_feature = NativeQuorumIngestRetry::create(config(first, second), {std::byte{1U}});
  ASSERT_TRUE(missing_feature.has_value()) << missing_feature.error().to_string();
  static_cast<void>(take_pending(*missing_feature));
  auto rejected = missing_feature->receive(
      server_frame(MessageType::kServerHello, 0U,
                   *encode_server_hello({.selected_major = kProtocolV2Major,
                                         .feature_bits = kProtocolV2QuorumSyncFeature})));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(missing_feature->state(), NativeQuorumIngestRetryState::kFailed);

  auto wrong_receipt = NativeQuorumIngestRetry::create(config(first, second), {std::byte{2U}});
  ASSERT_TRUE(wrong_receipt.has_value()) << wrong_receipt.error().to_string();
  complete_handshake(*wrong_receipt);
  const Frame request = take_request(*wrong_receipt);
  const QuorumSyncIngestAcknowledgement acknowledgement{.group_id = group(),
                                                        .leader_node_id = 2U,
                                                        .leader_term = 4U,
                                                        .log_index = 5U,
                                                        .entry_term = 4U,
                                                        .local_durable_physical_sequence = 6U};
  rejected = wrong_receipt->receive(
      server_frame(MessageType::kQuorumSyncIngestAcknowledgement, request.header.request_id,
                   *encode_quorum_sync_ingest_acknowledgement(acknowledgement)));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(wrong_receipt->state(), NativeQuorumIngestRetryState::kFailed);
}

} // namespace
} // namespace chronos::network
