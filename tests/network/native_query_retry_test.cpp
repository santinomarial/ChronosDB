#include "chronos/network/native_query_retry.hpp"

#include "gtest/gtest.h"
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace chronos::network {
namespace {

[[nodiscard]] common::Uuid group() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{1U};
  return common::Uuid{bytes};
}

[[nodiscard]] NativeQueryRetryConfig config(const TlsClientContext& first,
                                            const TlsClientContext& second) {
  return {.routing = {.group_id = group(),
                      .initial_node_id = 1U,
                      .minimum_placement_epoch = 4U,
                      .routes = {{1U, {{127U, 0U, 0U, 1U}, 7401U}, &first},
                                 {2U, {{127U, 0U, 0U, 2U}, 7402U}, &second}},
                      .limits = {.maximum_routes = 2U, .maximum_redirects = 2U}},
          .limits = {.maximum_result_rows = 2U,
                     .maximum_result_batches = 2U,
                     .maximum_result_payload_bytes = 4096U}};
}

[[nodiscard]] std::vector<std::byte> take_pending(NativeQueryRetry& retry) {
  const common::ByteView pending = retry.pending_write();
  std::vector<std::byte> bytes(pending.begin(), pending.end());
  EXPECT_TRUE(retry.consume_written(pending.size()).is_ok());
  return bytes;
}

[[nodiscard]] std::vector<std::byte> server_frame(const MessageType type, const std::uint64_t id,
                                                  const common::ByteView payload = {},
                                                  const std::uint32_t flags = 0U) {
  return encode_frame({.protocol_major =
                           type == MessageType::kServerHello ? kProtocolMajor : kProtocolV2Major,
                       .message_type = type,
                       .flags = flags,
                       .request_id = id},
                      payload)
      .value();
}

void complete_handshake(NativeQueryRetry& retry) {
  const auto hello = decode_frame(take_pending(retry));
  ASSERT_TRUE(hello.has_value());
  const auto offer = decode_client_hello(hello->payload);
  ASSERT_TRUE(offer.has_value());
  EXPECT_EQ(offer->minimum_major, kProtocolV2Major);
  EXPECT_EQ(offer->feature_bits, kProtocolV2LeaderRedirectFeature);
  const auto accepted = retry.receive(
      server_frame(MessageType::kServerHello, 0U,
                   *encode_server_hello({.selected_major = kProtocolV2Major,
                                         .feature_bits = kProtocolV2LeaderRedirectFeature})));
  ASSERT_TRUE(accepted.has_value()) << accepted.error().to_string();
}

[[nodiscard]] Frame take_request(NativeQueryRetry& retry) {
  auto request = decode_frame(take_pending(retry));
  EXPECT_TRUE(request.has_value());
  return request.value_or(Frame{});
}

[[nodiscard]] std::vector<std::byte> one_row_batch() {
  const std::array columns{QueryResultColumn{
      .name = "value",
      .type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
      .nullable = false}};
  const std::array value{std::byte{42U}, std::byte{0U}, std::byte{0U}, std::byte{0U},
                         std::byte{0U},  std::byte{0U}, std::byte{0U}, std::byte{0U}};
  const std::array cells{QueryResultCell{.value = value}};
  return encode_query_result_batch(1U, columns, cells).value();
}

TEST(NativeQueryRetryTest, ReplaysExactSqlAndPublishesOnlyCompleteResult) {
  TlsClientContext first;
  TlsClientContext second;
  const std::string sql = "SELECT value FROM events";
  auto retry = NativeQueryRetry::create(config(first, second), sql);
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  complete_handshake(*retry);
  const Frame first_request = take_request(*retry);
  ASSERT_EQ(first_request.header.request_id, 1U);
  const auto first_sql = decode_query_request(first_request.payload);
  ASSERT_TRUE(first_sql.has_value());
  EXPECT_TRUE(std::ranges::equal(*first_sql, std::as_bytes(std::span{sql.data(), sql.size()})));

  const auto redirect = encode_leader_redirect(
      {.group_id = group(), .leader_node_id = 2U, .leader_term = 7U, .placement_epoch = 4U});
  auto redirected = retry->receive(
      server_frame(MessageType::kLeaderRedirect, first_request.header.request_id, *redirect));
  ASSERT_TRUE(redirected.has_value()) << redirected.error().to_string();
  EXPECT_TRUE(redirected->reconnect_required);
  EXPECT_EQ(retry->current_route().node_id, 2U);
  EXPECT_FALSE(retry->result().has_value());

  complete_handshake(*retry);
  const Frame second_request = take_request(*retry);
  EXPECT_EQ(second_request.header.request_id, 1U);
  const auto second_sql = decode_query_request(second_request.payload);
  ASSERT_TRUE(second_sql.has_value());
  EXPECT_TRUE(std::ranges::equal(*second_sql, std::as_bytes(std::span{sql.data(), sql.size()})));

  const auto batch = one_row_batch();
  auto partial = retry->receive(server_frame(
      MessageType::kQueryResult, second_request.header.request_id, batch, kFrameFlagEndStream));
  ASSERT_TRUE(partial.has_value()) << partial.error().to_string();
  EXPECT_EQ(partial->result_batches_received, 1U);
  EXPECT_FALSE(partial->completed);
  EXPECT_FALSE(retry->result().has_value());

  auto complete =
      retry->receive(server_frame(MessageType::kQueryEnd, second_request.header.request_id));
  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_TRUE(complete->completed);
  ASSERT_TRUE(retry->result().has_value());
  EXPECT_EQ(retry->result()->row_count, 1U);
  EXPECT_EQ(retry->result()->payload_bytes, batch.size());
  ASSERT_EQ(retry->result()->encoded_batches.size(), 1U);
  EXPECT_EQ(retry->result()->encoded_batches.front(), batch);
  EXPECT_TRUE(retry->pending_write().empty());
}

TEST(NativeQueryRetryTest, FailsStickyOnStaleRedirectAndNeverPublishesRows) {
  TlsClientContext first;
  TlsClientContext second;
  auto retry = NativeQueryRetry::create(config(first, second), "SELECT 1");
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  complete_handshake(*retry);
  const Frame request = take_request(*retry);
  const auto redirect = encode_leader_redirect(
      {.group_id = group(), .leader_node_id = 2U, .leader_term = 1U, .placement_epoch = 3U});
  auto rejected = retry->receive(
      server_frame(MessageType::kLeaderRedirect, request.header.request_id, *redirect));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(retry->state(), NativeQueryRetryState::kFailed);
  EXPECT_EQ(retry->current_route().node_id, 1U);
  EXPECT_EQ(retry->attempts_started(), 1U);
  EXPECT_EQ(retry->failure(), rejected.error());
  EXPECT_FALSE(retry->result().has_value());
}

TEST(NativeQueryRetryTest, RequiresRedirectFeatureAndEnforcesAggregateLimits) {
  TlsClientContext first;
  TlsClientContext second;
  auto missing_feature = NativeQueryRetry::create(config(first, second), "SELECT 1");
  ASSERT_TRUE(missing_feature.has_value()) << missing_feature.error().to_string();
  static_cast<void>(take_pending(*missing_feature));
  auto rejected = missing_feature->receive(server_frame(
      MessageType::kServerHello, 0U, *encode_server_hello({.selected_major = kProtocolV2Major})));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(missing_feature->state(), NativeQueryRetryState::kFailed);

  auto limited = NativeQueryRetry::create(config(first, second), "SELECT value");
  ASSERT_TRUE(limited.has_value()) << limited.error().to_string();
  complete_handshake(*limited);
  const Frame request = take_request(*limited);
  const auto batch = one_row_batch();
  auto first_batch =
      limited->receive(server_frame(MessageType::kQueryResult, request.header.request_id, batch));
  ASSERT_TRUE(first_batch.has_value()) << first_batch.error().to_string();
  auto second_batch =
      limited->receive(server_frame(MessageType::kQueryResult, request.header.request_id, batch));
  ASSERT_TRUE(second_batch.has_value()) << second_batch.error().to_string();
  rejected = limited->receive(server_frame(MessageType::kQueryResult, request.header.request_id,
                                           batch, kFrameFlagEndStream));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(limited->state(), NativeQueryRetryState::kFailed);
  EXPECT_FALSE(limited->result().has_value());
}

} // namespace
} // namespace chronos::network
