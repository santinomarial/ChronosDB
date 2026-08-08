#include "chronos/network/connection_state.hpp"

#include <gtest/gtest.h>

namespace chronos::network {
namespace {

[[nodiscard]] Frame frame(const MessageType type, const std::uint64_t id,
                          std::vector<std::byte> payload = {}) {
  return {.header = {.message_type = type,
                     .request_id = id,
                     .payload_size = static_cast<std::uint32_t>(payload.size())},
          .payload = std::move(payload)};
}

TEST(ServerConnectionStateTest, RequiresHelloNegotiatesLimitsAndAcceptsLifecycles) {
  ServerConnectionState state =
      ServerConnectionState::create(
          {.limits = {.maximum_payload_size = 4096U}, .maximum_in_flight_requests = 2U})
          .value();
  EXPECT_FALSE(state.accept(frame(MessageType::kQueryRequest, 1U)).has_value());
  auto handshake = state.accept(
      frame(MessageType::kClientHello, 0U, *encode_client_hello({.maximum_payload_size = 2048U})));
  ASSERT_TRUE(handshake.has_value());
  EXPECT_EQ(handshake->kind, InboundActionKind::kHandshake);
  EXPECT_EQ(state.negotiated_maximum_payload_size(), 2048U);

  auto query =
      state.accept(frame(MessageType::kQueryRequest, 1U, *encode_query_request("SELECT 1")));
  ASSERT_TRUE(query.has_value());
  auto ingest = state.accept(frame(MessageType::kIngestRequest, 2U,
                                   *encode_ingest_request(DurabilityMode::kLocalSync, {})));
  ASSERT_TRUE(ingest.has_value());
  EXPECT_EQ(state.in_flight_requests(), 2U);
  EXPECT_EQ(state.active_request_type(1U), MessageType::kQueryRequest);
  EXPECT_EQ(state.active_request_type(2U), MessageType::kIngestRequest);
  EXPECT_FALSE(
      state.accept(frame(MessageType::kQueryRequest, 3U, *encode_query_request("SELECT 2")))
          .has_value());
  EXPECT_TRUE(state.complete(1U));
  EXPECT_FALSE(state.active_request_type(1U).has_value());
  EXPECT_FALSE(state.complete(1U));
  EXPECT_TRUE(state.accept(frame(MessageType::kQueryRequest, 3U, *encode_query_request("SELECT 2")))
                  .has_value());
}

TEST(ServerConnectionStateTest, CancellationIsIdempotentAndRequestIdsCannotRaceReuse) {
  ServerConnectionState state = ServerConnectionState::create().value();
  ASSERT_TRUE(
      state.accept(frame(MessageType::kClientHello, 0U, *encode_client_hello({}))).has_value());
  ASSERT_TRUE(state.accept(frame(MessageType::kQueryRequest, 9U, *encode_query_request("SELECT 1")))
                  .has_value());
  const auto first = state.accept(frame(MessageType::kCancel, 9U));
  ASSERT_TRUE(first.has_value());
  EXPECT_TRUE(first->cancellation_was_active);
  const auto repeated = state.accept(frame(MessageType::kCancel, 9U));
  ASSERT_TRUE(repeated.has_value());
  EXPECT_FALSE(repeated->cancellation_was_active);
  EXPECT_FALSE(
      state.accept(frame(MessageType::kQueryRequest, 9U, *encode_query_request("SELECT 2")))
          .has_value());
  EXPECT_FALSE(state.accept(frame(MessageType::kCancel, 10U)).has_value());
}

TEST(ServerConnectionStateTest, RejectsWrongDirectionRepeatedHelloAndPostCloseInput) {
  ServerConnectionState state = ServerConnectionState::create().value();
  ASSERT_TRUE(
      state.accept(frame(MessageType::kClientHello, 0U, *encode_client_hello({}))).has_value());
  EXPECT_FALSE(state.accept(frame(MessageType::kServerHello, 0U)).has_value());
  EXPECT_FALSE(
      state.accept(frame(MessageType::kClientHello, 0U, *encode_client_hello({}))).has_value());
  EXPECT_TRUE(state.accept(frame(MessageType::kPing, 0U)).has_value());
  state.close();
  EXPECT_EQ(state.phase(), ConnectionPhase::kClosed);
  EXPECT_FALSE(state.accept(frame(MessageType::kPing, 0U)).has_value());
}

TEST(ServerConnectionStateTest, RequiresExactQueryResponseStreamCompletion) {
  ServerConnectionState state = ServerConnectionState::create().value();
  ASSERT_TRUE(
      state.accept(frame(MessageType::kClientHello, 0U, *encode_client_hello({}))).has_value());
  ASSERT_TRUE(state.accept(frame(MessageType::kQueryRequest, 1U, *encode_query_request("SELECT 1")))
                  .has_value());
  EXPECT_FALSE(state.accept_response(frame(MessageType::kQueryEnd, 1U)).is_ok());
  EXPECT_TRUE(state
                  .accept_response({.header = {.message_type = MessageType::kQueryResult,
                                               .flags = kFrameFlagEndStream,
                                               .request_id = 1U}})
                  .is_ok());
  EXPECT_FALSE(state.accept_response(frame(MessageType::kQueryResult, 1U)).is_ok());
  EXPECT_TRUE(state.accept_response(frame(MessageType::kQueryEnd, 1U)).is_ok());
  EXPECT_EQ(state.in_flight_requests(), 0U);
}

} // namespace
} // namespace chronos::network
