#include "chronos/network/connection_state.hpp"

#include <array>
#include <gtest/gtest.h>

namespace chronos::network {
namespace {

[[nodiscard]] Frame frame(const MessageType type, const std::uint64_t id,
                          std::vector<std::byte> payload = {}, const std::uint16_t minor = 0U,
                          const std::uint16_t major = kProtocolMajor) {
  return {.header = {.protocol_major = major,
                     .protocol_minor = minor,
                     .message_type = type,
                     .request_id = id,
                     .payload_size = static_cast<std::uint32_t>(payload.size())},
          .payload = std::move(payload)};
}

TEST(ServerConnectionStateTest, NegotiatesProtocolTwoQuorumSyncAndRequiresItsReceipt) {
  ServerConnectionState state =
      ServerConnectionState::create({.maximum_protocol_major = kProtocolV2Major,
                                     .supported_feature_bits = kProtocolV2QuorumSyncFeature})
          .value();
  const auto hello = encode_client_hello({.minimum_major = kProtocolV2Major,
                                          .maximum_major = kProtocolV2Major,
                                          .maximum_minor = kProtocolV2LatestMinor,
                                          .feature_bits = kProtocolV2QuorumSyncFeature});
  ASSERT_TRUE(hello.has_value());
  const auto handshake = state.accept(frame(MessageType::kClientHello, 0U, *hello));
  ASSERT_TRUE(handshake.has_value()) << handshake.error().to_string();
  EXPECT_EQ(handshake->negotiated_major, kProtocolV2Major);
  EXPECT_EQ(state.negotiated_major(), kProtocolV2Major);
  EXPECT_EQ(state.negotiated_feature_bits(), kProtocolV2QuorumSyncFeature);

  const IngestProtocolContext context{.protocol_major = kProtocolV2Major,
                                      .feature_bits = kProtocolV2QuorumSyncFeature};
  const auto request = encode_ingest_request(DurabilityMode::kQuorumSync, {}, context);
  ASSERT_TRUE(request.has_value());
  ASSERT_TRUE(state.accept(frame(MessageType::kIngestRequest, 1U, *request, 0U, kProtocolV2Major))
                  .has_value());
  EXPECT_FALSE(state
                   .accept_response(frame(
                       MessageType::kIngestAcknowledgement, 1U,
                       *encode_ingest_acknowledgement({.outcome = IngestOutcome::kMatchingRetry}),
                       0U, kProtocolV2Major))
                   .is_ok());

  common::Uuid::Bytes group_bytes{};
  group_bytes.fill(std::byte{1});
  const auto acknowledgement =
      encode_quorum_sync_ingest_acknowledgement({.group_id = common::Uuid{group_bytes},
                                                 .leader_node_id = 1U,
                                                 .leader_term = 3U,
                                                 .log_index = 4U,
                                                 .entry_term = 3U,
                                                 .local_durable_physical_sequence = 8U});
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_TRUE(state
                  .accept_response(frame(MessageType::kQuorumSyncIngestAcknowledgement, 1U,
                                         *acknowledgement, 0U, kProtocolV2Major))
                  .is_ok());
  EXPECT_EQ(state.in_flight_requests(), 0U);
}

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  return Identifier::from_uuid(uuid(seed)).value();
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
                                               .request_id = 1U},
                                    .payload = {}})
                  .is_ok());
  EXPECT_FALSE(state.accept_response(frame(MessageType::kQueryResult, 1U)).is_ok());
  EXPECT_TRUE(state.accept_response(frame(MessageType::kQueryEnd, 1U)).is_ok());
  EXPECT_EQ(state.in_flight_requests(), 0U);
}

TEST(ServerConnectionStateTest, NegotiatesAndEnforcesSubscriptionLifecycle) {
  ServerConnectionState state = ServerConnectionState::create().value();
  const auto handshake = state.accept(frame(
      MessageType::kClientHello, 0U,
      *encode_client_hello({.maximum_minor = 1U, .feature_bits = kProtocolV1SubscriptionFeature})));
  ASSERT_TRUE(handshake.has_value()) << handshake.error().to_string();
  EXPECT_EQ(handshake->negotiated_minor, 1U);
  EXPECT_EQ(handshake->negotiated_feature_bits, kProtocolV1SubscriptionFeature);

  const auto subscription = state.accept(
      frame(MessageType::kSubscribeRequest, 1U,
            *encode_subscription_request({.mode = SubscriptionStartMode::kNewQuery,
                                          .subscription_id = uuid(std::byte{1}),
                                          .body = std::as_bytes(std::span{"SELECT 1", 8U})}),
            1U));
  ASSERT_TRUE(subscription.has_value()) << subscription.error().to_string();
  EXPECT_EQ(subscription->kind, InboundActionKind::kSubscribe);

  EXPECT_TRUE(state
                  .accept_response({.header = {.message_type = MessageType::kQueryResult,
                                               .flags = kFrameFlagEndStream,
                                               .request_id = 1U},
                                    .payload = {}})
                  .is_ok());
  const std::array<std::byte, 1> token{std::byte{9}};
  EXPECT_TRUE(state
                  .accept_response(
                      frame(MessageType::kSubscriptionReady, 1U, *encode_subscription_ready(token)))
                  .is_ok());

  SubscriptionLogId log{};
  log.fill(std::byte{4});
  const std::array<std::byte, 1> key{std::byte{5}};
  const std::array<std::byte, 1> body{std::byte{6}};
  const auto change = encode_subscription_change({SubscriptionChangeOperation::kUpsert, 7U,
                                                  identifier<schema::TabletId>(std::byte{2}), log,
                                                  3U, identifier<schema::SchemaId>(std::byte{3}),
                                                  schema::SchemaVersion::initial(), key, body});
  ASSERT_TRUE(change.has_value());
  EXPECT_TRUE(state.accept_response(frame(MessageType::kSubscriptionChange, 1U, *change)).is_ok());
  const auto acknowledgement = state.accept(frame(MessageType::kSubscriptionAcknowledge, 1U,
                                                  *encode_subscription_acknowledgement({7U}), 1U));
  ASSERT_TRUE(acknowledgement.has_value()) << acknowledgement.error().to_string();
  EXPECT_EQ(acknowledgement->kind, InboundActionKind::kSubscriptionAcknowledge);
  EXPECT_EQ(acknowledgement->acknowledged_delivery_sequence, 7U);
  EXPECT_TRUE(state
                  .accept_response(
                      frame(MessageType::kSubscriptionCheckpoint, 1U,
                            *encode_subscription_checkpoint(
                                {.acknowledged_delivery_sequence = 7U, .resume_token = token})))
                  .is_ok());

  const auto cancellation = state.accept(frame(MessageType::kCancel, 1U, {}, 1U));
  ASSERT_TRUE(cancellation.has_value());
  EXPECT_TRUE(cancellation->cancellation_was_active);
  EXPECT_TRUE(state.active_request_type(1U).has_value());
  EXPECT_FALSE(state.accept(frame(MessageType::kCancel, 1U, {}, 1U))->cancellation_was_active);
  EXPECT_TRUE(state
                  .accept_response(
                      frame(MessageType::kSubscriptionEnd, 1U,
                            *encode_subscription_end({.reason = SubscriptionEndReason::kCancelled,
                                                      .safe_delivery_sequence = 7U,
                                                      .resume_token = token})))
                  .is_ok());
  EXPECT_EQ(state.in_flight_requests(), 0U);
}

TEST(ServerConnectionStateTest, RejectsSubscriptionWithoutNegotiatedMinorAndFeature) {
  ServerConnectionState state = ServerConnectionState::create().value();
  ASSERT_TRUE(
      state.accept(frame(MessageType::kClientHello, 0U, *encode_client_hello({}))).has_value());
  EXPECT_FALSE(state
                   .accept(frame(MessageType::kSubscribeRequest, 1U,
                                 *encode_subscription_request(
                                     {.mode = SubscriptionStartMode::kNewQuery,
                                      .subscription_id = uuid(std::byte{1}),
                                      .body = std::as_bytes(std::span{"SELECT 1", 8U})}),
                                 1U))
                   .has_value());
}

} // namespace
} // namespace chronos::network
