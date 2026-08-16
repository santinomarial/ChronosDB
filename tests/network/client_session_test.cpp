#include "chronos/network/client_session.hpp"

#include <algorithm>
#include <array>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::network {
namespace {

[[nodiscard]] std::vector<std::byte> take_pending(NativeClientSession& client) {
  const common::ByteView pending = client.pending_write();
  std::vector<std::byte> bytes(pending.begin(), pending.end());
  EXPECT_TRUE(client.consume_written(pending.size()).is_ok());
  return bytes;
}

[[nodiscard]] std::vector<std::byte> server_frame(const MessageType type, const std::uint64_t id,
                                                  const common::ByteView payload = {},
                                                  const std::uint32_t flags = 0U,
                                                  const std::uint16_t minor = 0U,
                                                  const std::uint16_t major = kProtocolMajor) {
  return encode_frame({.protocol_major = major,
                       .protocol_minor = minor,
                       .message_type = type,
                       .flags = flags,
                       .request_id = id},
                      payload)
      .value();
}

TEST(NativeClientSessionTest, NegotiatesAndValidatesProtocolTwoQuorumSync) {
  NativeClientSession client =
      NativeClientSession::create({.minimum_protocol_major = kProtocolV2Major,
                                   .maximum_protocol_major = kProtocolV2Major,
                                   .maximum_protocol_minor = kProtocolV2LatestMinor,
                                   .requested_feature_bits = kProtocolV2QuorumSyncFeature})
          .value();
  ASSERT_TRUE(client.queue_handshake().is_ok());
  const auto hello_frame = decode_frame(take_pending(client));
  ASSERT_TRUE(hello_frame.has_value());
  EXPECT_EQ(hello_frame->header.protocol_major, kProtocolMajor);
  EXPECT_EQ(decode_client_hello(hello_frame->payload)->minimum_major, kProtocolV2Major);
  ASSERT_TRUE(client
                  .receive(server_frame(
                      MessageType::kServerHello, 0U,
                      *encode_server_hello({.selected_major = kProtocolV2Major,
                                            .feature_bits = kProtocolV2QuorumSyncFeature})))
                  .has_value());
  EXPECT_EQ(client.negotiated_major(), kProtocolV2Major);

  const auto request_id = client.queue_ingest(DurabilityMode::kQuorumSync, {});
  ASSERT_TRUE(request_id.has_value()) << request_id.error().to_string();
  const auto request = decode_frame(take_pending(client));
  ASSERT_TRUE(request.has_value());
  EXPECT_EQ(request->header.protocol_major, kProtocolV2Major);
  EXPECT_EQ(decode_ingest_request(request->payload, {.protocol_major = kProtocolV2Major,
                                                     .feature_bits = kProtocolV2QuorumSyncFeature})
                ->durability,
            DurabilityMode::kQuorumSync);

  common::Uuid::Bytes group_bytes{};
  group_bytes.fill(std::byte{2});
  const auto acknowledgement =
      encode_quorum_sync_ingest_acknowledgement({.group_id = common::Uuid{group_bytes},
                                                 .leader_node_id = 3U,
                                                 .leader_term = 5U,
                                                 .log_index = 9U,
                                                 .entry_term = 5U,
                                                 .local_durable_physical_sequence = 12U});
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_TRUE(client
                  .receive(server_frame(MessageType::kQuorumSyncIngestAcknowledgement, *request_id,
                                        *acknowledgement, 0U, 0U, kProtocolV2Major))
                  .has_value());
  EXPECT_EQ(client.in_flight_requests(), 0U);
}

TEST(NativeClientSessionTest, AcceptsOnlyNegotiatedRedirectBeforeQueryOutput) {
  NativeClientSession client =
      NativeClientSession::create({.minimum_protocol_major = kProtocolV2Major,
                                   .maximum_protocol_major = kProtocolV2Major,
                                   .maximum_protocol_minor = kProtocolV2LatestMinor,
                                   .requested_feature_bits = kProtocolV2LeaderRedirectFeature})
          .value();
  ASSERT_TRUE(client.queue_handshake().is_ok());
  static_cast<void>(take_pending(client));
  ASSERT_TRUE(client
                  .receive(server_frame(
                      MessageType::kServerHello, 0U,
                      *encode_server_hello({.selected_major = kProtocolV2Major,
                                            .feature_bits = kProtocolV2LeaderRedirectFeature})))
                  .has_value());
  common::Uuid::Bytes group_bytes{};
  group_bytes.fill(std::byte{3});
  const auto redirect = encode_leader_redirect({.group_id = common::Uuid{group_bytes},
                                                .leader_node_id = 5U,
                                                .leader_term = 8U,
                                                .placement_epoch = 13U});
  ASSERT_TRUE(redirect.has_value());
  const auto first = client.queue_query("SELECT 1");
  ASSERT_TRUE(first.has_value());
  static_cast<void>(take_pending(client));
  const auto received = client.receive(
      server_frame(MessageType::kLeaderRedirect, *first, *redirect, 0U, 0U, kProtocolV2Major));
  ASSERT_TRUE(received.has_value()) << received.error().to_string();
  ASSERT_EQ(received->size(), 1U);
  EXPECT_EQ(*decode_leader_redirect(received->front().payload), *decode_leader_redirect(*redirect));
  EXPECT_EQ(client.in_flight_requests(), 0U);

  const auto second = client.queue_query("SELECT 2");
  ASSERT_TRUE(second.has_value());
  static_cast<void>(take_pending(client));
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  ASSERT_TRUE(client
                  .receive(server_frame(MessageType::kQueryResult, *second,
                                        *encode_query_result_batch(0U, columns, {}), 0U, 0U,
                                        kProtocolV2Major))
                  .has_value());
  EXPECT_FALSE(client
                   .receive(server_frame(MessageType::kLeaderRedirect, *second, *redirect, 0U, 0U,
                                         kProtocolV2Major))
                   .has_value());
  EXPECT_EQ(client.phase(), ClientSessionPhase::kClosed);

  NativeClientSession unnegotiated =
      NativeClientSession::create({.minimum_protocol_major = kProtocolV2Major,
                                   .maximum_protocol_major = kProtocolV2Major,
                                   .maximum_protocol_minor = kProtocolV2LatestMinor})
          .value();
  ASSERT_TRUE(unnegotiated.queue_handshake().is_ok());
  static_cast<void>(take_pending(unnegotiated));
  ASSERT_TRUE(unnegotiated
                  .receive(server_frame(MessageType::kServerHello, 0U,
                                        *encode_server_hello({.selected_major = kProtocolV2Major})))
                  .has_value());
  const auto unnegotiated_request = unnegotiated.queue_query("SELECT 3");
  ASSERT_TRUE(unnegotiated_request.has_value());
  static_cast<void>(take_pending(unnegotiated));
  EXPECT_FALSE(unnegotiated
                   .receive(server_frame(MessageType::kLeaderRedirect, *unnegotiated_request,
                                         *redirect, 0U, 0U, kProtocolV2Major))
                   .has_value());
}

void complete_subscription_handshake(NativeClientSession& client) {
  ASSERT_TRUE(client.queue_handshake().is_ok());
  const auto hello_frame = decode_frame(take_pending(client));
  ASSERT_TRUE(hello_frame.has_value());
  const auto hello = decode_client_hello(hello_frame->payload);
  ASSERT_TRUE(hello.has_value());
  ASSERT_EQ(hello->maximum_minor, 1U);
  ASSERT_EQ(hello->feature_bits, kProtocolV1SubscriptionFeature);
  ASSERT_TRUE(client
                  .receive(server_frame(
                      MessageType::kServerHello, 0U,
                      *encode_server_hello(
                          {.selected_minor = 1U, .feature_bits = kProtocolV1SubscriptionFeature})))
                  .has_value());
  ASSERT_EQ(client.negotiated_minor(), 1U);
  ASSERT_EQ(client.negotiated_feature_bits(), kProtocolV1SubscriptionFeature);
}

[[nodiscard]] common::Uuid uuid(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return common::Uuid{bytes};
}

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::byte seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

void complete_handshake(NativeClientSession& client) {
  ASSERT_TRUE(client.queue_handshake().is_ok());
  const auto hello = decode_frame(take_pending(client));
  ASSERT_TRUE(hello.has_value());
  ASSERT_EQ(hello->header.message_type, MessageType::kClientHello);
  const std::vector<std::byte> response =
      server_frame(MessageType::kServerHello, 0U, *encode_server_hello({}));
  const std::size_t split = response.size() / 2U;
  ASSERT_TRUE(client.receive(common::ByteView{response}.first(split)).has_value());
  ASSERT_TRUE(client.receive(common::ByteView{response}.subspan(split)).has_value());
  ASSERT_EQ(client.phase(), ClientSessionPhase::kActive);
}

TEST(NativeClientSessionTest, RefusesQuorumSyncWithoutProtocolTwoFeature) {
  NativeClientSession client = NativeClientSession::create().value();
  complete_handshake(client);
  EXPECT_FALSE(client.queue_ingest(DurabilityMode::kQuorumSync, {}).has_value());
}

TEST(NativeClientSessionTest, OwnsPartialIoAndValidatesQueryStream) {
  NativeClientSession client = NativeClientSession::create().value();
  complete_handshake(client);
  const auto request_id = client.queue_query("SELECT 1");
  ASSERT_TRUE(request_id.has_value());
  const auto request = decode_frame(take_pending(client));
  ASSERT_TRUE(request.has_value());
  EXPECT_EQ(request->header.message_type, MessageType::kQueryRequest);

  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  const std::array<std::byte, 8> value{std::byte{7}};
  const std::array<QueryResultCell, 1> cells{QueryResultCell{.value = value}};
  const auto batch = encode_query_result_batch(1U, columns, cells).value();
  std::vector<std::byte> responses =
      server_frame(MessageType::kQueryResult, *request_id, batch, kFrameFlagEndStream);
  const std::vector<std::byte> end = server_frame(MessageType::kQueryEnd, *request_id);
  responses.insert(responses.end(), end.begin(), end.end());
  const auto decoded = client.receive(responses);
  ASSERT_TRUE(decoded.has_value());
  ASSERT_EQ(decoded->size(), 2U);
  EXPECT_EQ(client.in_flight_requests(), 0U);
}

TEST(NativeClientSessionTest, ValidatesIngestDurabilityAndCancellationIdentity) {
  NativeClientSession client = NativeClientSession::create().value();
  complete_handshake(client);
  const auto ingest_id = client.queue_ingest(DurabilityMode::kLocalSync, {});
  ASSERT_TRUE(ingest_id.has_value());
  static_cast<void>(take_pending(client));
  const auto acknowledgement =
      encode_ingest_acknowledgement({.requested_durability = DurabilityMode::kLocalSync,
                                     .effective_durability = DurabilityMode::kLocalSync,
                                     .outcome = IngestOutcome::kApplied,
                                     .record_sequence = 1U,
                                     .segment_number = 1U});
  ASSERT_TRUE(acknowledgement.has_value());
  EXPECT_TRUE(
      client
          .receive(server_frame(MessageType::kIngestAcknowledgement, *ingest_id, *acknowledgement))
          .has_value());
  const auto query_id = client.queue_query("SELECT 1");
  ASSERT_TRUE(query_id.has_value());
  static_cast<void>(take_pending(client));
  EXPECT_TRUE(client.queue_cancel(*query_id).is_ok());
  EXPECT_EQ(client.in_flight_requests(), 0U);
  EXPECT_TRUE(client.queue_cancel(*query_id).is_ok());
}

TEST(NativeClientSessionTest, FailsClosedOnWrongDirectionOrPrematureQueryEnd) {
  NativeClientSession client = NativeClientSession::create().value();
  complete_handshake(client);
  const auto id = client.queue_query("SELECT 1").value();
  static_cast<void>(take_pending(client));
  EXPECT_FALSE(client.receive(server_frame(MessageType::kQueryEnd, id)).has_value());
  EXPECT_EQ(client.phase(), ClientSessionPhase::kClosed);

  NativeClientSession wrong_direction = NativeClientSession::create().value();
  complete_handshake(wrong_direction);
  EXPECT_FALSE(wrong_direction.receive(server_frame(MessageType::kClientHello, 0U)).has_value());
  EXPECT_EQ(wrong_direction.phase(), ClientSessionPhase::kClosed);
}

TEST(NativeClientSessionTest, DrivesNegotiatedAtLeastOnceSubscriptionLifecycle) {
  NativeClientSession client =
      NativeClientSession::create(
          {.maximum_protocol_minor = 1U, .requested_feature_bits = kProtocolV1SubscriptionFeature})
          .value();
  complete_subscription_handshake(client);
  const std::uint64_t request_id =
      client.queue_subscription(uuid(std::byte{1}), "SELECT * FROM trades").value();
  const auto request_frame = decode_frame(take_pending(client));
  ASSERT_TRUE(request_frame.has_value());
  EXPECT_EQ(request_frame->header.protocol_minor, 1U);
  EXPECT_EQ(request_frame->header.message_type, MessageType::kSubscribeRequest);
  EXPECT_EQ(decode_subscription_request(request_frame->payload)->mode,
            SubscriptionStartMode::kNewQuery);

  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  const auto empty_snapshot = encode_query_result_batch(0U, columns, {}).value();
  ASSERT_TRUE(client
                  .receive(server_frame(MessageType::kQueryResult, request_id, empty_snapshot,
                                        kFrameFlagEndStream, 1U))
                  .has_value());
  const std::array<std::byte, 2> token{std::byte{8}, std::byte{9}};
  ASSERT_TRUE(client
                  .receive(server_frame(MessageType::kSubscriptionReady, request_id,
                                        *encode_subscription_ready(token), 0U, 1U))
                  .has_value());

  SubscriptionSourceId log{};
  log.fill(std::byte{4});
  const std::array<std::byte, 1> key{std::byte{5}};
  const std::array<std::byte, 1> body{std::byte{6}};
  const auto change =
      encode_subscription_change({.operation = SubscriptionChangeOperation::kUpsert,
                                  .delivery_sequence = 1U,
                                  .tablet_id = identifier<schema::TabletId>(std::byte{2}),
                                  .source_id = log,
                                  .source_sequence = 3U,
                                  .schema_id = identifier<schema::SchemaId>(std::byte{3}),
                                  .schema_version = schema::SchemaVersion::initial(),
                                  .result_key = key,
                                  .payload = body});
  ASSERT_TRUE(change.has_value());
  ASSERT_TRUE(
      client.receive(server_frame(MessageType::kSubscriptionChange, request_id, *change, 0U, 1U))
          .has_value());
  EXPECT_TRUE(client
                  .queue_subscription_acknowledgement(
                      request_id, SubscriptionAcknowledgement{.delivery_sequence = 1U})
                  .is_ok());
  const auto acknowledge_frame = decode_frame(take_pending(client));
  ASSERT_TRUE(acknowledge_frame.has_value());
  EXPECT_EQ(acknowledge_frame->header.message_type, MessageType::kSubscriptionAcknowledge);
  EXPECT_EQ(decode_subscription_acknowledgement(acknowledge_frame->payload)->delivery_sequence, 1U);
  ASSERT_TRUE(
      client
          .receive(server_frame(MessageType::kSubscriptionCheckpoint, request_id,
                                *encode_subscription_checkpoint(
                                    {.acknowledged_delivery_sequence = 1U, .resume_token = token}),
                                0U, 1U))
          .has_value());

  EXPECT_TRUE(client.queue_cancel(request_id).is_ok());
  EXPECT_EQ(client.in_flight_requests(), 1U);
  static_cast<void>(take_pending(client));
  EXPECT_TRUE(client
                  .receive(server_frame(
                      MessageType::kSubscriptionEnd, request_id,
                      *encode_subscription_end({.reason = SubscriptionEndReason::kCancelled,
                                                .safe_delivery_sequence = 1U,
                                                .resume_token = token}),
                      0U, 1U))
                  .has_value());
  EXPECT_EQ(client.in_flight_requests(), 0U);
}

TEST(NativeClientSessionTest, AcceptsProtocolOnePointTwoRaftSubscriptionChange) {
  NativeClientSession client =
      NativeClientSession::create(
          {.maximum_protocol_minor = 2U, .requested_feature_bits = kProtocolV1SubscriptionFeature})
          .value();
  ASSERT_TRUE(client.queue_handshake().is_ok());
  static_cast<void>(take_pending(client));
  ASSERT_TRUE(client
                  .receive(server_frame(
                      MessageType::kServerHello, 0U,
                      *encode_server_hello(
                          {.selected_minor = 2U, .feature_bits = kProtocolV1SubscriptionFeature})))
                  .has_value());
  const std::uint64_t request_id =
      client.queue_subscription(uuid(std::byte{1}), "SELECT 1").value();
  static_cast<void>(take_pending(client));
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  const std::array<QueryResultColumn, 1> columns{
      QueryResultColumn{.name = "value", .type = type, .nullable = false}};
  const auto empty_snapshot = encode_query_result_batch(0U, columns, {}).value();
  ASSERT_TRUE(client
                  .receive(server_frame(MessageType::kQueryResult, request_id, empty_snapshot,
                                        kFrameFlagEndStream, 2U))
                  .has_value());
  const std::array<std::byte, 1> token{std::byte{8}};
  ASSERT_TRUE(client
                  .receive(server_frame(MessageType::kSubscriptionReady, request_id,
                                        *encode_subscription_ready(token), 0U, 2U))
                  .has_value());

  SubscriptionSourceId source{};
  source.fill(std::byte{4});
  const std::array<std::byte, 1> key{std::byte{5}};
  const std::array<std::byte, 1> body{std::byte{6}};
  const auto change =
      encode_subscription_change({.operation = SubscriptionChangeOperation::kUpsert,
                                  .delivery_sequence = 1U,
                                  .tablet_id = identifier<schema::TabletId>(std::byte{2}),
                                  .source_kind = SubscriptionSourceKind::kRaft,
                                  .source_id = source,
                                  .source_sequence = 3U,
                                  .schema_id = identifier<schema::SchemaId>(std::byte{3}),
                                  .schema_version = schema::SchemaVersion::initial(),
                                  .result_key = key,
                                  .payload = body},
                                 {.protocol_minor = 2U});
  ASSERT_TRUE(change.has_value());
  EXPECT_TRUE(
      client.receive(server_frame(MessageType::kSubscriptionChange, request_id, *change, 0U, 2U))
          .has_value());
}

TEST(NativeClientSessionTest, RefusesSubscriptionWithoutNegotiation) {
  NativeClientSession client = NativeClientSession::create().value();
  complete_handshake(client);
  EXPECT_FALSE(client.queue_subscription(uuid(std::byte{1}), "SELECT 1").has_value());
}

} // namespace
} // namespace chronos::network
