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
                                                  const std::uint32_t flags = 0U) {
  return encode_frame({.message_type = type, .flags = flags, .request_id = id}, payload).value();
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

} // namespace
} // namespace chronos::network
