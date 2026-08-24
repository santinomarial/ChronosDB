#include "chronos/cluster/distributed_mutable_vector_query_transport.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

template <typename Id> [[nodiscard]] Id id(const std::uint8_t seed) {
  return Id::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {
              {"ts", schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false}}};
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment() {
  return {.query_id = uuid(1U),
          .database_id = id<manifest::DatabaseId>(2U),
          .table_id = id<schema::TableId>(3U),
          .tablet_id = id<schema::TabletId>(4U),
          .destination_schema_id = id<schema::SchemaId>(5U),
          .raft_group_id = uuid(6U),
          .serving_node = 2U,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 7U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{.term = 2U, .context = 3U, .read_index = 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = result_schema()};
}

[[nodiscard]] std::vector<std::byte> zero_row_batch() {
  const query::DistributedVectorResultSchema schema_value = result_schema();
  const std::array<network::QueryResultColumn, 1U> columns{
      network::QueryResultColumn{schema_value.columns[0].name, schema_value.columns[0].type,
                                 schema_value.columns[0].nullable}};
  return network::encode_query_result_batch(0U, columns, {}).value();
}

[[nodiscard]] DistributedVectorQueryResponseV2 success_response() {
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = id<schema::TabletId>(4U),
          .status_code = common::StatusCode::kOk,
          .payload =
              DistributedVectorResultExchangeMessage{.query_id = uuid(1U),
                                                     .tablet_id = id<schema::TabletId>(4U),
                                                     .sequence = 1U,
                                                     .terminal = true,
                                                     .encoded_result_batch = zero_row_batch()}};
}

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void refresh_request_checksums(std::vector<std::byte>& bytes) {
  const common::ByteView payload = common::ByteView{bytes}.subspan(
      kDistributedMutableVectorQueryRequestHeaderSize,
      bytes.size() - kDistributedMutableVectorQueryRequestHeaderSize -
          kDistributedMutableVectorQueryRequestTrailerSize);
  store_u32(bytes, 48U, common::crc32c(payload));
  store_u32(bytes, 76U, common::crc32c(common::ByteView{bytes}.first(76U)));
  store_u32(bytes, bytes.size() - 4U,
            common::crc32c(common::ByteView{bytes}.first(bytes.size() - 4U)));
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal_id,
                                      const raft::NodeId claimed_node_id) const override {
    ++calls;
    return principal_id == 91U && claimed_node_id == 1U;
  }

  mutable std::size_t calls{};
};

class Worker final : public DistributedMutableVectorQueryWorkerService {
public:
  common::Result<std::vector<DistributedVectorResultExchangeMessage>>
  execute(const query::DistributedMutableVectorFragment& received) override {
    ++calls;
    last = received;
    if (failure.has_value())
      return common::make_unexpected(*failure);
    return std::vector<DistributedVectorResultExchangeMessage>{*success_response().payload};
  }

  std::size_t calls{};
  std::optional<query::DistributedMutableVectorFragment> last;
  std::optional<common::Status> failure;
};

class HintProvider final : public DistributedQueryLeaderHintProvider {
public:
  common::Result<std::optional<DistributedQueryLeaderHint>>
  current_leader_hint(const schema::TabletId&, const raft::GroupId&) const override {
    ++calls;
    return DistributedQueryLeaderHint{3U, 9U};
  }

  mutable std::size_t calls{};
};

TEST(DistributedMutableVectorQueryTransportTest, RoundTripsDistinctChecksummedFragmentedRequests) {
  const DistributedMutableVectorQueryRequest request{1U, 2U, fragment()};
  EXPECT_EQ(encode_distributed_mutable_vector_query_request({1U, 3U, fragment()}).error().code(),
            common::StatusCode::kInvalidArgument);
  const auto encoded = encode_distributed_mutable_vector_query_request(request);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const auto decoded = decode_distributed_mutable_vector_query_request_exact(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, request);
  EXPECT_EQ(decode_distributed_vector_query_request_v2_exact(*encoded).error().code(),
            common::StatusCode::kCorruption);

  for (std::size_t split = 0U; split <= encoded->size(); ++split) {
    DistributedMutableVectorQueryRequestReader reader;
    const auto prefix = reader.consume(common::ByteView{*encoded}.first(split));
    ASSERT_TRUE(prefix.has_value()) << "split=" << split;
    const auto suffix = reader.consume(common::ByteView{*encoded}.subspan(split));
    ASSERT_TRUE(suffix.has_value()) << "split=" << split;
    ASSERT_TRUE(prefix->request.has_value() || suffix->request.has_value()) << "split=" << split;
    EXPECT_EQ(prefix->consumed_bytes + suffix->consumed_bytes, encoded->size());
  }

  std::vector<std::byte> future = *encoded;
  store_u16(future, 8U, 2U);
  refresh_request_checksums(future);
  EXPECT_EQ(decode_distributed_mutable_vector_query_request_exact(future).error().code(),
            common::StatusCode::kNotSupported);
  std::vector<std::byte> damaged = *encoded;
  damaged[kDistributedMutableVectorQueryRequestHeaderSize] ^= std::byte{1U};
  refresh_request_checksums(damaged);
  EXPECT_EQ(decode_distributed_mutable_vector_query_request_exact(damaged).error().code(),
            common::StatusCode::kCorruption);

  auto cursor = DistributedMutableVectorQueryRequestWriteCursor::create(request);
  ASSERT_TRUE(cursor.has_value());
  EXPECT_TRUE(std::ranges::equal(cursor->pending_write(), *encoded));
  ASSERT_TRUE(cursor->consume_written(31U).is_ok());
  EXPECT_EQ(cursor->consume_written(cursor->pending_write().size() + 1U).code(),
            common::StatusCode::kInvalidArgument);
  ASSERT_TRUE(cursor->consume_written(cursor->pending_write().size()).is_ok());
  EXPECT_TRUE(cursor->complete());
}

TEST(DistributedMutableVectorQueryReceiverTest,
     AuthenticatesBeforePublishingBoundedSchemaValidResponses) {
  Authorizer authorizer;
  Worker worker;
  HintProvider hints;
  auto receiver = DistributedMutableVectorQueryReceiver::create({.local_node_id = 2U,
                                                                 .authorizer = &authorizer,
                                                                 .worker = &worker,
                                                                 .leader_hint_provider = &hints});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();
  const auto request =
      encode_distributed_mutable_vector_query_request({1U, 2U, fragment()}).value();

  EXPECT_EQ(receiver->receive(request, {.authorized = false, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(receiver->receive(request, {.authorized = true, .principal_id = 92U}).error().code(),
            common::StatusCode::kUnauthenticated);
  auto remote_fragment = fragment();
  remote_fragment.serving_node = 3U;
  const auto misrouted =
      encode_distributed_mutable_vector_query_request({1U, 3U, std::move(remote_fragment)});
  ASSERT_TRUE(misrouted.has_value());
  EXPECT_EQ(receiver->receive(*misrouted, {.authorized = true, .principal_id = 91U}).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(worker.calls, 0U);

  const auto success = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(success.has_value()) << success.error().to_string();
  ASSERT_EQ(success->size(), 1U);
  const auto decoded =
      decode_distributed_vector_query_response_v2_exact(success->front(), result_schema());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->status_code, common::StatusCode::kOk);
  ASSERT_TRUE(worker.last.has_value());
  EXPECT_EQ(*worker.last, fragment());

  worker.failure = common::Status{common::StatusCode::kUnavailable, "leader changed"};
  const auto failed = receiver->receive(request, {.authorized = true, .principal_id = 91U});
  ASSERT_TRUE(failed.has_value());
  ASSERT_EQ(failed->size(), 1U);
  const auto decoded_failure =
      decode_distributed_vector_query_response_v2_exact(failed->front(), result_schema());
  ASSERT_TRUE(decoded_failure.has_value());
  EXPECT_EQ(decoded_failure->status_code, common::StatusCode::kUnavailable);
  EXPECT_EQ(decoded_failure->leader_hint, DistributedQueryLeaderHint(3U, 9U));
  EXPECT_EQ(hints.calls, 1U);
}

TEST(DistributedMutableVectorQuerySenderTest, RetainsOnlyCompleteCorrelatedTerminalResults) {
  auto sender = DistributedMutableVectorQuerySender::create(1U, fragment());
  ASSERT_TRUE(sender.has_value()) << sender.error().to_string();
  const auto now = DistributedMutableVectorQuerySender::TimePoint{};
  const auto attempt = sender->begin_attempt(now);
  ASSERT_TRUE(attempt.has_value()) << attempt.error().to_string();
  EXPECT_EQ(attempt->target_node_id, 2U);
  const auto decoded =
      decode_distributed_mutable_vector_query_request_exact(attempt->request_bytes);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->fragment, fragment());

  const std::array success{success_response()};
  auto wrong = success;
  wrong.front().query_id = uuid(99U);
  EXPECT_EQ(sender->accept_responses(wrong, now).code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kWaitingForResponse);
  EXPECT_TRUE(sender->accept_responses(success, now).is_ok());
  EXPECT_EQ(sender->state(), DistributedQuerySenderState::kSucceeded);
  ASSERT_TRUE(sender->result().has_value());
  EXPECT_EQ(sender->result()->size(), 1U);

  auto retrying = DistributedMutableVectorQuerySender::create(
      1U, fragment(),
      {.retry = {.maximum_attempts = 2U,
                 .initial_backoff = std::chrono::milliseconds{5},
                 .maximum_backoff = std::chrono::milliseconds{5}}});
  ASSERT_TRUE(retrying.has_value());
  ASSERT_TRUE(retrying->begin_attempt(now).has_value());
  const std::array unavailable_response{
      DistributedVectorQueryResponseV2{.source_node_id = 2U,
                                       .target_node_id = 1U,
                                       .query_id = uuid(1U),
                                       .tablet_id = id<schema::TabletId>(4U),
                                       .status_code = common::StatusCode::kUnavailable,
                                       .leader_hint = DistributedQueryLeaderHint{3U, 9U}}};
  ASSERT_TRUE(retrying->accept_responses(unavailable_response, now).is_ok());
  EXPECT_EQ(retrying->state(), DistributedQuerySenderState::kBackoff);
  EXPECT_EQ(retrying->suggested_leader(), DistributedQueryLeaderHint(3U, 9U));
  EXPECT_FALSE(retrying->begin_attempt(now).has_value());
  EXPECT_TRUE(retrying->begin_attempt(now + std::chrono::milliseconds{5}).has_value());
}

} // namespace
} // namespace chronos::cluster
