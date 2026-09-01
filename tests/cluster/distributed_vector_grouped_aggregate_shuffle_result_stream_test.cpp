#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{tablet, 2U}}, {{0U, 3U}}, {{0U, string_type(), false}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema schema_value() {
  return {.columns = {{"region", string_type(), false}}};
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

class Authorizer final : public ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    ++calls;
    return principal == 91U && node == 3U;
  }
  mutable std::size_t calls{};
};

TEST(DistributedVectorGroupedAggregateShuffleResultStreamTest,
     AuthenticatesAndPublishesOnlyCompleteContiguousPartition) {
  auto expected = authority();
  const auto schema = schema_value();
  const std::vector<std::vector<std::byte>> batches{batch("east-result"), batch("west-result")};
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, 0U, 3U, 9U, batches)
                    .value();
  EXPECT_EQ(sender.partition_id(), 0U);
  EXPECT_EQ(sender.source_node_id(), 3U);
  EXPECT_EQ(sender.coordinator_node_id(), 9U);
  Authorizer authorizer;
  auto receiver = DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
                      expected, schema, 9U, authorizer, {.authorized = true, .principal_id = 91U})
                      .value();
  EXPECT_EQ(receiver.take_complete_stream().error().code(), common::StatusCode::kInvalidArgument);
  while (!sender.complete()) {
    const auto pending = sender.pending_write();
    const std::size_t count = std::min<std::size_t>(7U, pending.size());
    auto read = receiver.consume(pending.first(count));
    ASSERT_TRUE(read.has_value()) << read.error().to_string();
    ASSERT_TRUE(sender.consume_written(count).is_ok());
  }
  ASSERT_TRUE(receiver.finish_input().is_ok());
  EXPECT_TRUE(receiver.complete());
  EXPECT_EQ(receiver.accepted_frames(), 2U);
  EXPECT_EQ(receiver.accepted_bytes(), sender.encoded_bytes());
  EXPECT_EQ(authorizer.calls, 1U);
  auto complete = receiver.take_complete_stream();
  ASSERT_TRUE(complete.has_value());
  EXPECT_EQ(complete->partition_id, 0U);
  EXPECT_EQ(complete->source_node_id, 3U);
  EXPECT_EQ(complete->target_node_id, 9U);
  EXPECT_EQ(complete->encoded_result_batches, batches);
  EXPECT_EQ(complete->frame_count, 2U);
}

TEST(DistributedVectorGroupedAggregateShuffleResultStreamTest,
     RejectsUnauthenticatedSequenceIncompleteAndTerminalSuffix) {
  auto expected = authority();
  const auto schema = schema_value();
  Authorizer authorizer;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(expected, schema,
                                                                                 9U, authorizer, {})
                .error()
                .code(),
            common::StatusCode::kUnauthenticated);
  DistributedVectorGroupedAggregateShuffleResultFrame second{.query_id = uuid(1U),
                                                             .source_node_id = 3U,
                                                             .target_node_id = 9U,
                                                             .partition_id = 0U,
                                                             .partition_count = 1U,
                                                             .hash_version =
                                                                 expected.hash_version(),
                                                             .sequence = 2U,
                                                             .terminal = true,
                                                             .encoded_result_batch = batch("late")};
  const auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_result_frame(second, expected, schema, 9U)
          .value();
  auto receiver = DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
                      expected, schema, 9U, authorizer, {.authorized = true, .principal_id = 91U})
                      .value();
  EXPECT_EQ(receiver.consume(encoded).error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(receiver.failed());

  const std::vector<std::vector<std::byte>> batches{batch("one")};
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, 0U, 3U, 9U, batches)
                    .value();
  auto incomplete = DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
                        expected, schema, 9U, authorizer, {.authorized = true, .principal_id = 91U})
                        .value();
  EXPECT_EQ(incomplete.finish_input().code(), common::StatusCode::kCorruption);
  auto suffix = DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
                    expected, schema, 9U, authorizer, {.authorized = true, .principal_id = 91U})
                    .value();
  std::vector<std::byte> bytes(sender.pending_write().begin(), sender.pending_write().end());
  bytes.push_back(std::byte{1U});
  EXPECT_EQ(suffix.consume(bytes).error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(suffix.accepted_frames(), 0U);
}

TEST(DistributedVectorGroupedAggregateShuffleResultStreamTest,
     CanonicalizesEmptyPartitionAndEnforcesWholeStreamBounds) {
  auto expected = authority();
  const auto schema = schema_value();
  const std::vector<std::vector<std::byte>> empty;
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
      expected, schema, 0U, 3U, 9U, empty);
  ASSERT_TRUE(sender.has_value());
  EXPECT_EQ(sender->frame_count(), 1U);
  const std::vector<std::vector<std::byte>> nonempty{batch("one"), batch("two")};
  auto larger = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, 0U, 3U, 9U, nonempty)
                    .value();
  DistributedVectorGroupedAggregateShuffleResultStreamLimits lower;
  lower.maximum_encoded_bytes = larger.encoded_bytes() - 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                expected, schema, 0U, 3U, 9U, nonempty, lower)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  Authorizer authorizer;
  auto receiver = DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
                      expected, schema, 9U, authorizer, {.authorized = true, .principal_id = 91U})
                      .value();
  ASSERT_TRUE(receiver.consume(sender->pending_write()).has_value());
  auto complete = receiver.take_complete_stream();
  ASSERT_TRUE(complete.has_value());
  EXPECT_TRUE(complete->encoded_result_batches.empty());
  EXPECT_EQ(complete->frame_count, 1U);
}

TEST(DistributedVectorGroupedAggregateShuffleResultStreamTest,
     MovedFromOwnersRemainSafeEmptyObjects) {
  auto expected = authority();
  const auto schema = schema_value();
  const std::vector<std::vector<std::byte>> batches{batch("one")};
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, 0U, 3U, 9U, batches)
                    .value();
  auto moved_sender = std::move(sender);
  EXPECT_FALSE(moved_sender.complete());

  Authorizer authorizer;
  auto receiver = DistributedVectorGroupedAggregateShuffleResultStreamReceiver::create(
                      expected, schema, 9U, authorizer, {.authorized = true, .principal_id = 91U})
                      .value();
  auto moved_receiver = std::move(receiver);
  EXPECT_FALSE(moved_receiver.failed());
}

} // namespace
} // namespace chronos::cluster
