#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_collector.hpp"

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
             uuid(1U), {{tablet, 2U}}, {{0U, 3U}, {1U, 4U}}, {{0U, string_type(), false}},
             {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
      .value();
}

[[nodiscard]] query::DistributedVectorResultSchema result_schema() {
  return {.columns = {{"region", string_type(), false}}};
}

[[nodiscard]] std::vector<std::byte> batch(const std::string& value) {
  const std::array columns{network::QueryResultColumn{"region", string_type(), false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{value})}};
  return network::encode_query_result_batch(1U, columns, cells).value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleCompleteResultStream
complete(const DistributedVectorGroupedAggregateShuffleAuthority& expected,
         const query::DistributedVectorResultSchema& schema, const std::uint32_t partition_id,
         const std::string& value, const raft::NodeId coordinator_node_id = 9U) {
  const auto source = expected.destination_node(partition_id).value();
  std::vector<std::vector<std::byte>> batches;
  if (!value.empty())
    batches.push_back(batch(value));
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    expected, schema, partition_id, source, coordinator_node_id, batches)
                    .value();
  return {.query_id = expected.query_id(),
          .source_node_id = source,
          .target_node_id = coordinator_node_id,
          .partition_id = partition_id,
          .encoded_result_batches = std::move(batches),
          .frame_count = static_cast<std::uint32_t>(sender.frame_count()),
          .encoded_bytes = sender.encoded_bytes()};
}

TEST(DistributedVectorGroupedAggregateShuffleResultCollectorTest,
     AcceptsExactRetriesAndPublishesOnlyCompletePartitionOrder) {
  auto expected = authority();
  auto schema = result_schema();
  auto collector =
      DistributedVectorGroupedAggregateShuffleResultCollector::create(expected, schema, 9U);
  ASSERT_TRUE(collector.has_value()) << collector.error().to_string();
  EXPECT_EQ(collector->coordinator_node_id(), 9U);
  EXPECT_FALSE(collector->ready());
  EXPECT_FALSE(collector->take_complete_streams().has_value());

  EXPECT_TRUE(collector->accept_stream(complete(expected, schema, 1U, "west")).is_ok());
  EXPECT_TRUE(collector->accept_stream(complete(expected, schema, 1U, "west")).is_ok());
  EXPECT_EQ(collector->accept_stream(complete(expected, schema, 1U, "conflict")).code(),
            common::StatusCode::kAlreadyExists);
  EXPECT_FALSE(collector->ready());
  auto metrics = collector->metrics();
  EXPECT_EQ(metrics.total_partitions, 2U);
  EXPECT_EQ(metrics.accepted_partitions, 1U);
  EXPECT_EQ(metrics.duplicate_streams, 1U);
  EXPECT_GT(metrics.retained_encoded_bytes, 0U);

  EXPECT_TRUE(collector->accept_stream(complete(expected, schema, 0U, "east")).is_ok());
  EXPECT_TRUE(collector->ready());
  EXPECT_EQ(collector->state(),
            DistributedVectorGroupedAggregateShuffleResultCollectorState::kComplete);
  auto streams = collector->take_complete_streams();
  ASSERT_TRUE(streams.has_value()) << streams.error().to_string();
  ASSERT_EQ(streams->size(), 2U);
  EXPECT_EQ((*streams)[0].partition_id, 0U);
  EXPECT_EQ((*streams)[0].source_node_id, 3U);
  EXPECT_EQ((*streams)[1].partition_id, 1U);
  EXPECT_EQ((*streams)[1].source_node_id, 4U);
  EXPECT_EQ(collector->state(),
            DistributedVectorGroupedAggregateShuffleResultCollectorState::kTaken);
  EXPECT_EQ(collector->metrics().retained_encoded_bytes, 0U);
  EXPECT_EQ(collector->accept_stream(complete(expected, schema, 0U, "east")).code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregateShuffleResultCollectorTest,
     RejectsIdentityExtentAndTotalByteExhaustionWithoutPartialAdmission) {
  auto expected = authority();
  auto schema = result_schema();
  auto sample = complete(expected, schema, 0U, "same");
  DistributedVectorGroupedAggregateShuffleResultCollectorLimits limits;
  limits.stream.maximum_encoded_bytes = sample.encoded_bytes;
  limits.maximum_total_encoded_bytes = sample.encoded_bytes;
  auto collector =
      DistributedVectorGroupedAggregateShuffleResultCollector::create(expected, schema, 9U, limits);
  ASSERT_TRUE(collector.has_value()) << collector.error().to_string();

  auto wrong_target = complete(expected, schema, 0U, "same");
  wrong_target.target_node_id = 8U;
  EXPECT_EQ(collector->accept_stream(std::move(wrong_target)).code(),
            common::StatusCode::kInvalidArgument);
  auto wrong_extent = complete(expected, schema, 0U, "same");
  ++wrong_extent.encoded_bytes;
  EXPECT_EQ(collector->accept_stream(std::move(wrong_extent)).code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(collector->accept_stream(std::move(sample)).is_ok());
  EXPECT_EQ(collector->accept_stream(complete(expected, schema, 1U, "same")).code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(collector->metrics().accepted_partitions, 1U);
  EXPECT_FALSE(collector->ready());
}

TEST(DistributedVectorGroupedAggregateShuffleResultCollectorTest,
     AcceptsAuthorityValidatedLocalAndRemotePartitionsWithoutASelfRoute) {
  auto expected = authority();
  auto schema = result_schema();
  auto collector =
      DistributedVectorGroupedAggregateShuffleResultCollector::create(expected, schema, 3U);
  ASSERT_TRUE(collector.has_value()) << collector.error().to_string();
  std::vector<std::vector<std::byte>> local_batches{batch("east")};
  auto local = create_distributed_vector_grouped_aggregate_shuffle_local_result_stream(
      expected, schema, 0U, 3U, std::move(local_batches));
  ASSERT_TRUE(local.has_value()) << local.error().to_string();
  EXPECT_TRUE(collector->accept_stream(std::move(*local)).is_ok());
  EXPECT_TRUE(collector->accept_stream(complete(expected, schema, 1U, "west", 3U)).is_ok());
  EXPECT_TRUE(collector->ready());
  auto streams = collector->take_complete_streams();
  ASSERT_TRUE(streams.has_value()) << streams.error().to_string();
  ASSERT_EQ(streams->size(), 2U);
  EXPECT_EQ((*streams)[0].source_node_id, 3U);
  EXPECT_EQ((*streams)[0].target_node_id, 3U);
  EXPECT_EQ((*streams)[1].source_node_id, 4U);
  EXPECT_EQ((*streams)[1].target_node_id, 3U);
}

} // namespace
} // namespace chronos::cluster
