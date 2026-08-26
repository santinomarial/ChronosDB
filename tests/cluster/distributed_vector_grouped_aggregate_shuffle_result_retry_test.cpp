#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_retry.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
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

[[nodiscard]] std::vector<std::byte>
drain(DistributedVectorGroupedAggregateShuffleResultStreamSender& stream) {
  std::vector<std::byte> bytes;
  while (!stream.complete()) {
    const common::ByteView pending = stream.pending_write();
    bytes.insert(bytes.end(), pending.begin(), pending.end());
    EXPECT_TRUE(stream.consume_written(pending.size()).is_ok());
  }
  return bytes;
}

TEST(DistributedVectorGroupedAggregateShuffleResultRetryTest,
     ReconstructsByteIdenticalAttemptsWithCappedBackoff) {
  auto expected = authority();
  const auto schema = schema_value();
  const std::vector<std::vector<std::byte>> batches{batch("east-larger-than-SSO"),
                                                    batch("west-larger-than-SSO")};
  DistributedVectorGroupedAggregateShuffleResultRetryLimits limits;
  limits.retry = {.maximum_attempts = 3U,
                  .initial_backoff = std::chrono::milliseconds{10},
                  .maximum_backoff = std::chrono::milliseconds{20}};
  auto retry =
      DistributedVectorGroupedAggregateShuffleResultRetry::create(
          expected, schema, {.partition_id = 0U, .source_node_id = 3U, .coordinator_node_id = 9U},
          batches, limits)
          .value();
  const auto start = DistributedVectorGroupedAggregateShuffleResultRetry::TimePoint{};
  auto first = retry.begin_attempt(start);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  EXPECT_EQ(first->attempt_number, 1U);
  EXPECT_EQ(first->target_node_id, 9U);
  const auto first_bytes = drain(first->stream);
  EXPECT_EQ(retry.begin_attempt(start).error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(retry.record_attempt_failure(common::StatusCode::kIoError, start).is_ok());
  EXPECT_EQ(*retry.next_attempt_not_before(), start + std::chrono::milliseconds{10});
  EXPECT_EQ(retry.begin_attempt(start + std::chrono::milliseconds{9}).error().code(),
            common::StatusCode::kUnavailable);

  auto second = retry.begin_attempt(start + std::chrono::milliseconds{10});
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(drain(second->stream), first_bytes);
  ASSERT_TRUE(retry
                  .record_attempt_failure(common::StatusCode::kResourceExhausted,
                                          start + std::chrono::milliseconds{10})
                  .is_ok());
  EXPECT_EQ(*retry.next_attempt_not_before(), start + std::chrono::milliseconds{30});
  auto third = retry.begin_attempt(start + std::chrono::milliseconds{30});
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(drain(third->stream), first_bytes);
  ASSERT_TRUE(retry
                  .record_attempt_failure(common::StatusCode::kUnavailable,
                                          start + std::chrono::milliseconds{30})
                  .is_ok());
  EXPECT_EQ(retry.state(), DistributedVectorGroupedAggregateShuffleResultRetryState::kFailed);
  EXPECT_EQ(retry.attempts_started(), 3U);
}

TEST(DistributedVectorGroupedAggregateShuffleResultRetryTest,
     ReceiptIsOnlySuccessAndPermanentFailureIsTerminal) {
  auto expected = authority();
  const auto schema = schema_value();
  const std::vector<std::vector<std::byte>> batches{batch("one")};
  auto succeeded =
      DistributedVectorGroupedAggregateShuffleResultRetry::create(
          expected, schema, {.partition_id = 0U, .source_node_id = 3U, .coordinator_node_id = 9U},
          batches)
          .value();
  ASSERT_TRUE(succeeded.begin_attempt({}).has_value());
  ASSERT_TRUE(succeeded.record_acknowledged().is_ok());
  EXPECT_EQ(succeeded.state(),
            DistributedVectorGroupedAggregateShuffleResultRetryState::kSucceeded);
  EXPECT_EQ(*succeeded.last_status_code(), common::StatusCode::kOk);

  auto failed = DistributedVectorGroupedAggregateShuffleResultRetry::create(
                    expected, schema,
                    {.partition_id = 0U, .source_node_id = 3U, .coordinator_node_id = 9U}, batches)
                    .value();
  ASSERT_TRUE(failed.begin_attempt({}).has_value());
  ASSERT_TRUE(failed.record_attempt_failure(common::StatusCode::kUnauthenticated, {}).is_ok());
  EXPECT_EQ(failed.state(), DistributedVectorGroupedAggregateShuffleResultRetryState::kFailed);
  EXPECT_FALSE(failed.next_attempt_not_before().has_value());
}

} // namespace
} // namespace chronos::cluster
