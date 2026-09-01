#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_retry.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet() {
  return schema::TabletId::from_uuid(uuid(2U)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleAuthority authority() {
  return DistributedVectorGroupedAggregateShuffleAuthority::create(
             uuid(1U), {{.tablet_id = tablet(), .node_id = 2U}},
             {{.partition_id = 0U, .node_id = 3U}}, keys(), aggregates())
      .value();
}

[[nodiscard]] DistributedVectorGroupedAggregateShuffleEdge edge() {
  return {.tablet_id = tablet(),
          .partition_id = 0U,
          .source_node_id = 2U,
          .target_node_id = 3U,
          .hash_version = kDistributedVectorGroupedAggregateShuffleHashVersionV1};
}

[[nodiscard]] std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage>
messages() {
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> result;
  for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
    EXPECT_TRUE(state.accumulate_count_star().has_value());
    std::vector<query::ScalarValue> values;
    values.push_back(query::ScalarValue::text(string_type(), ordinal == 0U ? "east-larger-than-SSO"
                                                                           : "west-larger-than-SSO")
                         .value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(std::move(state));
    query::DistributedVectorGroupedAggregateExchangeMessage message{
        {.query_id = uuid(1U),
         .tablet_id = tablet(),
         .sequence = ordinal + 1U,
         .group_ordinal = static_cast<std::uint32_t>(ordinal),
         .group_count = 2U,
         .terminal = ordinal == 1U,
         .empty = false},
        std::move(values),
        std::move(states)};
    result.push_back(query::encode_distributed_vector_grouped_aggregate_exchange_message(
                         message, keys(), aggregates())
                         .value());
  }
  return result;
}

[[nodiscard]] std::vector<std::byte>
drain(DistributedVectorGroupedAggregateShuffleStreamSender& stream) {
  std::vector<std::byte> bytes;
  while (!stream.complete()) {
    const common::ByteView pending = stream.pending_write();
    bytes.insert(bytes.end(), pending.begin(), pending.end());
    EXPECT_TRUE(stream.consume_written(pending.size()).is_ok());
  }
  return bytes;
}

TEST(DistributedVectorGroupedAggregateShuffleRetryTest,
     ReconstructsByteIdenticalAttemptsWithCappedBackoffAndImmutableRoute) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  DistributedVectorGroupedAggregateShuffleRetryLimits limits;
  limits.retry = {.maximum_attempts = 3U,
                  .initial_backoff = std::chrono::milliseconds{10},
                  .maximum_backoff = std::chrono::milliseconds{20}};
  auto retry = DistributedVectorGroupedAggregateShuffleRetry::create(expected, edge(), messages(),
                                                                     resources, limits)
                   .value();
  const auto start = DistributedVectorGroupedAggregateShuffleRetry::TimePoint{};

  auto first = retry.begin_attempt(start);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  EXPECT_EQ(first->attempt_number, 1U);
  EXPECT_EQ(first->target_node_id, 3U);
  const std::vector<std::byte> first_bytes = drain(first->stream);
  EXPECT_EQ(retry.begin_attempt(start).error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(retry.record_attempt_failure(common::StatusCode::kIoError, start).is_ok());
  const auto first_retry_time = retry.next_attempt_not_before();
  if (!first_retry_time.has_value()) {
    FAIL() << "retryable failure produced no first retry deadline";
  }
  EXPECT_EQ(first_retry_time.value(), start + std::chrono::milliseconds{10});
  EXPECT_EQ(retry.begin_attempt(start + std::chrono::milliseconds{9}).error().code(),
            common::StatusCode::kUnavailable);

  auto second = retry.begin_attempt(start + std::chrono::milliseconds{10});
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_EQ(second->attempt_number, 2U);
  EXPECT_EQ(drain(second->stream), first_bytes);
  ASSERT_TRUE(retry
                  .record_attempt_failure(common::StatusCode::kResourceExhausted,
                                          start + std::chrono::milliseconds{10})
                  .is_ok());
  const auto second_retry_time = retry.next_attempt_not_before();
  if (!second_retry_time.has_value()) {
    FAIL() << "second retryable failure produced no retry deadline";
  }
  EXPECT_EQ(second_retry_time.value(), start + std::chrono::milliseconds{30});

  auto third = retry.begin_attempt(start + std::chrono::milliseconds{30});
  ASSERT_TRUE(third.has_value()) << third.error().to_string();
  EXPECT_EQ(drain(third->stream), first_bytes);
  ASSERT_TRUE(retry
                  .record_attempt_failure(common::StatusCode::kUnavailable,
                                          start + std::chrono::milliseconds{30})
                  .is_ok());
  EXPECT_EQ(retry.state(), DistributedVectorGroupedAggregateShuffleRetryState::kFailed);
  EXPECT_EQ(retry.attempts_started(), 3U);
  EXPECT_EQ(retry.begin_attempt(start).error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorGroupedAggregateShuffleRetryTest,
     AcknowledgmentIsTheOnlySuccessBoundaryAndPermanentFailuresDoNotRetry) {
  auto expected = authority();
  auto resources = query::QueryResourceContext::create(4U << 20U).value();
  auto succeeded =
      DistributedVectorGroupedAggregateShuffleRetry::create(expected, edge(), messages(), resources)
          .value();
  ASSERT_TRUE(succeeded.begin_attempt({}).has_value());
  ASSERT_TRUE(succeeded.record_acknowledged().is_ok());
  EXPECT_EQ(succeeded.state(), DistributedVectorGroupedAggregateShuffleRetryState::kSucceeded);
  const auto last_status = succeeded.last_status_code();
  if (!last_status.has_value()) {
    FAIL() << "acknowledged retry produced no terminal status";
  }
  EXPECT_EQ(last_status.value(), common::StatusCode::kOk);
  EXPECT_EQ(succeeded.record_acknowledged().code(), common::StatusCode::kInvalidArgument);

  auto failed =
      DistributedVectorGroupedAggregateShuffleRetry::create(expected, edge(), messages(), resources)
          .value();
  ASSERT_TRUE(failed.begin_attempt({}).has_value());
  ASSERT_TRUE(failed.record_attempt_failure(common::StatusCode::kUnauthenticated, {}).is_ok());
  EXPECT_EQ(failed.state(), DistributedVectorGroupedAggregateShuffleRetryState::kFailed);
  EXPECT_FALSE(failed.next_attempt_not_before().has_value());

  DistributedVectorGroupedAggregateShuffleRetryLimits invalid_limits;
  invalid_limits.retry.maximum_attempts = 0U;
  EXPECT_EQ(DistributedVectorGroupedAggregateShuffleRetry::create(expected, edge(), messages(),
                                                                  resources, invalid_limits)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cluster
