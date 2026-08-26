#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_collected_result_execution.hpp"
#include "support/failing_allocator.hpp"

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

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

struct Fixture {
  DistributedVectorGroupedAggregateShuffleAuthority authority;
  query::DistributedVectorResultSchema schema;
  schema::LogicalType text_type;
  schema::LogicalType count_type;
};

[[nodiscard]] Fixture fixture() {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  auto text = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  auto count = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  auto authority =
      DistributedVectorGroupedAggregateShuffleAuthority::create(
          uuid(1U), {{.tablet_id = tablet, .node_id = 2U}}, {{.partition_id = 0U, .node_id = 3U}},
          {{.column_ordinal = 0U, .type = text, .nullable = false}},
          {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
          .value();
  return {std::move(authority),
          {.columns = {{"region", text, false}, {"count", count, false}}},
          text,
          count};
}

[[nodiscard]] std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream>
streams(const Fixture& value) {
  const std::string text{"allocation-execution-result-larger-than-SSO"};
  const std::array<std::byte, 8U> count{std::byte{1U}};
  const std::array columns{network::QueryResultColumn{"region", value.text_type, false},
                           network::QueryResultColumn{"count", value.count_type, false}};
  const std::array cells{network::QueryResultCell{.value = std::as_bytes(std::span{text})},
                         network::QueryResultCell{.value = count}};
  std::vector<std::vector<std::byte>> batches;
  batches.push_back(network::encode_query_result_batch(1U, columns, cells).value());
  auto sender = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
                    value.authority, value.schema, 0U, 3U, 9U, batches)
                    .value();
  std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> result;
  result.push_back({.query_id = value.authority.query_id(),
                    .source_node_id = 3U,
                    .target_node_id = 9U,
                    .partition_id = 0U,
                    .encoded_result_batches = std::move(batches),
                    .frame_count = static_cast<std::uint32_t>(sender.frame_count()),
                    .encoded_bytes = sender.encoded_bytes()});
  return result;
}

TEST(DistributedVectorGroupedAggregateShuffleCollectedResultExecutionAllocationFailureTest,
     ClassifiesConstructionAndMaterializationAllocations) {
  auto value = fixture();
  bool saw_create_failure{};
  std::optional<DistributedVectorGroupedAggregateShuffleCollectedResultExecution> execution;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto input = streams(value);
    auto created = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
          value.authority, value.schema, std::move(input));
    });
    if (!created.has_value()) {
      saw_create_failure = true;
      EXPECT_EQ(created.error().code(), common::StatusCode::kResourceExhausted);
      continue;
    }
    execution.emplace(std::move(*created));
    break;
  }
  EXPECT_TRUE(saw_create_failure);
  ASSERT_TRUE(execution.has_value());

  bool saw_next_failure{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto value_for_attempt = fixture();
    auto attempt =
        DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
            value_for_attempt.authority, value_for_attempt.schema, streams(value_for_attempt))
            .value();
    auto step = run_failure(fail_after, [&] { return attempt.next(); });
    if (!step.has_value()) {
      saw_next_failure = true;
      EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
      continue;
    }
    EXPECT_EQ(step->kind(), query::PhysicalOperatorStepKind::kChunk);
    break;
  }
  EXPECT_TRUE(saw_next_failure);
}

} // namespace
} // namespace chronos::cluster
