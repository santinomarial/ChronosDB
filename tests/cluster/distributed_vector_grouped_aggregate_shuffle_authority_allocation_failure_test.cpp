#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_authority.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
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

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  return Identifier::from_uuid(uuid(seed)).value();
}

[[nodiscard]] query::DistributedMutableVectorFragment fragment(const std::uint8_t tablet_seed,
                                                               const raft::NodeId node_id) {
  const auto int64 = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.query_id = uuid(9U),
          .database_id = id<manifest::DatabaseId>(50U),
          .table_id = id<schema::TableId>(51U),
          .tablet_id = id<schema::TabletId>(tablet_seed),
          .destination_schema_id = id<schema::SchemaId>(52U),
          .raft_group_id = uuid(53U),
          .serving_node = node_id,
          .applied_position = 10U,
          .observed_leader_commit_position = 10U,
          .placement_epoch = 8U,
          .read_policy = {.consistency = query::DistributedReadConsistency::kLeaderLinearizable},
          .linearizable_barrier = raft::ReadBarrier{2U, 3U, 10U},
          .destination_column_ordinals = {0U},
          .plan = {.mode = query::DistributedVectorPlanMode::kRows, .row_output_indices = {0U}},
          .result_schema = {.columns = {{"value", int64, false}}}};
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityAllocationFailureTest,
     ClassifiesEveryRetainedSourceIndexAllocation) {
  const auto string_type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    std::vector<DistributedVectorGroupedAggregateShuffleSource> sources{
        {.tablet_id = schema::TabletId::from_uuid(uuid(1U)).value(), .node_id = 10U},
        {.tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(), .node_id = 20U}};
    std::vector<DistributedVectorGroupedAggregateShuffleDestination> destinations{
        {.partition_id = 0U, .node_id = 30U}, {.partition_id = 1U, .node_id = 40U}};
    std::vector<query::VectorGroupKeyDefinition> keys{
        {.column_ordinal = 0U, .type = string_type, .nullable = false}};
    std::vector<query::VectorAggregateDefinition> aggregates{
        {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleAuthority::create(
          uuid(9U), std::move(sources), std::move(destinations), std::move(keys),
          std::move(aggregates));
    });
    if (result.has_value()) {
      EXPECT_EQ(result->sources().size(), 2U);
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);
}

TEST(DistributedVectorGroupedAggregateShuffleAuthorityAllocationFailureTest,
     ClassifiesEveryProofBoundFragmentDerivationAllocation) {
  const auto string_type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector fragments{fragment(1U, 40U), fragment(2U, 20U), fragment(3U, 40U)};
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = string_type, .nullable = false}};
  const std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleAuthority::create_from_mutable_fragments(
          fragments, keys, aggregates);
    });
    if (result.has_value()) {
      EXPECT_EQ(result->partition_count(), 2U);
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
