#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_execution.hpp"
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleCompleteStream
stream(const DistributedVectorGroupedAggregateShuffleAuthority& authority) {
  auto state = query::MergeableVectorAggregateState::create(aggregates().front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "allocation-result").value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<query::DistributedVectorGroupedAggregateExchangeMessage> messages;
  messages.push_back({{.query_id = uuid(1U),
                       .tablet_id = tablet(),
                       .sequence = 1U,
                       .group_ordinal = 0U,
                       .group_count = 1U,
                       .terminal = true,
                       .empty = false},
                      std::move(values),
                      std::move(states)});
  auto nested =
      query::encode_distributed_vector_grouped_aggregate_exchange_message(
          messages.front(), authority.key_definitions(), authority.aggregate_definitions())
          .value();
  return {.edge = {.tablet_id = tablet(),
                   .partition_id = 0U,
                   .source_node_id = 2U,
                   .target_node_id = 2U,
                   .hash_version = authority.hash_version()},
          .messages = std::move(messages),
          .encoded_bytes = kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
                           nested.bytes().size() +
                           kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize};
}

TEST(DistributedVectorGroupedAggregateShuffleResultExecutionAllocationFailureTest,
     ClassifiesEveryExclusiveDestinationGathererConstructionAllocation) {
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet(), .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 2U}}, keys(), aggregates())
                       .value();
  const auto complete = stream(authority);
  bool succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto destination = DistributedVectorGroupedAggregateShuffleDestinationExecution::start(
                           authority, {.local_node_id = 2U})
                           .value();
    ASSERT_TRUE(destination.accept_local_stream(complete).is_ok());
    std::vector<DistributedVectorGroupedAggregateShuffleDestinationExecution> destinations;
    destinations.push_back(std::move(destination));
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleResultExecution::create(
          authority, std::move(destinations));
    });
    if (result.has_value()) {
      succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted)
        << result.error().to_string();
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::cluster
