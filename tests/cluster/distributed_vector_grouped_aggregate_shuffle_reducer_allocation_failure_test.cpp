#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_reducer.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
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

[[nodiscard]] DistributedVectorGroupedAggregateShuffleCompleteStream
stream(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       const schema::TabletId& tablet, const schema::LogicalType& type,
       const std::vector<query::VectorGroupKeyDefinition>& keys,
       const std::vector<query::VectorAggregateDefinition>& aggregates) {
  std::vector<query::DistributedVectorGroupedAggregateExchangeMessage> messages;
  std::size_t encoded_bytes{};
  for (std::size_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    auto state = query::MergeableVectorAggregateState::create(aggregates.front()).value();
    EXPECT_TRUE(state.accumulate_count_star().has_value());
    std::vector<query::ScalarValue> values;
    values.push_back(query::ScalarValue::text(type, ordinal == 0U ? "allocation-east-key"
                                                                  : "allocation-west-key")
                         .value());
    std::vector<query::MergeableVectorAggregateState> states;
    states.push_back(std::move(state));
    messages.push_back({{.query_id = uuid(1U),
                         .tablet_id = tablet,
                         .sequence = ordinal + 1U,
                         .group_ordinal = static_cast<std::uint32_t>(ordinal),
                         .group_count = 2U,
                         .terminal = ordinal == 1U,
                         .empty = false},
                        std::move(values),
                        std::move(states)});
    auto nested = query::encode_distributed_vector_grouped_aggregate_exchange_message(
                      messages.back(), keys, aggregates)
                      .value();
    encoded_bytes += kDistributedVectorGroupedAggregateShuffleFrameV1HeaderSize +
                     nested.bytes().size() +
                     kDistributedVectorGroupedAggregateShuffleFrameV1TrailerSize;
  }
  return {.edge = {.tablet_id = tablet,
                   .partition_id = 0U,
                   .source_node_id = 2U,
                   .target_node_id = 3U,
                   .hash_version = authority.hash_version()},
          .messages = std::move(messages),
          .encoded_bytes = encoded_bytes};
}

TEST(DistributedVectorGroupedAggregateShuffleReducerAllocationFailureTest,
     ClassifiesConstructionAndRetryableStreamAdmissionAllocations) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const auto type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = type, .nullable = false}};
  const std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet, .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 3U}}, keys, aggregates)
                       .value();

  bool create_success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateShuffleReducer::create(authority, 0U, 3U);
    });
    if (result.has_value()) {
      create_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(create_success);

  auto complete = stream(authority, tablet, type, keys, aggregates);
  bool saw_failure{};
  bool accept_success{};
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    auto reducer =
        DistributedVectorGroupedAggregateShuffleReducer::create(authority, 0U, 3U).value();
    const common::Status status =
        run_failure(fail_after, [&] { return reducer.accept_stream(complete); });
    if (status.is_ok()) {
      accept_success = true;
      break;
    }
    saw_failure = true;
    EXPECT_EQ(status.code(), common::StatusCode::kResourceExhausted) << status.to_string();
    EXPECT_EQ(reducer.metrics().accepted_sources, 0U);
    EXPECT_TRUE(reducer.accept_stream(complete).is_ok());
    EXPECT_EQ(reducer.metrics().accepted_sources, 1U);
  }
  EXPECT_TRUE(saw_failure);
  EXPECT_TRUE(accept_success);
}

} // namespace
} // namespace chronos::cluster
