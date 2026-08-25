#include "chronos/cluster/distributed_vector_grouped_aggregate_query_transport_v2.hpp"
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
    test::ScopedAllocationFailure failure{fail_after};
    try {
      result.emplace(operation());
    } catch (...) {
      failure.disable();
      throw;
    }
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] std::vector<query::VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U,
           .type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
           .nullable = false}};
}

[[nodiscard]] std::vector<query::VectorAggregateDefinition> aggregates() {
  return {{.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
}

[[nodiscard]] DistributedVectorGroupedAggregateQueryResponseV2 response() {
  const auto expected = aggregates();
  auto state = query::MergeableVectorAggregateState::create(expected.front()).value();
  EXPECT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(
                       schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
                       "a grouped response key larger than short string storage")
                       .value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {.source_node_id = 2U,
          .target_node_id = 1U,
          .query_id = uuid(1U),
          .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
          .status_code = common::StatusCode::kOk,
          .payload = query::DistributedVectorGroupedAggregateExchangeMessage{
              {.query_id = uuid(1U),
               .tablet_id = schema::TabletId::from_uuid(uuid(2U)).value(),
               .sequence = 1U,
               .group_ordinal = 0U,
               .group_count = 1U,
               .terminal = true,
               .empty = false},
              std::move(values),
              std::move(states)}};
}

TEST(DistributedVectorGroupedAggregateQueryTransportV2AllocationFailureTest,
     ClassifiesOwnedFrameAllocationsAndReleasesDecodedKeyCredit) {
  const auto expected_keys = keys();
  const auto expected_aggregates = aggregates();
  const auto value = response();
  bool encode_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_grouped_aggregate_query_response_v2(value, expected_keys,
                                                                           expected_aggregates);
    });
    if (result.has_value()) {
      encode_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ASSERT_TRUE(encode_success);
  const auto encoded = encode_distributed_vector_grouped_aggregate_query_response_v2(
      value, expected_keys, expected_aggregates);
  ASSERT_TRUE(encoded.has_value());

  bool decode_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    {
      auto result = run_failure(fail_after, [&] {
        return decode_distributed_vector_grouped_aggregate_query_response_v2_exact(
            *encoded, expected_keys, expected_aggregates, resources);
      });
      if (result.has_value())
        decode_success = true;
      else
        EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    if (decode_success)
      break;
  }
  EXPECT_TRUE(decode_success);

  bool reader_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto resources = query::QueryResourceContext::create(4U << 20U).value();
    {
      auto owned_keys = keys();
      auto owned_aggregates = aggregates();
      DistributedVectorGroupedAggregateQueryResponseV2Reader reader{
          std::move(owned_keys), std::move(owned_aggregates), resources};
      auto result = run_failure(fail_after, [&] { return reader.consume(*encoded); });
      if (result.has_value())
        reader_success = true;
      else
        EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
    if (reader_success)
      break;
  }
  EXPECT_TRUE(reader_success);
}

} // namespace
} // namespace chronos::cluster
