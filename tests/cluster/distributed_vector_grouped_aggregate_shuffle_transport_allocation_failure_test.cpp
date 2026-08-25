#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_transport.hpp"
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

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

TEST(DistributedVectorGroupedAggregateShuffleTransportAllocationFailureTest,
     ClassifiesEncodeDecodeAndReaderAllocationsWithoutLeakingQueryCredit) {
  const auto tablet = schema::TabletId::from_uuid(uuid(2U)).value();
  const std::vector<query::VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = string_type(), .nullable = false}};
  const std::vector<query::VectorAggregateDefinition> aggregates{
      {.operation = query::VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  auto authority = DistributedVectorGroupedAggregateShuffleAuthority::create(
                       uuid(1U), {{.tablet_id = tablet, .node_id = 2U}},
                       {{.partition_id = 0U, .node_id = 3U}}, keys, aggregates)
                       .value();
  auto state = query::MergeableVectorAggregateState::create(aggregates[0]).value();
  ASSERT_TRUE(state.accumulate_count_star().has_value());
  std::vector<query::MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  std::vector<query::ScalarValue> values;
  values.push_back(query::ScalarValue::text(string_type(), "shuffle-key-larger-than-SSO").value());
  DistributedVectorGroupedAggregateShuffleFrameV1 frame{
      .query_id = uuid(1U),
      .edge = {.tablet_id = tablet,
               .partition_id = 0U,
               .source_node_id = 2U,
               .target_node_id = 3U,
               .hash_version = authority.hash_version()},
      .partition_count = 1U,
      .payload = {{.query_id = uuid(1U),
                   .tablet_id = tablet,
                   .sequence = 1U,
                   .group_ordinal = 0U,
                   .group_count = 1U,
                   .terminal = true,
                   .empty = false},
                  std::move(values),
                  std::move(states)}};

  bool encoded_success{};
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(frame, authority);
    });
    if (result.has_value()) {
      encoded_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(encoded_success);
  const auto encoded =
      encode_distributed_vector_grouped_aggregate_shuffle_frame_v1(frame, authority).value();

  query::QueryResourceContext resources = query::QueryResourceContext::create(4U << 20U).value();
  bool decoded_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    {
      auto result = run_failure(fail_after, [&] {
        return decode_distributed_vector_grouped_aggregate_shuffle_frame_v1_exact(
            encoded, authority, resources);
      });
      if (result.has_value()) {
        decoded_success = true;
        break;
      }
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(decoded_success);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  bool reader_success{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    DistributedVectorGroupedAggregateShuffleFrameV1Reader reader{authority, resources};
    {
      auto result = run_failure(fail_after, [&] { return reader.consume(encoded); });
      if (result.has_value()) {
        ASSERT_TRUE(result->frame.has_value());
        reader_success = true;
        break;
      }
      EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
      EXPECT_TRUE(reader.failed());
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reader_success);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::cluster
