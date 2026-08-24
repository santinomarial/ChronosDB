#include "chronos/query/distributed_vector_fragment_v2.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>

namespace chronos::query {
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
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] DistributedVectorFragmentDispatchV2 dispatch_v2() {
  const schema::LogicalType type =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.dispatch = {.query_id = uuid(1U),
                       .database_id = manifest::DatabaseId::from_uuid(uuid(2U)).value(),
                       .table_id = schema::TableId::from_uuid(uuid(3U)).value(),
                       .tablet_id = schema::TabletId::from_uuid(uuid(4U)).value(),
                       .destination_schema_id = schema::SchemaId::from_uuid(uuid(5U)).value(),
                       .raft_group_id = uuid(6U),
                       .snapshot_generation = 1U,
                       .serving_node = 7U,
                       .placement_epoch = 1U,
                       .read_policy = {.consistency = DistributedReadConsistency::kLocalEventual,
                                       .maximum_staleness_positions = std::nullopt},
                       .linearizable_barrier = std::nullopt,
                       .destination_column_ordinals = {0U},
                       .event_time_predicate = std::nullopt,
                       .plan = {.mode = DistributedVectorPlanMode::kRows,
                                .row_output_indices = {0U, 0U},
                                .visible_row_output_indices = {0U},
                                .group_key_input_indices = {},
                                .aggregates = {},
                                .order_keys = {},
                                .limit = std::nullopt}},
          .result_schema = {.columns = {{"value", type, false}, {"hidden", type, false}}}};
}

TEST(DistributedVectorFragmentV2AllocationFailureTest, ClassifiesEveryOwnedCodecAllocation) {
  const DistributedVectorFragmentDispatchV2 value = dispatch_v2();
  bool encoded_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(
        fail_after, [&] { return encode_distributed_vector_fragment_dispatch_v2(value); });
    if (result.has_value()) {
      encoded_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ASSERT_TRUE(encoded_success);

  const auto encoded = encode_distributed_vector_fragment_dispatch_v2(value);
  ASSERT_TRUE(encoded.has_value());
  bool decoded_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      return decode_distributed_vector_fragment_dispatch_v2_exact(encoded->bytes());
    });
    if (result.has_value()) {
      decoded_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ASSERT_TRUE(decoded_success);

  bool reader_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto result = run_failure(fail_after, [&] {
      DistributedVectorFragmentV2Reader reader;
      return reader.consume(encoded->bytes());
    });
    if (result.has_value()) {
      ASSERT_TRUE(result->dispatch.has_value());
      reader_success = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reader_success);
}

} // namespace
} // namespace chronos::query
