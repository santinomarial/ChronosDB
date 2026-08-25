#include "chronos/query/distributed_vector_grouped_aggregate_coordinator.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

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
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet() {
  return schema::TabletId::from_uuid(uuid(2U)).value();
}

[[nodiscard]] schema::LogicalType string_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
}

[[nodiscard]] std::vector<VectorGroupKeyDefinition> keys() {
  return {{.column_ordinal = 0U, .type = string_type(), .nullable = false}};
}

[[nodiscard]] std::vector<VectorAggregateDefinition> definitions() {
  return {{.operation = VectorAggregateOperation::kMinimum,
           .input = VectorAggregateInput{
               .column_ordinal = 1U, .type = string_type(), .nullable = false}}};
}

void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
}

[[nodiscard]] columnar::OwnedPhysicalColumn string_column() {
  constexpr std::string_view kValue = "coordinator variable extremum larger than SSO";
  columnar::ColumnVectorBuffers buffers;
  append_u32(buffers.offsets, 0U);
  const auto bytes = std::as_bytes(std::span{kValue.data(), kValue.size()});
  buffers.values.assign(bytes.begin(), bytes.end());
  append_u32(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
  return columnar::OwnedPhysicalColumn::create(
             {.type = string_type(), .nullable = false, .row_count = 1U, .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] DistributedVectorGroupedAggregateExchangeMessage message() {
  const auto expected = definitions();
  auto state = MergeableVectorAggregateState::create(expected[0]).value();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const auto column = string_column();
  EXPECT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
  std::vector<MergeableVectorAggregateState> states;
  states.push_back(std::move(state));
  return {{.query_id = uuid(1U),
           .tablet_id = tablet(),
           .sequence = 1U,
           .group_ordinal = 0U,
           .group_count = 1U,
           .terminal = true,
           .empty = false},
          {ScalarValue::text(string_type(), "group-key-larger-than-SSO").value()},
          std::move(states)};
}

TEST(DistributedVectorGroupedAggregateCoordinatorAllocationFailureTest,
     ClassifiesConstructionRetentionMergeAndOutputAllocations) {
  const auto expected_keys = keys();
  const auto expected_definitions = definitions();
  auto partial = message();

  bool construction_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 96U; ++fail_after) {
    auto tablets = std::vector<schema::TabletId>{tablet()};
    auto key_values = expected_keys;
    auto definition_values = expected_definitions;
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorGroupedAggregateCoordinator::create(
          uuid(1U), std::move(tablets), std::move(key_values), std::move(definition_values));
    });
    if (result.has_value()) {
      construction_succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(construction_succeeded);

  bool retention_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 96U; ++fail_after) {
    auto coordinator = DistributedVectorGroupedAggregateCoordinator::create(
        uuid(1U), {tablet()}, expected_keys, expected_definitions);
    ASSERT_TRUE(coordinator.has_value());
    const common::Status accepted =
        run_failure(fail_after, [&] { return coordinator->accept(partial); });
    if (accepted.is_ok()) {
      retention_succeeded = true;
      break;
    }
    EXPECT_EQ(accepted.code(), common::StatusCode::kResourceExhausted);
    ASSERT_TRUE(coordinator->accept(partial).is_ok());
    ASSERT_TRUE(coordinator->finish().is_ok());
    EXPECT_EQ(coordinator->next()->kind(), PhysicalOperatorStepKind::kChunk);
  }
  EXPECT_TRUE(retention_succeeded);

  bool finish_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 192U; ++fail_after) {
    auto coordinator = DistributedVectorGroupedAggregateCoordinator::create(
        uuid(1U), {tablet()}, expected_keys, expected_definitions);
    ASSERT_TRUE(coordinator.has_value());
    ASSERT_TRUE(coordinator->accept(partial).is_ok());
    const common::Status finished = run_failure(fail_after, [&] { return coordinator->finish(); });
    if (finished.is_ok()) {
      finish_succeeded = true;
      break;
    }
    EXPECT_EQ(finished.code(), common::StatusCode::kResourceExhausted);
    ASSERT_TRUE(coordinator->finish().is_ok());
    EXPECT_EQ(coordinator->group_count(), 1U);
  }
  EXPECT_TRUE(finish_succeeded);

  bool output_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 96U; ++fail_after) {
    auto coordinator = DistributedVectorGroupedAggregateCoordinator::create(
        uuid(1U), {tablet()}, expected_keys, expected_definitions);
    ASSERT_TRUE(coordinator.has_value());
    ASSERT_TRUE(coordinator->accept(partial).is_ok());
    ASSERT_TRUE(coordinator->finish().is_ok());
    auto output = run_failure(fail_after, [&] { return coordinator->next(); });
    if (output.has_value()) {
      output_succeeded = true;
      break;
    }
    EXPECT_EQ(output.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(coordinator->next().error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(coordinator->group_count(), 0U);
  }
  EXPECT_TRUE(output_succeeded);
}

} // namespace
} // namespace chronos::query
