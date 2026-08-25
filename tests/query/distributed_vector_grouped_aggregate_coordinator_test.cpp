#include "chronos/query/distributed_vector_grouped_aggregate_coordinator.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] schema::TabletId tablet(const std::uint8_t seed) {
  return schema::TabletId::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::vector<VectorGroupKeyDefinition> keys() {
  return {
      {.column_ordinal = 0U, .type = type(schema::LogicalTypeKind::kString), .nullable = false}};
}

[[nodiscard]] std::vector<VectorAggregateDefinition> definitions() {
  return {{.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
          {.operation = VectorAggregateOperation::kSum,
           .input = VectorAggregateInput{.column_ordinal = 1U,
                                         .type = type(schema::LogicalTypeKind::kInt64),
                                         .nullable = true}}};
}

[[nodiscard]] columnar::OwnedPhysicalColumn
int64_column(const std::span<const std::int64_t> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kInt64),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] std::vector<MergeableVectorAggregateState>
states(const std::span<const VectorAggregateDefinition> expected, const std::size_t row_count,
       const std::span<const std::int64_t> present_values) {
  std::vector<MergeableVectorAggregateState> result;
  result.push_back(MergeableVectorAggregateState::create(expected[0]).value());
  result.push_back(MergeableVectorAggregateState::create(expected[1]).value());
  for (std::size_t row = 0U; row < row_count; ++row)
    EXPECT_TRUE(result[0].accumulate_count_star().has_value());
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const auto column = int64_column(present_values);
  for (std::uint32_t row = 0U; row < column.row_count(); ++row)
    EXPECT_TRUE(result[1].accumulate_cell(column.cell(row).value(), resources).has_value());
  return result;
}

[[nodiscard]] DistributedVectorGroupedAggregateExchangeMessage
message(const common::Uuid query_id, const schema::TabletId tablet_id,
        const std::span<const VectorAggregateDefinition> expected, std::string key,
        const std::size_t rows, const std::span<const std::int64_t> values,
        const std::uint32_t ordinal, const std::uint32_t group_count) {
  return {{.query_id = query_id,
           .tablet_id = tablet_id,
           .sequence = static_cast<std::uint64_t>(ordinal) + 1U,
           .group_ordinal = ordinal,
           .group_count = group_count,
           .terminal = ordinal + 1U == group_count,
           .empty = false},
          {ScalarValue::text(type(schema::LogicalTypeKind::kString), std::move(key)).value()},
          states(expected, rows, values)};
}

[[nodiscard]] DistributedVectorGroupedAggregateExchangeMessage
empty_message(const common::Uuid query_id, const schema::TabletId tablet_id) {
  return {{.query_id = query_id,
           .tablet_id = tablet_id,
           .sequence = 1U,
           .group_ordinal = 0U,
           .group_count = 0U,
           .terminal = true,
           .empty = true},
          {},
          {}};
}

[[nodiscard]] ScalarValue cell(const VectorChunk& chunk, const std::size_t column) {
  const columnar::PhysicalColumnView* physical = chunk.column(column);
  EXPECT_NE(physical, nullptr);
  return ScalarValue::from_column_cell(
             physical->type(), chunk.cell({.column_ordinal = column, .selected_row = 0U}).value())
      .value();
}

TEST(DistributedVectorGroupedAggregateCoordinatorTest,
     ArbitratesExactRetriesClosesEveryTabletAndMergesInPlanOrder) {
  const common::Uuid query_id = uuid(1U);
  const schema::TabletId first = tablet(2U);
  const schema::TabletId second = tablet(3U);
  const schema::TabletId empty = tablet(4U);
  const auto expected_keys = keys();
  const auto expected_definitions = definitions();
  auto coordinator = DistributedVectorGroupedAggregateCoordinator::create(
      query_id, {first, second, empty}, expected_keys, expected_definitions);
  ASSERT_TRUE(coordinator.has_value()) << coordinator.error().to_string();
  EXPECT_EQ(coordinator->next().error().code(), common::StatusCode::kInvalidArgument);

  const std::array<std::int64_t, 2> first_a_values{1, 3};
  const std::array<std::int64_t, 1> first_b_values{5};
  const std::array<std::int64_t, 1> second_a_values{7};
  const std::array<std::int64_t, 1> second_c_values{9};
  auto first_a = message(query_id, first, expected_definitions, "A", 2U, first_a_values, 0U, 2U);
  auto first_b = message(query_id, first, expected_definitions, "B", 1U, first_b_values, 1U, 2U);
  auto second_a = message(query_id, second, expected_definitions, "A", 2U, second_a_values, 0U, 2U);
  auto second_c = message(query_id, second, expected_definitions, "C", 1U, second_c_values, 1U, 2U);
  auto empty_terminal = empty_message(query_id, empty);

  EXPECT_EQ(coordinator->accept(first_b).code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(coordinator->accept(second_a).is_ok());
  EXPECT_TRUE(coordinator->accept(second_c).is_ok());
  EXPECT_TRUE(coordinator->accept(first_a).is_ok());
  EXPECT_TRUE(coordinator->accept(first_a).is_ok());
  EXPECT_EQ(coordinator->finish().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(coordinator->accept(first_b).is_ok());
  EXPECT_TRUE(coordinator->accept(empty_terminal).is_ok());
  EXPECT_EQ(coordinator->retained_message_count(), 5U);
  EXPECT_GT(coordinator->retained_encoded_bytes(), 0U);

  const std::array<std::int64_t, 1> conflicting_values{99};
  auto conflicting =
      message(query_id, first, expected_definitions, "A", 1U, conflicting_values, 0U, 2U);
  EXPECT_EQ(coordinator->accept(conflicting).code(), common::StatusCode::kAlreadyExists);
  EXPECT_TRUE(coordinator->finish().is_ok());
  EXPECT_EQ(coordinator->group_count(), 3U);
  EXPECT_EQ(coordinator->accept(first_a).code(), common::StatusCode::kInvalidArgument);

  const std::array<std::string_view, 3> expected_group_keys{"A", "B", "C"};
  const std::array<std::int64_t, 3> expected_counts{4, 1, 1};
  const std::array<std::int64_t, 3> expected_sums{11, 5, 9};
  for (std::size_t group = 0U; group < expected_group_keys.size(); ++group) {
    auto output = coordinator->next();
    ASSERT_TRUE(output.has_value()) << output.error().to_string();
    ASSERT_EQ(output->kind(), PhysicalOperatorStepKind::kChunk);
    const VectorChunk& chunk = output->chunk()->chunk();
    EXPECT_EQ(std::get<std::string>(cell(chunk, 0U).storage()), expected_group_keys[group]);
    EXPECT_EQ(std::get<std::int64_t>(cell(chunk, 1U).storage()), expected_counts[group]);
    EXPECT_EQ(std::get<std::int64_t>(cell(chunk, 2U).storage()), expected_sums[group]);
  }
  EXPECT_EQ(coordinator->next()->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(coordinator->next()->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(DistributedVectorGroupedAggregateCoordinatorTest,
     RejectsAuthorityShapeDriftAndOwnsTheFirstIncompleteWorkerFailure) {
  const common::Uuid query_id = uuid(1U);
  const schema::TabletId first = tablet(2U);
  const schema::TabletId second = tablet(3U);
  const auto expected_keys = keys();
  const auto expected_definitions = definitions();
  EXPECT_EQ(DistributedVectorGroupedAggregateCoordinator::create({}, {first}, expected_keys,
                                                                 expected_definitions)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(DistributedVectorGroupedAggregateCoordinator::create(
                query_id, {first, first}, expected_keys, expected_definitions)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  std::vector<schema::TabletId> overallocated_tablets;
  overallocated_tablets.reserve(8192U);
  overallocated_tablets.push_back(first);
  std::vector<VectorGroupKeyDefinition> overallocated_keys = expected_keys;
  overallocated_keys.reserve(8192U);
  std::vector<VectorAggregateDefinition> overallocated_definitions = expected_definitions;
  overallocated_definitions.reserve(8192U);
  DistributedVectorGroupedAggregateCoordinatorLimits canonical_limits;
  canonical_limits.maximum_retained_configuration_bytes = 4096U;
  auto canonicalized = DistributedVectorGroupedAggregateCoordinator::create(
      query_id, std::move(overallocated_tablets), std::move(overallocated_keys),
      std::move(overallocated_definitions), canonical_limits);
  EXPECT_TRUE(canonicalized.has_value())
      << (canonicalized.has_value() ? "" : canonicalized.error().to_string());
  DistributedVectorGroupedAggregateCoordinatorLimits narrow;
  narrow.maximum_total_encoded_bytes = 1U;
  EXPECT_EQ(DistributedVectorGroupedAggregateCoordinator::create(query_id, {first}, expected_keys,
                                                                 expected_definitions, narrow)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto changed = DistributedVectorGroupedAggregateCoordinator::create(
      query_id, {first}, expected_keys, expected_definitions);
  ASSERT_TRUE(changed.has_value());
  const std::array<std::int64_t, 1> value{1};
  auto first_group = message(query_id, first, expected_definitions, "A", 1U, value, 0U, 2U);
  auto changed_shape = message(query_id, first, expected_definitions, "B", 1U, value, 1U, 3U);
  EXPECT_TRUE(changed->accept(first_group).is_ok());
  EXPECT_EQ(changed->accept(changed_shape).code(), common::StatusCode::kInvalidArgument);

  auto failed = DistributedVectorGroupedAggregateCoordinator::create(
      query_id, {first, second}, expected_keys, expected_definitions);
  ASSERT_TRUE(failed.has_value());
  const common::Status first_failure{common::StatusCode::kUnavailable, "first failure"};
  const common::Status second_failure{common::StatusCode::kInternal, "second failure"};
  EXPECT_TRUE(failed->worker_failed(first, first_failure).is_ok());
  EXPECT_EQ(failed->worker_failed(second, second_failure), first_failure);
  EXPECT_EQ(failed->finish(), first_failure);
  EXPECT_EQ(failed->next().error(), first_failure);

  auto empty = DistributedVectorGroupedAggregateCoordinator::create(
      query_id, {first}, expected_keys, expected_definitions);
  ASSERT_TRUE(empty.has_value());
  auto terminal = empty_message(query_id, first);
  EXPECT_TRUE(empty->accept(terminal).is_ok());
  EXPECT_TRUE(empty->worker_failed(first, second_failure).is_ok());
  EXPECT_TRUE(empty->finish().is_ok());
  EXPECT_EQ(empty->group_count(), 0U);
  EXPECT_EQ(empty->next()->kind(), PhysicalOperatorStepKind::kEnd);
}

} // namespace
} // namespace chronos::query
