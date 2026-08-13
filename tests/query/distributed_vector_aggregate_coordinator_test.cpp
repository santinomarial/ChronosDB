#include "chronos/query/distributed_vector_aggregate_coordinator.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <span>
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

[[nodiscard]] std::vector<VectorAggregateDefinition> definitions() {
  return {{.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
          {.operation = VectorAggregateOperation::kAverage,
           .input = VectorAggregateInput{.column_ordinal = 0U,
                                         .type = type(schema::LogicalTypeKind::kFloat64),
                                         .nullable = true}}};
}

[[nodiscard]] DistributedVectorResultSchema result_schema() {
  return {.columns = {{"count", type(schema::LogicalTypeKind::kInt64), false},
                      {"average", type(schema::LogicalTypeKind::kFloat64), true}}};
}

[[nodiscard]] columnar::OwnedPhysicalColumn float64_column(const std::span<const double> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(values.size() * sizeof(double));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kFloat64),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] MergeableVectorAggregateState count_state(const VectorAggregateDefinition& definition,
                                                        const std::size_t count) {
  auto state = MergeableVectorAggregateState::create(definition).value();
  for (std::size_t index = 0U; index < count; ++index)
    EXPECT_TRUE(state.accumulate_count_star().has_value());
  return state;
}

[[nodiscard]] MergeableVectorAggregateState
average_state(const VectorAggregateDefinition& definition, const std::span<const double> values,
              const QueryResourceContext& resources) {
  auto state = MergeableVectorAggregateState::create(definition).value();
  const auto column = float64_column(values);
  for (std::uint32_t row = 0U; row < column.row_count(); ++row)
    EXPECT_TRUE(state.accumulate_cell(column.cell(row).value(), resources).has_value());
  return state;
}

[[nodiscard]] DistributedVectorAggregateExchangeMessage
message(const common::Uuid query_id, const schema::TabletId tablet_id, const std::size_t ordinal,
        MergeableVectorAggregateState state) {
  return {{.query_id = query_id,
           .tablet_id = tablet_id,
           .sequence = ordinal + 1U,
           .aggregate_ordinal = static_cast<std::uint32_t>(ordinal),
           .terminal = ordinal == 1U},
          std::move(state)};
}

TEST(DistributedVectorAggregateCoordinatorV2Test,
     CoordinatesExactRetriesAndGloballyFinalizesSufficientState) {
  const common::Uuid query_id = uuid(1U);
  const schema::TabletId first = tablet(2U);
  const schema::TabletId second = tablet(3U);
  const auto expected = definitions();
  auto coordinator = DistributedVectorAggregateCoordinatorV2::create(query_id, {first, second},
                                                                     expected, result_schema());
  ASSERT_TRUE(coordinator.has_value());
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();

  const std::array first_values{2.0, 4.0};
  auto first_count = message(query_id, first, 0U, count_state(expected[0], 2U));
  auto first_average =
      message(query_id, first, 1U, average_state(expected[1], first_values, resources));
  const std::array second_values{10.0};
  auto second_count = message(query_id, second, 0U, count_state(expected[0], 1U));
  auto second_average =
      message(query_id, second, 1U, average_state(expected[1], second_values, resources));

  EXPECT_EQ(coordinator->accept(first_average).code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(coordinator->accept(second_count).is_ok());
  EXPECT_TRUE(coordinator->accept(first_count).is_ok());
  EXPECT_TRUE(coordinator->accept(first_count).is_ok());
  EXPECT_TRUE(coordinator->accept(first_average).is_ok());
  EXPECT_TRUE(coordinator->accept(second_average).is_ok());

  auto conflicting = message(query_id, first, 0U, count_state(expected[0], 1U));
  EXPECT_EQ(coordinator->accept(conflicting).code(), common::StatusCode::kAlreadyExists);
  auto result = std::move(*coordinator).finish();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->definitions, expected);
  ASSERT_EQ(result->values.size(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(result->values[0].storage()), 3);
  EXPECT_DOUBLE_EQ(std::get<double>(result->values[1].storage()), 16.0 / 3.0);
  EXPECT_GT(result->retained_encoded_bytes, 0U);
  EXPECT_EQ(result->result_schema, result_schema());
  EXPECT_EQ(std::move(*coordinator).finish().error().code(), common::StatusCode::kInvalidArgument);
}

TEST(DistributedVectorAggregateCoordinatorV2Test, BoundsAuthorityAndOwnsFirstFailure) {
  const common::Uuid query_id = uuid(1U);
  const schema::TabletId first = tablet(2U);
  const schema::TabletId second = tablet(3U);
  const auto expected = definitions();
  EXPECT_EQ(DistributedVectorAggregateCoordinatorV2::create({}, {first}, expected, result_schema())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(DistributedVectorAggregateCoordinatorV2::create(query_id, {first, first}, expected,
                                                            result_schema())
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto wrong_schema = result_schema();
  wrong_schema.columns[1].nullable = false;
  EXPECT_EQ(DistributedVectorAggregateCoordinatorV2::create(query_id, {first}, expected,
                                                            std::move(wrong_schema))
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  DistributedVectorAggregateCoordinatorLimitsV2 narrow;
  narrow.maximum_total_encoded_bytes = 1U;
  EXPECT_EQ(DistributedVectorAggregateCoordinatorV2::create(query_id, {first}, expected,
                                                            result_schema(), narrow)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto failed = DistributedVectorAggregateCoordinatorV2::create(query_id, {first, second}, expected,
                                                                result_schema());
  ASSERT_TRUE(failed.has_value());
  const common::Status first_failure{common::StatusCode::kUnavailable, "first failure"};
  const common::Status second_failure{common::StatusCode::kInternal, "second failure"};
  EXPECT_TRUE(failed->worker_failed(first, first_failure).is_ok());
  EXPECT_EQ(failed->worker_failed(second, second_failure), first_failure);
  EXPECT_EQ(std::move(*failed).finish().error(), first_failure);

  auto completed =
      DistributedVectorAggregateCoordinatorV2::create(query_id, {first}, expected, result_schema());
  ASSERT_TRUE(completed.has_value());
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const std::array values{5.0};
  auto count = message(query_id, first, 0U, count_state(expected[0], 1U));
  auto average = message(query_id, first, 1U, average_state(expected[1], values, resources));
  ASSERT_TRUE(completed->accept(count).is_ok());
  ASSERT_TRUE(completed->accept(average).is_ok());
  EXPECT_TRUE(completed->worker_failed(first, second_failure).is_ok());
  EXPECT_TRUE(std::move(*completed).finish().has_value());
}

} // namespace
} // namespace chronos::query
