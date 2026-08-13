#include "chronos/query/distributed_vector_aggregate_coordinator.hpp"
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

[[nodiscard]] std::vector<VectorAggregateDefinition> definitions() {
  return {{.operation = VectorAggregateOperation::kMaximum,
           .input = VectorAggregateInput{
               .column_ordinal = 0U,
               .type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
               .nullable = false}}};
}

[[nodiscard]] DistributedVectorResultSchema result_schema() {
  return {
      .columns = {{"maximum", schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
                   true}}};
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
             {.type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
              .nullable = false,
              .row_count = 1U,
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] DistributedVectorAggregateExchangeMessage
message(const VectorAggregateDefinition& definition) {
  auto state = MergeableVectorAggregateState::create(definition).value();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  const auto column = string_column();
  EXPECT_TRUE(state.accumulate_cell(column.cell(0U).value(), resources).has_value());
  return {{.query_id = uuid(1U),
           .tablet_id = tablet(),
           .sequence = 1U,
           .aggregate_ordinal = 0U,
           .terminal = true},
          std::move(state)};
}

TEST(DistributedVectorAggregateCoordinatorV2AllocationFailureTest,
     ClassifiesConstructionRetentionAndRetryableFinalization) {
  const auto expected = definitions();
  auto partial = message(expected[0]);
  bool construction_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    std::vector<schema::TabletId> tablets{tablet()};
    auto definitions_value = expected;
    auto schema_value = result_schema();
    auto result = run_failure(fail_after, [&] {
      return DistributedVectorAggregateCoordinatorV2::create(
          uuid(1U), std::move(tablets), std::move(definitions_value), std::move(schema_value));
    });
    if (result.has_value()) {
      construction_succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(construction_succeeded);

  bool retention_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto coordinator = DistributedVectorAggregateCoordinatorV2::create(uuid(1U), {tablet()},
                                                                       expected, result_schema());
    ASSERT_TRUE(coordinator.has_value());
    const common::Status accepted =
        run_failure(fail_after, [&] { return coordinator->accept(partial); });
    if (accepted.is_ok()) {
      retention_succeeded = true;
      break;
    }
    EXPECT_EQ(accepted.code(), common::StatusCode::kResourceExhausted);
    ASSERT_TRUE(coordinator->accept(partial).is_ok());
    EXPECT_TRUE(std::move(*coordinator).finish().has_value());
  }
  EXPECT_TRUE(retention_succeeded);

  bool finish_succeeded{};
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    auto coordinator = DistributedVectorAggregateCoordinatorV2::create(uuid(1U), {tablet()},
                                                                       expected, result_schema());
    ASSERT_TRUE(coordinator.has_value());
    ASSERT_TRUE(coordinator->accept(partial).is_ok());
    auto result = run_failure(fail_after, [&] { return std::move(*coordinator).finish(); });
    if (result.has_value()) {
      finish_succeeded = true;
      break;
    }
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
    auto retried = std::move(*coordinator).finish();
    ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
  }
  EXPECT_TRUE(finish_succeeded);
}

} // namespace
} // namespace chronos::query
