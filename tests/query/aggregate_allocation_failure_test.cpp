#include "chronos/common/status.hpp"
#include "chronos/query/aggregate.hpp"
#include "chronos/schema/logical_type.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

class EmptySource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

class OneChunkSource final : public PhysicalOperator {
public:
  explicit OneChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk chunk = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(chunk));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

void append_u32(std::vector<std::byte>& output, const std::uint32_t value) {
  for (std::size_t byte = 0U; byte < sizeof(value); ++byte)
    output.push_back(static_cast<std::byte>((value >> (byte * 8U)) & 0xffU));
}

[[nodiscard]] AccountedVectorChunk grouped_input(const QueryResourceContext& resources) {
  constexpr std::array<std::string_view, 2> kKeys{"alpha", "beta"};
  columnar::ColumnVectorBuffers buffers;
  append_u32(buffers.offsets, 0U);
  for (const std::string_view key : kKeys) {
    for (const char byte : key)
      buffers.values.push_back(static_cast<std::byte>(byte));
    append_u32(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(
      columnar::OwnedPhysicalColumn::create(
          {.type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
           .nullable = false,
           .row_count = 2U,
           .null_count = 0U},
          std::move(buffers))
          .value());
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(2U).value()).value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

[[nodiscard]] AccountedVectorChunk variable_extremum_input(const QueryResourceContext& resources) {
  constexpr std::array<std::string_view, 3> kValues{"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
                                                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                                    "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"};
  columnar::ColumnVectorBuffers buffers;
  append_u32(buffers.offsets, 0U);
  for (const std::string_view value : kValues) {
    for (const char byte : value)
      buffers.values.push_back(static_cast<std::byte>(byte));
    append_u32(buffers.offsets, static_cast<std::uint32_t>(buffers.values.size()));
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(
      columnar::OwnedPhysicalColumn::create(
          {.type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
           .nullable = false,
           .row_count = static_cast<std::uint32_t>(kValues.size()),
           .null_count = 0U},
          std::move(buffers))
          .value());
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(kValues.size()).value()).value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

[[nodiscard]] AccountedVectorChunk grouped_extremum_input(const QueryResourceContext& resources) {
  constexpr std::array<std::int64_t, 3> kKeys{1, 1, 2};
  constexpr std::array<std::string_view, 3> kValues{"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
                                                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                                    "mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"};
  columnar::ColumnVectorBuffers key_buffers;
  key_buffers.values.resize(kKeys.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < kKeys.size(); ++row) {
    const std::uint64_t bits = static_cast<std::uint64_t>(kKeys[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      key_buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  columnar::ColumnVectorBuffers value_buffers;
  append_u32(value_buffers.offsets, 0U);
  for (const std::string_view value : kValues) {
    for (const char byte : value)
      value_buffers.values.push_back(static_cast<std::byte>(byte));
    append_u32(value_buffers.offsets, static_cast<std::uint32_t>(value_buffers.values.size()));
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(
      columnar::OwnedPhysicalColumn::create(
          {.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
           .nullable = false,
           .row_count = static_cast<std::uint32_t>(kKeys.size()),
           .null_count = 0U},
          std::move(key_buffers))
          .value());
  columns.push_back(
      columnar::OwnedPhysicalColumn::create(
          {.type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
           .nullable = false,
           .row_count = static_cast<std::uint32_t>(kValues.size()),
           .null_count = 0U},
          std::move(value_buffers))
          .value());
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(kKeys.size()).value()).value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

[[nodiscard]] schema::LogicalType int64_type() {
  return schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
}

[[nodiscard]] std::vector<VectorAggregateDefinition> definitions() {
  return {
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt},
      {.operation = VectorAggregateOperation::kSum,
       .input = VectorAggregateInput{.column_ordinal = 0U, .type = int64_type(), .nullable = true}},
      {.operation = VectorAggregateOperation::kAverage,
       .input = VectorAggregateInput{.column_ordinal = 0U, .type = int64_type(), .nullable = true}},
  };
}

TEST(UngroupedAggregateAllocationFailureTest, CreationClassifiesEveryOwnedAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::unique_ptr<PhysicalOperator> input = std::make_unique<EmptySource>();
    std::vector<VectorAggregateDefinition> configured = definitions();
    std::size_t observed = 0U;
    auto aggregate = run_with_allocation_failure(fail_after, observed, [&] {
      return UngroupedAggregateOperator::create(std::move(input), configured);
    });
    EXPECT_GT(observed, 0U);
    if (aggregate.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(aggregate.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(UngroupedAggregateAllocationFailureTest,
     PullClassifiesEveryOutputAllocationFailureAndReleasesCredit) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto aggregate =
        UngroupedAggregateOperator::create(std::make_unique<EmptySource>(), definitions()).value();
    std::size_t observed = 0U;
    auto step = run_with_allocation_failure(fail_after, observed,
                                            [&] { return aggregate->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop aggregate output step"});
      aggregate.reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(UngroupedAggregateAllocationFailureTest,
     PullClassifiesEveryVariableExtremumAllocationFailureAndReleasesCredit) {
  const schema::LogicalType string =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<VectorAggregateDefinition> extrema{
      {.operation = VectorAggregateOperation::kMinimum,
       .input = VectorAggregateInput{.column_ordinal = 0U, .type = string, .nullable = false}},
      {.operation = VectorAggregateOperation::kMaximum,
       .input = VectorAggregateInput{.column_ordinal = 0U, .type = string, .nullable = false}}};
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
    auto aggregate =
        UngroupedAggregateOperator::create(
            std::make_unique<OneChunkSource>(variable_extremum_input(resources)), extrema)
            .value();
    std::size_t observed = 0U;
    auto step = run_with_allocation_failure(fail_after, observed,
                                            [&] { return aggregate->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop aggregate output step"});
      aggregate.reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(GroupedAggregateAllocationFailureTest, CreationClassifiesEveryOwnedAllocationFailure) {
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U,
       .type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
       .nullable = false}};
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 32U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::unique_ptr<PhysicalOperator> input = std::make_unique<EmptySource>();
    std::vector<VectorAggregateDefinition> configured = definitions();
    std::size_t observed = 0U;
    auto grouped = run_with_allocation_failure(fail_after, observed, [&] {
      return GroupedAggregateOperator::create(std::move(input), keys, configured);
    });
    EXPECT_GT(observed, 0U);
    if (grouped.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(grouped.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(GroupedAggregateAllocationFailureTest,
     PullClassifiesEveryStateAndOutputAllocationFailureAndReleasesCredit) {
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U,
       .type = schema::LogicalType::create(schema::LogicalTypeKind::kString).value(),
       .nullable = false}};
  const std::vector<VectorAggregateDefinition> counts{
      {.operation = VectorAggregateOperation::kCountStar, .input = std::nullopt}};
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
    auto grouped = GroupedAggregateOperator::create(
                       std::make_unique<OneChunkSource>(grouped_input(resources)), keys, counts)
                       .value();
    std::size_t observed = 0U;
    auto step =
        run_with_allocation_failure(fail_after, observed, [&] { return grouped->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop grouped output step"});
      grouped.reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

TEST(GroupedAggregateAllocationFailureTest,
     PullClassifiesEveryVariableExtremumAllocationFailureAndReleasesCredit) {
  const schema::LogicalType string =
      schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const std::vector<VectorGroupKeyDefinition> keys{
      {.column_ordinal = 0U, .type = int64_type(), .nullable = false}};
  const std::vector<VectorAggregateDefinition> extrema{
      {.operation = VectorAggregateOperation::kMinimum,
       .input = VectorAggregateInput{.column_ordinal = 1U, .type = string, .nullable = false}},
      {.operation = VectorAggregateOperation::kMaximum,
       .input = VectorAggregateInput{.column_ordinal = 1U, .type = string, .nullable = false}}};
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
    auto grouped =
        GroupedAggregateOperator::create(
            std::make_unique<OneChunkSource>(grouped_extremum_input(resources)), keys, extrema)
            .value();
    std::size_t observed = 0U;
    auto step =
        run_with_allocation_failure(fail_after, observed, [&] { return grouped->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop grouped output step"});
      grouped.reset();
      EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
      break;
    }
    EXPECT_EQ(step.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_TRUE(resources.is_cancelled());
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
