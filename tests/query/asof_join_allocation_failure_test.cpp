#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/asof_join.hpp"
#include "support/failing_allocator.hpp"

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
  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

class OneChunkSource final : public PhysicalOperator {
public:
  explicit OneChunkSource(AccountedVectorChunk chunk) : chunk_(std::move(chunk)) {}

  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (!chunk_.has_value())
      return PhysicalOperatorStep::end();
    AccountedVectorChunk output = std::move(*chunk_);
    chunk_.reset();
    return PhysicalOperatorStep::chunk(std::move(output));
  }

private:
  std::optional<AccountedVectorChunk> chunk_;
};

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

// Value and storage width are the conventional inputs to one fixed-width test column.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] columnar::OwnedPhysicalColumn fixed_column(const schema::LogicalTypeKind kind,
                                                         const std::uint64_t value,
                                                         const std::size_t width) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(width);
  for (std::size_t byte = 0U; byte < width; ++byte)
    buffers.values[byte] = static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind), .nullable = false, .row_count = 1U, .null_count = 0U},
             std::move(buffers))
      .value();
}
// NOLINTEND(bugprone-easily-swappable-parameters)

[[nodiscard]] columnar::OwnedPhysicalColumn string_column(const std::string_view value) {
  columnar::ColumnVectorBuffers buffers;
  buffers.offsets.resize(2U * sizeof(std::uint32_t));
  buffers.values.reserve(value.size());
  for (const char byte : value)
    buffers.values.push_back(static_cast<std::byte>(byte));
  const std::uint32_t end = static_cast<std::uint32_t>(value.size());
  for (std::size_t byte = 0U; byte < sizeof(end); ++byte)
    buffers.offsets[sizeof(end) + byte] = static_cast<std::byte>((end >> (byte * 8U)) & 0xffU);
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kString),
                                                .nullable = false,
                                                .row_count = 1U,
                                                .null_count = 0U},
                                               std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn uuid_column() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{1U};
  columnar::ColumnVectorBuffers buffers;
  buffers.values.assign(bytes.begin(), bytes.end());
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kUuid),
                                                .nullable = false,
                                                .row_count = 1U,
                                                .null_count = 0U},
                                               std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk accounted(std::vector<columnar::OwnedPhysicalColumn> columns,
                                             const QueryResourceContext& resources) {
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(1U).value()).value();
  const std::size_t charge = chunk.retained_buffer_bytes() + 1'024U;
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(charge).value(),
                                      resources)
      .value();
}

[[nodiscard]] AccountedVectorChunk left_input(const QueryResourceContext& resources) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64, 1U, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs, 10U, 8U));
  columns.push_back(string_column("left"));
  return accounted(std::move(columns), resources);
}

[[nodiscard]] AccountedVectorChunk right_input(const QueryResourceContext& resources) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64, 1U, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs, 9U, 8U));
  columns.push_back(string_column("right"));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64, 1U, 8U));
  columns.push_back(uuid_column());
  columns.push_back(fixed_column(schema::LogicalTypeKind::kUInt64, 1U, 8U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kUInt32, 0U, 4U));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kUInt8, 1U, 1U));
  return accounted(std::move(columns), resources);
}

[[nodiscard]] VectorAsofJoinDefinition definition() {
  return {
      .left_input_columns = {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
                             {.type = type(schema::LogicalTypeKind::kTimestampNs),
                              .nullable = false},
                             {.type = type(schema::LogicalTypeKind::kString), .nullable = false}},
      .right_input_columns = {{.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kTimestampNs),
                               .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kString), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kInt64), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kUuid), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kUInt64), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kUInt32), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kUInt8), .nullable = false}},
      .equality_keys = {{.left_column_ordinal = 0U, .right_column_ordinal = 0U}},
      .left_timestamp_column_ordinal = 1U,
      .right_timestamp_column_ordinal = 1U,
      .right_physical_ordering_key_ordinals = {3U},
      .right_row_version_first_column_ordinal = 4U,
      .left_output_column_ordinals = {0U, 1U, 2U},
      .right_output_column_ordinals = {0U, 1U, 2U, 3U, 4U, 5U, 6U, 7U},
      .left_outer = true};
}

TEST(AsofJoinAllocationFailureTest, CreationClassifiesEveryOwnedAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    std::unique_ptr<PhysicalOperator> left = std::make_unique<EmptySource>();
    std::unique_ptr<PhysicalOperator> right = std::make_unique<EmptySource>();
    VectorAsofJoinDefinition configured = definition();
    std::size_t observed = 0U;
    auto join = run_with_allocation_failure(fail_after, observed, [&] {
      return AsofJoinOperator::create(std::move(left), std::move(right), std::move(configured));
    });
    EXPECT_GT(observed, 0U);
    if (join.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(join.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(AsofJoinAllocationFailureTest, PullClassifiesEveryAllocationAndReleasesCredit) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
    QueryResourceContext resources = QueryResourceContext::create(16U << 20U).value();
    auto join = AsofJoinOperator::create(std::make_unique<OneChunkSource>(left_input(resources)),
                                         std::make_unique<OneChunkSource>(right_input(resources)),
                                         definition())
                    .value();
    std::size_t observed = 0U;
    auto step =
        run_with_allocation_failure(fail_after, observed, [&] { return join->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop ASOF output"});
      join.reset();
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
