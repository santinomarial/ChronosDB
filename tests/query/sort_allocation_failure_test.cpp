#include "chronos/common/status.hpp"
#include "chronos/query/sort.hpp"
#include "chronos/schema/logical_type.hpp"
#include "support/failing_allocator.hpp"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
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

[[nodiscard]] AccountedVectorChunk input(const QueryResourceContext& resources) {
  constexpr std::uint32_t kRows = 17U;
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(static_cast<std::size_t>(kRows) * sizeof(std::int64_t));
  for (std::uint32_t row = 0U; row < kRows; ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(static_cast<std::int64_t>(kRows - row));
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[static_cast<std::size_t>(row) * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(
      columnar::OwnedPhysicalColumn::create(
          {.type = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(),
           .nullable = false,
           .row_count = kRows,
           .null_count = 0U},
          std::move(buffers))
          .value());
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(kRows).value()).value();
  return AccountedVectorChunk::create(std::move(chunk),
                                      resources.reserve(std::size_t{4U} * 1024U).value(), resources)
      .value();
}

TEST(SortAllocationFailureTest, CreationClassifiesEveryOwnedAllocationFailure) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 16U; ++fail_after) {
    std::unique_ptr<PhysicalOperator> source = std::make_unique<EmptySource>();
    std::vector<VectorSortKey> keys{{.column_ordinal = 0U}};
    std::size_t observed = 0U;
    auto sorted = run_with_allocation_failure(fail_after, observed, [&] {
      return SortOperator::create(std::move(source), std::move(keys));
    });
    EXPECT_GT(observed, 0U);
    if (sorted.has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(sorted.error().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(SortAllocationFailureTest, PullClassifiesEveryAllocationAndReleasesAllCredit) {
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
    auto sorted = SortOperator::create(std::make_unique<OneChunkSource>(input(resources)),
                                       std::vector<VectorSortKey>{{.column_ordinal = 0U}})
                      .value();
    std::size_t observed = 0U;
    auto step =
        run_with_allocation_failure(fail_after, observed, [&] { return sorted->next(resources); });
    EXPECT_GT(observed, 0U);
    if (step.has_value()) {
      reached_success = true;
      step = common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "drop sort output"});
      sorted.reset();
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
