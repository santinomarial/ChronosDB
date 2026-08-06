#include "chronos/common/status.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

// -1 is SQL NULL, 0 is FALSE, and 1 is TRUE.
[[nodiscard]] columnar::OwnedPhysicalColumn bool_column(const std::span<const std::int8_t> values) {
  columnar::ColumnVectorBuffers buffers;
  const auto rows = static_cast<std::uint32_t>(values.size());
  buffers.validity.resize(columnar::bitmap_size(rows));
  buffers.values.resize(columnar::bitmap_size(rows));
  std::uint32_t null_count = 0U;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    if (values[row] < 0) {
      ++null_count;
      continue;
    }
    buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    if (values[row] > 0)
      buffers.values[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
  }
  return columnar::OwnedPhysicalColumn::create({.type = type(schema::LogicalTypeKind::kBool),
                                                .nullable = true,
                                                .row_count = rows,
                                                .null_count = null_count},
                                               std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
int64_column(const std::span<const std::int64_t> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kInt64),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk accounted_chunk(const QueryResourceContext& resources,
                                                   const std::span<const std::int64_t> values,
                                                   const std::span<const std::int8_t> predicates,
                                                   std::vector<std::uint32_t> selection) {
  QueryMemoryReservation reservation = resources.reserve(1'024U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column(values));
  columns.push_back(bool_column(predicates));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns),
                          VectorSelection::from_indices(static_cast<std::uint32_t>(values.size()),
                                                        std::move(selection))
                              .value())
          .value();
  return AccountedVectorChunk::create(std::move(chunk), std::move(reservation), resources).value();
}

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override {
    const common::Result<void> active = resources.check_cancelled();
    if (!active.has_value())
      return common::make_unexpected(active.error());
    if (next_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[next_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t next_{};
};

class FailingSource final : public PhysicalOperator {
public:
  [[nodiscard]] common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "injected physical source failure"});
  }
};

TEST(VectorSelectionFilterTest, CompactsExistingIndicesWithSqlWhereTruth) {
  const auto predicate = bool_column(std::vector<std::int8_t>{1, 0, -1, 1, 0});
  VectorSelection input = VectorSelection::from_indices(5U, {0U, 2U, 3U, 4U}).value();
  const std::size_t retained = input.retained_buffer_bytes();
  const auto output = VectorSelection::where_true(std::move(input), predicate);
  ASSERT_TRUE(output.has_value());
  const std::vector<std::uint32_t> expected{0U, 3U};
  EXPECT_TRUE(std::ranges::equal(output->indices(), expected));
  EXPECT_EQ(output->retained_buffer_bytes(), retained);
  EXPECT_FALSE(output->is_identity());
}

TEST(VectorSelectionFilterTest, ValidatesPredicateTypeAndPhysicalShape) {
  const auto integer = int64_column(std::vector<std::int64_t>{1, 2});
  EXPECT_EQ(VectorSelection::where_true(VectorSelection::all(2U).value(), integer).error().code(),
            common::StatusCode::kInvalidArgument);
  const auto predicate = bool_column(std::vector<std::int8_t>{1});
  EXPECT_EQ(VectorSelection::where_true(VectorSelection::all(2U).value(), predicate).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(AccountedVectorChunkTest, RequiresLiveCreditCoveringRetainedBuffers) {
  const auto resources = QueryResourceContext::create(4'096U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(bool_column(std::vector<std::int8_t>{1, 0}));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(2U).value()).value();
  const std::size_t retained = chunk.retained_buffer_bytes();
  EXPECT_FALSE(AccountedVectorChunk::create(std::move(chunk), {}, resources).has_value());

  std::vector<columnar::OwnedPhysicalColumn> short_columns;
  short_columns.push_back(bool_column(std::vector<std::int8_t>{1, 0}));
  VectorChunk short_chunk =
      VectorChunk::create(std::move(short_columns), VectorSelection::all(2U).value()).value();
  auto short_credit = resources.reserve(retained - 1U).value();
  EXPECT_EQ(AccountedVectorChunk::create(std::move(short_chunk), std::move(short_credit), resources)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(AccountedVectorChunkTest, RejectsCreditOwnedByAnotherQuery) {
  const auto owner = QueryResourceContext::create(4'096U).value();
  const auto impostor = QueryResourceContext::create(4'096U).value();
  QueryMemoryReservation reservation = owner.reserve(1'024U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(bool_column(std::vector<std::int8_t>{1, 0}));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(2U).value()).value();
  EXPECT_EQ(AccountedVectorChunk::create(std::move(chunk), std::move(reservation), impostor)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(owner.reserved_memory_bytes(), 0U);
}

TEST(PhysicalOperatorStepTest, EndIsExplicitAndHasNoChunk) {
  PhysicalOperatorStep end = PhysicalOperatorStep::end();
  EXPECT_EQ(end.kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(end.chunk(), nullptr);
  EXPECT_EQ(std::move(end).take_chunk().error().code(), common::StatusCode::kInvalidArgument);
}

TEST(BooleanFilterOperatorTest, FiltersOneChunkWithoutChangingItsChargeAndEndsSticky) {
  const auto resources = QueryResourceContext::create(4'096U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{10, 20, 30, 40, 50},
                                   std::vector<std::int8_t>{1, 0, -1, 1, 0}, {0U, 1U, 2U, 3U, 4U}));
  EXPECT_EQ(resources.reserved_memory_bytes(), 1'024U);
  auto filter = BooleanFilterOperator::create(std::make_unique<ChunkSource>(std::move(chunks)), 1U);
  ASSERT_TRUE(filter.has_value());
  {
    auto step = (*filter)->next(resources);
    ASSERT_TRUE(step.has_value());
    ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
    ASSERT_NE(step->chunk(), nullptr);
    EXPECT_EQ(step->chunk()->charged_memory_bytes(), 1'024U);
    const std::vector<std::uint32_t> expected{0U, 3U};
    EXPECT_TRUE(std::ranges::equal(step->chunk()->chunk().selection().indices(), expected));
  }
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ((*filter)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_TRUE(resources.request_cancel());
  EXPECT_EQ((*filter)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(BooleanFilterOperatorTest, PreCancelledPullRetainsOwnershipUntilThePipelineUnwinds) {
  const auto resources = QueryResourceContext::create(4'096U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{1, 2},
                                   std::vector<std::int8_t>{1, 1}, {0U, 1U}));
  auto filter =
      BooleanFilterOperator::create(std::make_unique<ChunkSource>(std::move(chunks)), 1U).value();
  EXPECT_TRUE(resources.request_cancel());
  EXPECT_EQ(filter->next(resources).error().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(resources.reserved_memory_bytes(), 1'024U);
  filter.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(BooleanFilterOperatorTest, PropagatesFailureAndRequestsSiblingCancellation) {
  const auto resources = QueryResourceContext::create(1U).value();
  auto filter = BooleanFilterOperator::create(std::make_unique<FailingSource>(), 0U).value();
  const auto failed = filter->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInternal);
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_EQ(filter->next(resources).error().code(), common::StatusCode::kCancelled);
}

TEST(BooleanFilterOperatorTest, InvalidPredicateOrdinalCancelsAndReleasesTheChunk) {
  const auto resources = QueryResourceContext::create(4'096U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources, std::vector<std::int64_t>{1, 2},
                                   std::vector<std::int8_t>{1, 1}, {0U, 1U}));
  auto filter =
      BooleanFilterOperator::create(std::make_unique<ChunkSource>(std::move(chunks)), 2U).value();
  const auto failed = filter->next(resources);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kOutOfRange);
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(BooleanFilterOperatorTest, RejectsAChunkChargedToAnotherQuery) {
  const auto owner = QueryResourceContext::create(4'096U).value();
  const auto impostor = QueryResourceContext::create(4'096U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(owner, std::vector<std::int64_t>{1, 2},
                                   std::vector<std::int8_t>{1, 1}, {0U, 1U}));
  auto filter =
      BooleanFilterOperator::create(std::make_unique<ChunkSource>(std::move(chunks)), 1U).value();
  const auto failed = filter->next(impostor);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(impostor.is_cancelled());
  EXPECT_EQ(owner.reserved_memory_bytes(), 0U);
}

TEST(BooleanFilterOperatorPropertyTest, MatchesScalarWhereTruthAcrossDeterministicChunkBoundaries) {
  constexpr std::size_t kMemoryLimit = std::size_t{64U} * 1'024U;
  constexpr std::array<std::int8_t, 3> kTruthValues{-1, 0, 1};
  const auto resources = QueryResourceContext::create(kMemoryLimit).value();
  std::vector<AccountedVectorChunk> chunks;
  std::vector<std::vector<std::uint32_t>> expected;
  std::uint32_t state = 0x9e37'79b9U;
  for (std::uint32_t chunk_index = 0U; chunk_index < 17U; ++chunk_index) {
    const std::uint32_t rows = (chunk_index % 13U) + 1U;
    std::vector<std::int64_t> values(rows);
    std::vector<std::int8_t> predicates(rows);
    std::vector<std::uint32_t> selected;
    std::vector<std::uint32_t> selected_expected;
    for (std::uint32_t row = 0U; row < rows; ++row) {
      state = state * 1'664'525U + 1'013'904'223U;
      values[row] = static_cast<std::int64_t>(state);
      predicates[row] = kTruthValues[(state >> 8U) % kTruthValues.size()];
      if ((state & 3U) != 0U) {
        selected.push_back(row);
        if (predicates[row] == 1)
          selected_expected.push_back(row);
      }
    }
    expected.push_back(std::move(selected_expected));
    chunks.push_back(accounted_chunk(resources, values, predicates, std::move(selected)));
  }

  auto filter =
      BooleanFilterOperator::create(std::make_unique<ChunkSource>(std::move(chunks)), 1U).value();
  for (const std::vector<std::uint32_t>& rows : expected) {
    auto step = filter->next(resources);
    ASSERT_TRUE(step.has_value());
    ASSERT_NE(step->chunk(), nullptr);
    EXPECT_TRUE(std::ranges::equal(step->chunk()->chunk().selection().indices(), rows));
  }
  EXPECT_EQ(filter->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

} // namespace
} // namespace chronos::query
