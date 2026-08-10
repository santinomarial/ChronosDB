#include "chronos/query/physical_operator.hpp"
#include "chronos/query/value.hpp"
#include "chronos/query/vector_chunk.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
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

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn int64_column(const std::vector<std::int64_t>& values,
                                                         const std::vector<bool>& present = {},
                                                         const std::size_t retained_capacity = 0U) {
  columnar::ColumnVectorBuffers buffers;
  std::uint32_t null_count = 0U;
  if (!present.empty()) {
    buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
    for (std::size_t row = 0U; row < present.size(); ++row) {
      if (present[row]) {
        buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
      } else {
        ++null_count;
      }
    }
  }
  if (retained_capacity > 0U)
    buffers.values.reserve(retained_capacity);
  buffers.values.resize(values.size() * sizeof(std::int64_t));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!present.empty() && !present[row])
      continue;
    const std::uint64_t bits = std::bit_cast<std::uint64_t>(values[row]);
    for (std::size_t byte = 0U; byte < sizeof(bits); ++byte) {
      buffers.values[row * sizeof(bits) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & 0xffU);
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kInt64),
              .nullable = !present.empty(),
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
timestamp_column(const std::vector<std::int64_t>& values) {
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
             {.type = type(schema::LogicalTypeKind::kTimestampNs),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] std::int64_t selected_int64(const VectorChunk& chunk,
                                          const std::size_t selected_row) {
  const auto cell = chunk.cell({.column_ordinal = 0U, .selected_row = selected_row}).value();
  const ScalarValue scalar =
      ScalarValue::from_column_cell(type(schema::LogicalTypeKind::kInt64), cell).value();
  return *std::get_if<std::int64_t>(&scalar.storage());
}

class OwnedTestBacking final : public VectorChunkBacking {
public:
  explicit OwnedTestBacking(std::vector<columnar::OwnedPhysicalColumn> columns,
                            std::shared_ptr<bool> destroyed = {},
                            std::optional<std::size_t> reported_buffer = {},
                            std::optional<std::size_t> reported_retained = {},
                            const bool missing_column = false)
      : columns_(std::move(columns)), destroyed_(std::move(destroyed)),
        missing_column_(missing_column) {
    std::size_t buffer_bytes = 0U;
    std::size_t retained_bytes = columns_.capacity() * sizeof(columnar::OwnedPhysicalColumn);
    for (const columnar::OwnedPhysicalColumn& column : columns_) {
      buffer_bytes += column.buffer_bytes();
      retained_bytes += column.retained_buffer_bytes();
    }
    buffer_bytes_ = reported_buffer.value_or(buffer_bytes);
    retained_buffer_bytes_ = reported_retained.value_or(retained_bytes);
  }

  ~OwnedTestBacking() override {
    if (destroyed_ != nullptr)
      *destroyed_ = true;
  }

  [[nodiscard]] std::size_t column_count() const noexcept override {
    return columns_.size();
  }

  [[nodiscard]] const columnar::PhysicalColumnView*
  column(const std::size_t ordinal) const noexcept override {
    if (missing_column_ || ordinal >= columns_.size())
      return nullptr;
    return &columns_[ordinal].view();
  }

  [[nodiscard]] std::size_t buffer_bytes() const noexcept override {
    return buffer_bytes_;
  }

  [[nodiscard]] std::size_t retained_buffer_bytes() const noexcept override {
    return retained_buffer_bytes_;
  }

private:
  std::vector<columnar::OwnedPhysicalColumn> columns_;
  std::shared_ptr<bool> destroyed_;
  bool missing_column_;
  std::size_t buffer_bytes_{};
  std::size_t retained_buffer_bytes_{};
};

[[nodiscard]] std::shared_ptr<const VectorChunkBacking>
test_backing(std::vector<columnar::OwnedPhysicalColumn> columns,
             std::shared_ptr<bool> destroyed = {}, std::optional<std::size_t> reported_buffer = {},
             std::optional<std::size_t> reported_retained = {}, const bool missing_column = false) {
  return std::make_shared<const OwnedTestBacking>(
      std::move(columns), std::move(destroyed), reported_buffer, reported_retained, missing_column);
}

TEST(VectorSelectionTest, OwnsExplicitOrderedIdentityAndSparseSelections) {
  const auto all = VectorSelection::all(4U);
  ASSERT_TRUE(all.has_value());
  EXPECT_TRUE(all->is_identity());
  const std::vector<std::uint32_t> expected{0U, 1U, 2U, 3U};
  EXPECT_TRUE(std::ranges::equal(all->indices(), expected));
  EXPECT_EQ(all->physical_row(3U).value(), 3U);
  EXPECT_EQ(all->physical_row(4U).error().code(), common::StatusCode::kOutOfRange);

  const auto sparse = VectorSelection::from_indices(8U, {1U, 3U, 7U});
  ASSERT_TRUE(sparse.has_value());
  EXPECT_FALSE(sparse->is_identity());
  EXPECT_EQ(sparse->selected_row_count(), 3U);
  EXPECT_EQ(sparse->buffer_bytes(), 3U * sizeof(std::uint32_t));
  EXPECT_GE(sparse->retained_buffer_bytes(), sparse->buffer_bytes());

  const auto empty = VectorSelection::from_indices(8U, {});
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ(empty->selected_row_count(), 0U);
  EXPECT_FALSE(empty->is_identity());
}

TEST(VectorSelectionTest, RejectsInvalidDomainsDuplicatesAndReordering) {
  EXPECT_FALSE(VectorSelection::all(0U).has_value());
  EXPECT_FALSE(VectorSelection::from_indices(0U, {}).has_value());
  EXPECT_FALSE(VectorSelection::from_indices(4U, {0U, 4U}).has_value());
  EXPECT_FALSE(VectorSelection::from_indices(4U, {1U, 1U}).has_value());
  EXPECT_FALSE(VectorSelection::from_indices(4U, {2U, 1U}).has_value());
}

TEST(VectorSelectionTest, IdentityTimestampRangeKernelPreservesExactOrderAndBounds) {
  std::vector<std::int64_t> timestamps(65U);
  std::vector<std::uint32_t> expected;
  for (std::uint32_t row = 0U; row < timestamps.size(); ++row) {
    timestamps[row] = static_cast<std::int64_t>(row) - 32;
    if (timestamps[row] >= -7 && timestamps[row] < 13)
      expected.push_back(row);
  }
  const auto column = timestamp_column(timestamps);
  const TimestampRangePredicate predicate{.lower = TimestampRangeBound{-7, true},
                                          .upper = TimestampRangeBound{13, false}};

  auto filtered = VectorSelection::where_timestamp_in_range(VectorSelection::all(65U).value(),
                                                            column.view(), predicate);

  ASSERT_TRUE(filtered.has_value()) << filtered.error().to_string();
  EXPECT_TRUE(std::ranges::equal(filtered->indices(), expected));
  EXPECT_FALSE(filtered->is_identity());
}

TEST(VectorChunkTest, MapsSelectedRowsAcrossCanonicalPhysicalColumns) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column({10, 20, 30, 40}, {true, false, true, true}));
  auto selection = VectorSelection::from_indices(4U, {0U, 2U, 3U}).value();
  const auto chunk = VectorChunk::create(std::move(columns), std::move(selection));
  ASSERT_TRUE(chunk.has_value()) << chunk.error().to_string();
  EXPECT_EQ(chunk->physical_row_count(), 4U);
  EXPECT_EQ(chunk->selected_row_count(), 3U);
  EXPECT_EQ(chunk->column_count(), 1U);
  EXPECT_EQ(selected_int64(*chunk, 0U), 10);
  EXPECT_EQ(selected_int64(*chunk, 1U), 30);
  EXPECT_EQ(selected_int64(*chunk, 2U), 40);
  EXPECT_EQ(chunk->column(1U), nullptr);
  EXPECT_EQ(chunk->cell({.column_ordinal = 1U, .selected_row = 0U}).error().code(),
            common::StatusCode::kOutOfRange);
  EXPECT_EQ(chunk->cell({.column_ordinal = 0U, .selected_row = 3U}).error().code(),
            common::StatusCode::kOutOfRange);
}

TEST(VectorChunkTest, SupportsColumnFreeCardinalityAndEmptySelection) {
  auto selection = VectorSelection::from_indices(5U, {}).value();
  const auto chunk = VectorChunk::create({}, std::move(selection));
  ASSERT_TRUE(chunk.has_value());
  EXPECT_EQ(chunk->physical_row_count(), 5U);
  EXPECT_EQ(chunk->selected_row_count(), 0U);
  EXPECT_EQ(chunk->column_count(), 0U);
  EXPECT_EQ(chunk->buffer_bytes(), 0U);
}

TEST(VectorChunkBackingTest, PinsBorrowedColumnsAndKeepsBackingAccountingAfterProjection) {
  auto destroyed = std::make_shared<bool>(false);
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column({10, 20, 30}));
  columns.push_back(int64_column({40, 50, 60}));
  std::shared_ptr<const VectorChunkBacking> backing = test_backing(std::move(columns), destroyed);
  const std::size_t backing_buffer_bytes = backing->buffer_bytes();
  const std::size_t backing_retained_bytes = backing->retained_buffer_bytes();

  {
    auto chunk =
        VectorChunk::create_backed(backing, VectorSelection::from_indices(3U, {0U, 2U}).value());
    ASSERT_TRUE(chunk.has_value());
    EXPECT_EQ(chunk->column_count(), 2U);
    EXPECT_EQ(chunk->buffer_bytes(), backing_buffer_bytes + 2U * sizeof(std::uint32_t));
    EXPECT_GE(chunk->retained_buffer_bytes(), backing_retained_bytes);
    const std::size_t chunk_buffer_bytes = chunk->buffer_bytes();
    const std::size_t chunk_retained_bytes = chunk->retained_buffer_bytes();
    backing.reset();
    EXPECT_FALSE(*destroyed);

    auto projected =
        VectorChunk::project_columns(std::move(*chunk), std::array<std::size_t, 1>{1U});
    ASSERT_TRUE(projected.has_value());
    EXPECT_EQ(projected->column_count(), 1U);
    EXPECT_EQ(projected->buffer_bytes(), chunk_buffer_bytes);
    EXPECT_EQ(projected->retained_buffer_bytes(), chunk_retained_bytes);
    EXPECT_EQ(selected_int64(*projected, 0U), 40);
    EXPECT_EQ(selected_int64(*projected, 1U), 60);
    EXPECT_FALSE(*destroyed);
  }
  EXPECT_TRUE(*destroyed);
}

TEST(VectorChunkBackingTest, RejectsInvalidShapeAndUnderreportedOwnership) {
  EXPECT_EQ(VectorChunk::create_backed({}, VectorSelection::all(1U).value()).error().code(),
            common::StatusCode::kInvalidArgument);

  {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column({1, 2}));
    auto result = VectorChunk::create_backed(test_backing(std::move(columns)),
                                             VectorSelection::all(3U).value());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kInvalidArgument);
  }
  {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column({1, 2}));
    auto result = VectorChunk::create_backed(test_backing(std::move(columns), {}, 0U, 0U),
                                             VectorSelection::all(2U).value());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kInvalidArgument);
  }
  {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column({1, 2}));
    auto result = VectorChunk::create_backed(test_backing(std::move(columns), {}, {}, {}, true),
                                             VectorSelection::all(2U).value());
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kInvalidArgument);
  }
}

TEST(VectorChunkBackingTest, EnforcesEveryConfiguredConstructionBound) {
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column({1, 2}));
  columns.push_back(int64_column({3, 4}));
  const std::shared_ptr<const VectorChunkBacking> backing = test_backing(std::move(columns));

  EXPECT_EQ(
      VectorChunk::create_backed(backing, VectorSelection::all(2U).value(), {.maximum_rows = 0U})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(
      VectorChunk::create_backed(backing, VectorSelection::all(2U).value(), {.maximum_rows = 1U})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);
  EXPECT_EQ(
      VectorChunk::create_backed(backing, VectorSelection::all(2U).value(), {.maximum_columns = 1U})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);
  EXPECT_EQ(VectorChunk::create_backed(backing, VectorSelection::all(2U).value(),
                                       {.maximum_buffer_bytes = backing->buffer_bytes()})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(VectorChunk::create_backed(
                backing, VectorSelection::all(2U).value(),
                {.maximum_retained_buffer_bytes = backing->retained_buffer_bytes()})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
}

TEST(VectorChunkBackingTest, AccountedOwnershipKeepsThePinAndCreditCoupled) {
  const auto resources = QueryResourceContext::create(4'096U).value();
  auto destroyed = std::make_shared<bool>(false);
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column({1, 2, 3}));
  std::shared_ptr<const VectorChunkBacking> backing = test_backing(std::move(columns), destroyed);
  auto chunk = VectorChunk::create_backed(backing, VectorSelection::all(3U).value()).value();
  const std::size_t charge = chunk.retained_buffer_bytes();
  {
    auto accounted = AccountedVectorChunk::create(std::move(chunk),
                                                  resources.reserve(charge).value(), resources);
    ASSERT_TRUE(accounted.has_value());
    backing.reset();
    EXPECT_FALSE(*destroyed);
    EXPECT_EQ(resources.reserved_memory_bytes(), charge);
  }
  EXPECT_TRUE(*destroyed);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(VectorChunkBackingTest, SharedCreditCoversOnePinAcrossIndependentChunkOwners) {
  const auto resources = QueryResourceContext::create(4'096U).value();
  auto destroyed = std::make_shared<bool>(false);
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(int64_column({1, 2, 3}));
  std::shared_ptr<const VectorChunkBacking> backing = test_backing(std::move(columns), destroyed);
  auto chunk = VectorChunk::create_backed(backing, VectorSelection::all(3U).value()).value();
  const std::size_t retained = chunk.retained_buffer_bytes();
  const std::size_t shared_bytes = retained / 2U;
  const std::size_t local_bytes = retained - shared_bytes;
  QuerySharedMemoryReservation shared = resources.reserve_shared(shared_bytes).value();
  {
    auto accounted = AccountedVectorChunk::create(
        std::move(chunk), resources.reserve(local_bytes).value(), shared, resources);
    ASSERT_TRUE(accounted.has_value()) << accounted.error().to_string();
    backing.reset();
    shared.reset();
    EXPECT_FALSE(*destroyed);
    EXPECT_EQ(accounted->charged_memory_bytes(), retained);
    EXPECT_EQ(resources.reserved_memory_bytes(), retained);
    EXPECT_TRUE(accounted->belongs_to(resources));
  }
  EXPECT_TRUE(*destroyed);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  std::vector<columnar::OwnedPhysicalColumn> insufficient_columns;
  insufficient_columns.push_back(int64_column({1, 2, 3}));
  auto insufficient = VectorChunk::create_backed(test_backing(std::move(insufficient_columns)),
                                                 VectorSelection::all(3U).value())
                          .value();
  const std::size_t required = insufficient.retained_buffer_bytes();
  QuerySharedMemoryReservation too_small = resources.reserve_shared(1U).value();
  auto rejected = AccountedVectorChunk::create(
      std::move(insufficient), resources.reserve(required - 2U).value(), too_small, resources);
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  too_small.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(VectorChunkTest, EnforcesShapeLogicalAndRetainedBoundsBeforeRetention) {
  {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column({1, 2, 3}));
    auto selection = VectorSelection::all(3U).value();
    const auto mismatched = VectorChunk::create(std::move(columns), std::move(selection),
                                                {.maximum_rows = 2U,
                                                 .maximum_columns = 1U,
                                                 .maximum_buffer_bytes = 1'024U,
                                                 .maximum_retained_buffer_bytes = 1'024U});
    ASSERT_FALSE(mismatched.has_value());
    EXPECT_EQ(mismatched.error().code(), common::StatusCode::kResourceExhausted);
  }
  {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column({1, 2}));
    auto selection = VectorSelection::all(3U).value();
    const auto mismatched = VectorChunk::create(std::move(columns), std::move(selection));
    ASSERT_FALSE(mismatched.has_value());
    EXPECT_EQ(mismatched.error().code(), common::StatusCode::kInvalidArgument);
  }
  {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column({1, 2}));
    auto selection = VectorSelection::all(2U).value();
    const auto logical = VectorChunk::create(std::move(columns), std::move(selection),
                                             {.maximum_rows = 2U,
                                              .maximum_columns = 1U,
                                              .maximum_buffer_bytes = 23U,
                                              .maximum_retained_buffer_bytes = 1'024U});
    ASSERT_FALSE(logical.has_value());
    EXPECT_EQ(logical.error().code(), common::StatusCode::kResourceExhausted);
  }
  {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column({1, 2}));
    columns.push_back(int64_column({3, 4}));
    auto selection = VectorSelection::all(2U).value();
    const auto column_limited = VectorChunk::create(std::move(columns), std::move(selection),
                                                    {.maximum_rows = 2U,
                                                     .maximum_columns = 1U,
                                                     .maximum_buffer_bytes = 1'024U,
                                                     .maximum_retained_buffer_bytes = 1'024U});
    ASSERT_FALSE(column_limited.has_value());
    EXPECT_EQ(column_limited.error().code(), common::StatusCode::kResourceExhausted);
  }
  {
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column({1, 2}, {}, 256U));
    auto selection = VectorSelection::all(2U).value();
    const auto retained = VectorChunk::create(std::move(columns), std::move(selection),
                                              {.maximum_rows = 2U,
                                               .maximum_columns = 1U,
                                               .maximum_buffer_bytes = 1'024U,
                                               .maximum_retained_buffer_bytes = 64U});
    ASSERT_FALSE(retained.has_value());
    EXPECT_EQ(retained.error().code(), common::StatusCode::kResourceExhausted);
  }
  {
    std::vector<std::uint32_t> selected;
    selected.reserve(64U);
    selected.push_back(0U);
    selected.push_back(1U);
    auto selection = VectorSelection::from_indices(2U, std::move(selected)).value();
    const auto retained_selection = VectorChunk::create({}, std::move(selection),
                                                        {.maximum_rows = 2U,
                                                         .maximum_columns = 1U,
                                                         .maximum_buffer_bytes = 1'024U,
                                                         .maximum_retained_buffer_bytes = 64U});
    ASSERT_FALSE(retained_selection.has_value());
    EXPECT_EQ(retained_selection.error().code(), common::StatusCode::kResourceExhausted);
  }
  auto selection = VectorSelection::all(1U).value();
  EXPECT_FALSE(VectorChunk::create({}, std::move(selection), {.maximum_rows = 0U}).has_value());
}

TEST(VectorChunkPropertyTest, PreservesOrderAcrossDeterministicBatchAndSelectionBoundaries) {
  for (std::uint32_t rows = 1U; rows <= 257U; rows += 17U) {
    std::vector<std::int64_t> values(rows);
    std::vector<std::uint32_t> selected;
    for (std::uint32_t row = 0U; row < rows; ++row) {
      values[row] = static_cast<std::int64_t>(row) * -13 + 7;
      if ((row % 3U) != 1U)
        selected.push_back(row);
    }
    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.push_back(int64_column(values));
    auto selection = VectorSelection::from_indices(rows, selected).value();
    const auto chunk = VectorChunk::create(std::move(columns), std::move(selection),
                                           {.maximum_rows = rows,
                                            .maximum_columns = 1U,
                                            .maximum_buffer_bytes = 1U << 20U,
                                            .maximum_retained_buffer_bytes = 1U << 20U});
    ASSERT_TRUE(chunk.has_value());
    std::vector<columnar::OwnedPhysicalColumn> backed_columns;
    backed_columns.push_back(int64_column(values));
    const auto backed =
        VectorChunk::create_backed(test_backing(std::move(backed_columns)),
                                   VectorSelection::from_indices(rows, selected).value(),
                                   {.maximum_rows = rows,
                                    .maximum_columns = 1U,
                                    .maximum_buffer_bytes = 1U << 20U,
                                    .maximum_retained_buffer_bytes = 1U << 20U});
    ASSERT_TRUE(backed.has_value());
    for (std::size_t index = 0U; index < selected.size(); ++index) {
      EXPECT_EQ(selected_int64(*chunk, index), values[selected[index]]);
      EXPECT_EQ(selected_int64(*backed, index), selected_int64(*chunk, index));
    }
  }
}

} // namespace
} // namespace chronos::query
