#include "chronos/common/status.hpp"
#include "chronos/query/sort.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t byte = 0U; byte < sizeof(value); ++byte)
    bytes[offset + byte] = static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
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

[[nodiscard]] columnar::OwnedPhysicalColumn
string_column(const std::span<const std::optional<std::string>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.offsets.resize((values.size() + 1U) * sizeof(std::uint32_t));
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
    } else {
      set_bit(buffers.validity, static_cast<std::uint32_t>(row));
      // The branch immediately above proves presence.
      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
      for (const char character : *values[row])
        buffers.values.push_back(static_cast<std::byte>(character));
    }
    store_u32(buffers.offsets, (row + 1U) * sizeof(std::uint32_t),
              static_cast<std::uint32_t>(buffers.values.size()));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kString),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
bool_column(const std::span<const std::optional<bool>> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
      continue;
    }
    set_bit(buffers.validity, static_cast<std::uint32_t>(row));
    // The branch immediately above proves presence.
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    if (*values[row])
      set_bit(buffers.values, static_cast<std::uint32_t>(row));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kBool),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk
accounted_chunk(const QueryResourceContext& resources,
                std::vector<columnar::OwnedPhysicalColumn> columns,
                std::vector<std::uint32_t> selection = {}) {
  const std::uint32_t rows = columns.front().row_count();
  common::Result<VectorSelection> selected =
      selection.empty() ? VectorSelection::all(rows)
                        : VectorSelection::from_indices(rows, std::move(selection));
  VectorChunk chunk = VectorChunk::create(std::move(columns), std::move(*selected)).value();
  QueryMemoryReservation reservation =
      resources.reserve(chunk.retained_buffer_bytes() + 512U).value();
  return AccountedVectorChunk::create(std::move(chunk), std::move(reservation), resources).value();
}

template <typename... Columns>
[[nodiscard]] std::vector<columnar::OwnedPhysicalColumn> columns(Columns&&... values) {
  std::vector<columnar::OwnedPhysicalColumn> output;
  output.reserve(sizeof...(values));
  (output.push_back(std::forward<Columns>(values)), ...);
  return output;
}

class ChunkSource final : public PhysicalOperator {
public:
  explicit ChunkSource(std::vector<AccountedVectorChunk> chunks) : chunks_(std::move(chunks)) {}

  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    if (cursor_ == chunks_.size())
      return PhysicalOperatorStep::end();
    return PhysicalOperatorStep::chunk(std::move(chunks_[cursor_++]));
  }

private:
  std::vector<AccountedVectorChunk> chunks_;
  std::size_t cursor_{};
};

class EmptySource final : public PhysicalOperator {
public:
  common::Result<PhysicalOperatorStep> next(const QueryResourceContext&) override {
    return PhysicalOperatorStep::end();
  }
};

[[nodiscard]] std::int64_t read_int64(const VectorChunk& chunk, const std::size_t column,
                                      const std::size_t row) {
  const common::ByteView bytes = chunk.cell({column, row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

TEST(SortOperatorTest, SortsMultipleChunksVariableNullAndDescendingKeysStably) {
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{16U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(
      resources,
      columns(string_column(std::vector<std::optional<std::string>>{"b", "a", std::nullopt}),
              int64_column(std::vector<std::int64_t>{2, 1, 9}),
              int64_column(std::vector<std::int64_t>{10, 11, 12}))));
  chunks.push_back(accounted_chunk(
      resources, columns(string_column(std::vector<std::optional<std::string>>{"a", "b", "a"}),
                         int64_column(std::vector<std::int64_t>{3, 2, 3}),
                         int64_column(std::vector<std::int64_t>{20, 21, 22}))));

  std::vector<VectorSortKey> keys{{.column_ordinal = 0U,
                                   .direction = PhysicalSortDirection::kAscending,
                                   .null_placement = ScalarNullPlacement::kLast},
                                  {.column_ordinal = 1U,
                                   .direction = PhysicalSortDirection::kDescending,
                                   .null_placement = ScalarNullPlacement::kFirst}};
  auto sorted =
      SortOperator::create(std::make_unique<ChunkSource>(std::move(chunks)), std::move(keys));
  ASSERT_TRUE(sorted.has_value()) << sorted.error().message();
  auto step = (*sorted)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().message();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  AccountedVectorChunk output = std::move(*step).take_chunk().value();
  ASSERT_EQ(output.chunk().selected_row_count(), 6U);
  std::vector<std::int64_t> identities;
  for (std::size_t row = 0U; row < output.chunk().selected_row_count(); ++row)
    identities.push_back(read_int64(output.chunk(), 2U, row));
  EXPECT_EQ(identities, (std::vector<std::int64_t>{20, 22, 11, 10, 21, 12}));
  EXPECT_EQ((*sorted)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(SortOperatorTest, EmptyInputIsStickyAndReleasesStateCredit) {
  QueryResourceContext resources = QueryResourceContext::create(std::size_t{1024U} * 1024U).value();
  auto sorted = SortOperator::create(
      std::make_unique<EmptySource>(),
      std::vector<VectorSortKey>{{.column_ordinal = 0U,
                                  .direction = PhysicalSortDirection::kAscending,
                                  .null_placement = ScalarNullPlacement::kLast}});
  ASSERT_TRUE(sorted.has_value());
  EXPECT_EQ((*sorted)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ((*sorted)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
}

TEST(SortOperatorTest, GathersFilteredPhysicalRowsIntoCanonicalNullableBooleanOutput) {
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(resources,
                                   columns(int64_column(std::vector<std::int64_t>{5, 1, 4, 2, 3}),
                                           bool_column(std::vector<std::optional<bool>>{
                                               true, false, true, false, std::nullopt})),
                                   {0U, 2U, 4U}));
  auto sorted = SortOperator::create(std::make_unique<ChunkSource>(std::move(chunks)),
                                     std::vector<VectorSortKey>{{.column_ordinal = 0U}});
  ASSERT_TRUE(sorted.has_value());
  auto step = (*sorted)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().message();
  AccountedVectorChunk output = std::move(*step).take_chunk().value();
  ASSERT_EQ(output.chunk().physical_row_count(), 3U);
  ASSERT_TRUE(output.chunk().selection().is_identity());
  EXPECT_EQ(read_int64(output.chunk(), 0U, 0U), 3);
  EXPECT_EQ(read_int64(output.chunk(), 0U, 1U), 4);
  EXPECT_EQ(read_int64(output.chunk(), 0U, 2U), 5);
  EXPECT_TRUE(output.chunk().cell({1U, 0U})->is_null());
  EXPECT_TRUE(output.chunk().cell({1U, 1U})->boolean().value());
  EXPECT_TRUE(output.chunk().cell({1U, 2U})->boolean().value());
}

TEST(SortOperatorTest, EnforcesRowKeyAndQueryIdentityLimits) {
  EXPECT_EQ(SortOperator::create(std::make_unique<EmptySource>(), {}, {}).error().code(),
            common::StatusCode::kInvalidArgument);
  SortLimits invalid_limits;
  invalid_limits.maximum_rows = 0U;
  EXPECT_EQ(SortOperator::create(std::make_unique<EmptySource>(),
                                 std::vector<VectorSortKey>{{.column_ordinal = 0U}}, invalid_limits)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  QueryResourceContext owner =
      QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  QueryResourceContext caller =
      QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> chunks;
  chunks.push_back(accounted_chunk(owner, columns(int64_column(std::vector<std::int64_t>{2, 1}))));
  auto foreign = SortOperator::create(std::make_unique<ChunkSource>(std::move(chunks)),
                                      std::vector<VectorSortKey>{{.column_ordinal = 0U}});
  ASSERT_TRUE(foreign.has_value());
  EXPECT_EQ((*foreign)->next(caller).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(caller.is_cancelled());
  EXPECT_EQ(owner.reserved_memory_bytes(), 0U);

  QueryResourceContext bounded =
      QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> too_many_rows;
  too_many_rows.push_back(
      accounted_chunk(bounded, columns(int64_column(std::vector<std::int64_t>{2, 1}))));
  SortLimits one_row;
  one_row.maximum_rows = 1U;
  auto limited = SortOperator::create(std::make_unique<ChunkSource>(std::move(too_many_rows)),
                                      std::vector<VectorSortKey>{{.column_ordinal = 0U}}, one_row);
  ASSERT_TRUE(limited.has_value());
  EXPECT_EQ((*limited)->next(bounded).error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(bounded.is_cancelled());
  EXPECT_EQ(bounded.reserved_memory_bytes(), 0U);

  QueryResourceContext ordinal_resources =
      QueryResourceContext::create(std::size_t{4U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> ordinal_chunks;
  ordinal_chunks.push_back(
      accounted_chunk(ordinal_resources, columns(int64_column(std::vector<std::int64_t>{1}))));
  auto invalid_ordinal =
      SortOperator::create(std::make_unique<ChunkSource>(std::move(ordinal_chunks)),
                           std::vector<VectorSortKey>{{.column_ordinal = 1U}});
  ASSERT_TRUE(invalid_ordinal.has_value());
  EXPECT_EQ((*invalid_ordinal)->next(ordinal_resources).error().code(),
            common::StatusCode::kOutOfRange);
  EXPECT_EQ(ordinal_resources.reserved_memory_bytes(), 0U);

  QueryResourceContext shape_resources =
      QueryResourceContext::create(std::size_t{8U} * 1024U * 1024U).value();
  std::vector<AccountedVectorChunk> mismatched;
  mismatched.push_back(
      accounted_chunk(shape_resources, columns(int64_column(std::vector<std::int64_t>{1}))));
  mismatched.push_back(accounted_chunk(
      shape_resources,
      columns(string_column(std::vector<std::optional<std::string>>{"different"}))));
  auto changed_shape = SortOperator::create(std::make_unique<ChunkSource>(std::move(mismatched)),
                                            std::vector<VectorSortKey>{{.column_ordinal = 0U}});
  ASSERT_TRUE(changed_shape.has_value());
  EXPECT_EQ((*changed_shape)->next(shape_resources).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(shape_resources.is_cancelled());
  EXPECT_EQ(shape_resources.reserved_memory_bytes(), 0U);
}

TEST(SortOperatorPropertyTest, MatchesIndependentStableIntegerModelAcrossChunkBoundaries) {
  std::mt19937_64 random{0x4f524445525f4259ULL};
  struct Row {
    std::int64_t key;
    std::int64_t identity;
  };
  std::vector<Row> model;
  std::vector<AccountedVectorChunk> chunks;
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  std::int64_t identity = 0;
  for (std::size_t chunk_index = 0U; chunk_index < 17U; ++chunk_index) {
    std::vector<std::int64_t> keys;
    std::vector<std::int64_t> identities;
    const std::size_t rows = 1U + static_cast<std::size_t>(random() % 23U);
    for (std::size_t row = 0U; row < rows; ++row) {
      const std::int64_t key = static_cast<std::int64_t>(random() % 13U) - 6;
      keys.push_back(key);
      identities.push_back(identity);
      model.push_back({.key = key, .identity = identity++});
    }
    chunks.push_back(
        accounted_chunk(resources, columns(int64_column(keys), int64_column(identities))));
  }
  std::stable_sort(model.begin(), model.end(),
                   [](const Row& left, const Row& right) { return left.key < right.key; });
  auto sorted = SortOperator::create(std::make_unique<ChunkSource>(std::move(chunks)),
                                     std::vector<VectorSortKey>{{.column_ordinal = 0U}});
  ASSERT_TRUE(sorted.has_value());
  auto step = (*sorted)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().message();
  AccountedVectorChunk output = std::move(*step).take_chunk().value();
  ASSERT_EQ(output.chunk().selected_row_count(), model.size());
  for (std::size_t row = 0U; row < model.size(); ++row) {
    EXPECT_EQ(read_int64(output.chunk(), 0U, row), model[row].key);
    EXPECT_EQ(read_int64(output.chunk(), 1U, row), model[row].identity);
  }
}

} // namespace
} // namespace chronos::query
