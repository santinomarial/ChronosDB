#include "chronos/common/uuid.hpp"
#include "chronos/query/asof_join.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

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

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

void set_bit(std::vector<std::byte>& bytes, const std::uint32_t row) {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

template <typename Value>
[[nodiscard]] columnar::OwnedPhysicalColumn
fixed_column(const schema::LogicalTypeKind kind,
             const std::span<const std::optional<Value>> values) {
  static_assert(std::is_integral_v<Value>);
  columnar::ColumnVectorBuffers buffers;
  buffers.validity.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  buffers.values.resize(values.size() * sizeof(Value));
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
      continue;
    }
    set_bit(buffers.validity, static_cast<std::uint32_t>(row));
    using Unsigned = std::make_unsigned_t<Value>;
    const Unsigned bits =
        std::bit_cast<Unsigned>(values[row].value()); // NOLINT(bugprone-unchecked-optional-access)
    for (std::size_t byte = 0U; byte < sizeof(Value); ++byte) {
      buffers.values[row * sizeof(Value) + byte] =
          static_cast<std::byte>((bits >> (byte * 8U)) & Unsigned{0xffU});
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

template <typename Value>
[[nodiscard]] columnar::OwnedPhysicalColumn unsigned_column(const schema::LogicalTypeKind kind,
                                                            const std::span<const Value> values) {
  static_assert(std::is_unsigned_v<Value>);
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * sizeof(Value));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    for (std::size_t byte = 0U; byte < sizeof(Value); ++byte) {
      buffers.values[row * sizeof(Value) + byte] =
          static_cast<std::byte>((values[row] >> (byte * 8U)) & Value{0xffU});
    }
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(kind),
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
  const auto store_offset = [&buffers](const std::size_t row, const std::uint32_t value) {
    for (std::size_t byte = 0U; byte < sizeof(value); ++byte) {
      buffers.offsets[row * sizeof(value) + byte] =
          static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
    }
  };
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
    } else {
      set_bit(buffers.validity, static_cast<std::uint32_t>(row));
      for (const char byte : values[row].value()) // NOLINT(bugprone-unchecked-optional-access)
        buffers.values.push_back(static_cast<std::byte>(byte));
    }
    store_offset(row + 1U, static_cast<std::uint32_t>(buffers.values.size()));
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
uuid_column(const std::span<const common::Uuid> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.reserve(values.size() * common::Uuid::kSize);
  for (const common::Uuid& value : values)
    buffers.values.insert(buffers.values.end(), value.bytes().begin(), value.bytes().end());
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kUuid),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] AccountedVectorChunk accounted(std::vector<columnar::OwnedPhysicalColumn> columns,
                                             const std::uint32_t rows,
                                             const QueryResourceContext& resources) {
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(rows).value()).value();
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(32'768U).value(),
                                      resources)
      .value();
}

[[nodiscard]] AccountedVectorChunk left_input(const QueryResourceContext& resources) {
  const std::array<std::optional<std::string>, 4U> symbols{"A", "A", "B", std::nullopt};
  const std::array<std::optional<std::int64_t>, 4U> times{10, std::nullopt, 5, 7};
  const std::array<std::optional<std::int64_t>, 4U> payloads{1, 2, 3, 4};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(string_column(symbols));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs,
                                 std::span<const std::optional<std::int64_t>>{times}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{payloads}));
  return accounted(std::move(columns), 4U, resources);
}

[[nodiscard]] AccountedVectorChunk right_input(const QueryResourceContext& resources) {
  const std::array<std::optional<std::string>, 6U> symbols{"A", "A", "A", "A", "B", std::nullopt};
  const std::array<std::optional<std::int64_t>, 6U> times{8, 10, 10, 10, 6, 7};
  const std::array<std::optional<std::int64_t>, 6U> payloads{80, 100, 101, 102, 60, 70};
  const std::array<std::optional<std::int64_t>, 6U> physical{1, 1, 2, 2, 1, 1};
  common::Uuid::Bytes wal_bytes{};
  wal_bytes.front() = std::byte{1U};
  const common::Uuid wal{wal_bytes};
  const std::array<common::Uuid, 6U> wal_ids{wal, wal, wal, wal, wal, wal};
  constexpr std::array<std::uint64_t, 6U> kSequences{1, 1, 1, 2, 1, 1};
  constexpr std::array<std::uint32_t, 6U> kRows{0, 0, 0, 0, 0, 0};
  constexpr std::array<std::uint8_t, 6U> kOperations{1, 1, 1, 1, 1, 1};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(string_column(symbols));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs,
                                 std::span<const std::optional<std::int64_t>>{times}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{payloads}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{physical}));
  columns.push_back(uuid_column(wal_ids));
  columns.push_back(unsigned_column(schema::LogicalTypeKind::kUInt64,
                                    std::span<const std::uint64_t>{kSequences}));
  columns.push_back(
      unsigned_column(schema::LogicalTypeKind::kUInt32, std::span<const std::uint32_t>{kRows}));
  columns.push_back(
      unsigned_column(schema::LogicalTypeKind::kUInt8, std::span<const std::uint8_t>{kOperations}));
  return accounted(std::move(columns), 6U, resources);
}

[[nodiscard]] VectorAsofJoinDefinition definition(const bool left_outer) {
  return {
      .left_input_columns = {{.type = type(schema::LogicalTypeKind::kString), .nullable = true},
                             {.type = type(schema::LogicalTypeKind::kTimestampNs),
                              .nullable = true},
                             {.type = type(schema::LogicalTypeKind::kInt64), .nullable = true}},
      .right_input_columns = {{.type = type(schema::LogicalTypeKind::kString), .nullable = true},
                              {.type = type(schema::LogicalTypeKind::kTimestampNs),
                               .nullable = true},
                              {.type = type(schema::LogicalTypeKind::kInt64), .nullable = true},
                              {.type = type(schema::LogicalTypeKind::kInt64), .nullable = true},
                              {.type = type(schema::LogicalTypeKind::kUuid), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kUInt64), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kUInt32), .nullable = false},
                              {.type = type(schema::LogicalTypeKind::kUInt8), .nullable = false}},
      .equality_keys = {{.left_column_ordinal = 0U, .right_column_ordinal = 0U}},
      .left_timestamp_column_ordinal = 1U,
      .right_timestamp_column_ordinal = 1U,
      .right_physical_ordering_key_ordinals = {3U},
      .right_row_version_first_column_ordinal = 4U,
      .left_output_column_ordinals = {2U},
      .right_output_column_ordinals = {2U},
      .left_outer = left_outer};
}

[[nodiscard]] std::optional<std::int64_t>
read_i64(const VectorChunk& chunk, const std::size_t column, const std::size_t row) {
  const columnar::ColumnCellView cell = chunk.cell({column, row}).value();
  if (cell.is_null())
    return std::nullopt;
  const common::ByteView bytes = cell.bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t byte = 0U; byte < bytes.size(); ++byte)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[byte])) << (byte * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

struct ModelLeftRow {
  std::optional<std::int64_t> key;
  std::optional<std::int64_t> time;
  std::int64_t payload;
};

struct ModelRightRow {
  std::optional<std::int64_t> key;
  std::optional<std::int64_t> time;
  std::int64_t payload;
  std::optional<std::int64_t> physical;
  std::uint64_t sequence;
  std::uint32_t row_ordinal;
};

[[nodiscard]] AccountedVectorChunk model_left_input(const std::span<const ModelLeftRow> rows,
                                                    const QueryResourceContext& resources) {
  std::vector<std::optional<std::int64_t>> keys;
  std::vector<std::optional<std::int64_t>> times;
  std::vector<std::optional<std::int64_t>> payloads;
  for (const ModelLeftRow& row : rows) {
    keys.push_back(row.key);
    times.push_back(row.time);
    payloads.emplace_back(row.payload);
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{keys}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs,
                                 std::span<const std::optional<std::int64_t>>{times}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{payloads}));
  return accounted(std::move(columns), static_cast<std::uint32_t>(rows.size()), resources);
}

[[nodiscard]] AccountedVectorChunk model_right_input(const std::span<const ModelRightRow> rows,
                                                     const QueryResourceContext& resources) {
  std::vector<std::optional<std::int64_t>> keys;
  std::vector<std::optional<std::int64_t>> times;
  std::vector<std::optional<std::int64_t>> payloads;
  std::vector<std::optional<std::int64_t>> physical;
  std::vector<common::Uuid> wal_ids;
  std::vector<std::uint64_t> sequences;
  std::vector<std::uint32_t> row_ordinals;
  std::vector<std::uint8_t> operations(rows.size(), 1U);
  common::Uuid::Bytes wal_bytes{};
  wal_bytes.front() = std::byte{1U};
  for (const ModelRightRow& row : rows) {
    keys.push_back(row.key);
    times.push_back(row.time);
    payloads.emplace_back(row.payload);
    physical.push_back(row.physical);
    wal_ids.emplace_back(wal_bytes);
    sequences.push_back(row.sequence);
    row_ordinals.push_back(row.row_ordinal);
  }
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{keys}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kTimestampNs,
                                 std::span<const std::optional<std::int64_t>>{times}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{payloads}));
  columns.push_back(fixed_column(schema::LogicalTypeKind::kInt64,
                                 std::span<const std::optional<std::int64_t>>{physical}));
  columns.push_back(uuid_column(wal_ids));
  columns.push_back(
      unsigned_column(schema::LogicalTypeKind::kUInt64, std::span<const std::uint64_t>{sequences}));
  columns.push_back(unsigned_column(schema::LogicalTypeKind::kUInt32,
                                    std::span<const std::uint32_t>{row_ordinals}));
  columns.push_back(
      unsigned_column(schema::LogicalTypeKind::kUInt8, std::span<const std::uint8_t>{operations}));
  return accounted(std::move(columns), static_cast<std::uint32_t>(rows.size()), resources);
}

[[nodiscard]] bool later_model(const ModelRightRow& candidate, const ModelRightRow& current) {
  if (candidate.time != current.time)
    return candidate.time > current.time;
  if (candidate.physical != current.physical) {
    if (!candidate.physical.has_value() || !current.physical.has_value())
      return !candidate.physical.has_value();
    return candidate.physical > current.physical;
  }
  return std::tie(candidate.sequence, candidate.row_ordinal) >
         std::tie(current.sequence, current.row_ordinal);
}

[[nodiscard]] VectorAsofJoinDefinition model_definition() {
  VectorAsofJoinDefinition result = definition(true);
  result.left_input_columns[0U].type = type(schema::LogicalTypeKind::kInt64);
  result.right_input_columns[0U].type = type(schema::LogicalTypeKind::kInt64);
  return result;
}

TEST(AsofJoinOperatorTest, LeftJoinUsesExactTimestampPhysicalAndVersionWinner) {
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  std::vector<AccountedVectorChunk> left;
  left.push_back(left_input(resources));
  std::vector<AccountedVectorChunk> right;
  right.push_back(right_input(resources));
  auto join =
      AsofJoinOperator::create(std::make_unique<ChunkSource>(std::move(left)),
                               std::make_unique<ChunkSource>(std::move(right)), definition(true));
  ASSERT_TRUE(join.has_value()) << join.error().message();
  auto step = (*join)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().message();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step->chunk()->chunk();
  ASSERT_EQ(output.column_count(), 3U);
  ASSERT_EQ(output.selected_row_count(), 4U);
  EXPECT_EQ(read_i64(output, 0U, 0U), 1);
  EXPECT_EQ(read_i64(output, 1U, 0U), 102);
  EXPECT_TRUE(output.cell({2U, 0U}).value().boolean().value());
  for (std::size_t row_index = 1U; row_index < 4U; ++row_index) {
    EXPECT_FALSE(read_i64(output, 1U, row_index).has_value());
    EXPECT_FALSE(output.cell({2U, row_index}).value().boolean().value());
  }
  step = (*join)->next(resources);
  ASSERT_TRUE(step.has_value());
  EXPECT_EQ(step->kind(), PhysicalOperatorStepKind::kEnd);
  step = PhysicalOperatorStep::end();
  (*join).reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(AsofJoinOperatorTest, InnerJoinDropsMissesAndReleasesBothInputs) {
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  std::vector<AccountedVectorChunk> left;
  left.push_back(left_input(resources));
  std::vector<AccountedVectorChunk> right;
  right.push_back(right_input(resources));
  auto join =
      AsofJoinOperator::create(std::make_unique<ChunkSource>(std::move(left)),
                               std::make_unique<ChunkSource>(std::move(right)), definition(false))
          .value();
  auto step = join->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step.chunk()->chunk().selected_row_count(), 1U);
  EXPECT_EQ(read_i64(step.chunk()->chunk(), 1U, 0U), 102);
  step = PhysicalOperatorStep::end();
  join.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(AsofJoinOperatorTest, RejectsHostileSuffixAndFiniteStateLimit) {
  VectorAsofJoinDefinition malformed = definition(false);
  malformed.right_row_version_first_column_ordinal = 3U;
  auto invalid_join = AsofJoinOperator::create(
      std::make_unique<ChunkSource>(std::vector<AccountedVectorChunk>{}),
      std::make_unique<ChunkSource>(std::vector<AccountedVectorChunk>{}), std::move(malformed));
  ASSERT_FALSE(invalid_join.has_value());
  EXPECT_EQ(invalid_join.error().code(), common::StatusCode::kInvalidArgument);

  auto exhausted_join = AsofJoinOperator::create(
      std::make_unique<ChunkSource>(std::vector<AccountedVectorChunk>{}),
      std::make_unique<ChunkSource>(std::vector<AccountedVectorChunk>{}), definition(false),
      {.maximum_left_rows = 2'048U,
       .maximum_right_rows = 2'048U,
       .maximum_equality_keys = 1U,
       .maximum_physical_ordering_keys = 1U,
       .maximum_state_bytes = 1U});
  ASSERT_FALSE(exhausted_join.has_value());
  EXPECT_EQ(exhausted_join.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(AsofJoinOperatorTest, RuntimeShapeFailureCancelsAndReleasesBothSides) {
  QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
  std::vector<AccountedVectorChunk> left;
  left.push_back(left_input(resources));
  std::vector<AccountedVectorChunk> right;
  right.push_back(right_input(resources));
  VectorAsofJoinDefinition hostile = definition(true);
  hostile.left_input_columns[2U].nullable = false;
  auto join =
      AsofJoinOperator::create(std::make_unique<ChunkSource>(std::move(left)),
                               std::make_unique<ChunkSource>(std::move(right)), std::move(hostile))
          .value();
  auto step = join->next(resources);
  ASSERT_FALSE(step.has_value());
  EXPECT_EQ(step.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(resources.is_cancelled());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(AsofJoinOperatorPropertyTest, MatchesIndependentDeterministicSmallModel) {
  std::uint64_t random = 0x41534f465f4d4f44ULL;
  for (std::size_t example = 0U; example < 32U; ++example) {
    SCOPED_TRACE(example);
    std::vector<ModelLeftRow> left_rows;
    std::vector<ModelRightRow> right_rows;
    for (std::size_t row = 0U; row < 9U; ++row) {
      random = random * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
      left_rows.push_back(
          {.key = random % 7U == 0U
                      ? std::nullopt
                      : std::optional<std::int64_t>{static_cast<std::int64_t>(random % 4U)},
           .time = random % 11U == 0U ? std::nullopt
                                      : std::optional<std::int64_t>{static_cast<std::int64_t>(
                                            (random >> 8U) % 20U)},
           .payload = static_cast<std::int64_t>(row)});
    }
    for (std::size_t row = 0U; row < 19U; ++row) {
      random = random * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
      right_rows.push_back(
          {.key = random % 7U == 0U
                      ? std::nullopt
                      : std::optional<std::int64_t>{static_cast<std::int64_t>(random % 4U)},
           .time = random % 11U == 0U ? std::nullopt
                                      : std::optional<std::int64_t>{static_cast<std::int64_t>(
                                            (random >> 8U) % 20U)},
           .payload = static_cast<std::int64_t>(100U + row),
           .physical = random % 13U == 0U ? std::nullopt
                                          : std::optional<std::int64_t>{static_cast<std::int64_t>(
                                                (random >> 16U) % 5U)},
           .sequence = static_cast<std::uint64_t>(row + 1U),
           .row_ordinal = static_cast<std::uint32_t>(row & 3U)});
    }
    std::vector<std::optional<std::int64_t>> expected;
    for (const ModelLeftRow& left : left_rows) {
      const ModelRightRow* winner = nullptr;
      for (const ModelRightRow& right : right_rows) {
        if (!left.key.has_value() || !right.key.has_value() || left.key != right.key ||
            !left.time.has_value() || !right.time.has_value() || right.time > left.time) {
          continue;
        }
        if (winner == nullptr || later_model(right, *winner))
          winner = std::addressof(right);
      }
      expected.push_back(winner == nullptr ? std::nullopt
                                           : std::optional<std::int64_t>{winner->payload});
    }

    QueryResourceContext resources = QueryResourceContext::create(8U << 20U).value();
    std::vector<AccountedVectorChunk> left_chunks;
    left_chunks.push_back(model_left_input(left_rows, resources));
    std::vector<AccountedVectorChunk> right_chunks;
    right_chunks.push_back(model_right_input(right_rows, resources));
    auto join = AsofJoinOperator::create(std::make_unique<ChunkSource>(std::move(left_chunks)),
                                         std::make_unique<ChunkSource>(std::move(right_chunks)),
                                         model_definition())
                    .value();
    auto step = join->next(resources).value();
    ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
    const VectorChunk& output = step.chunk()->chunk();
    ASSERT_EQ(output.selected_row_count(), expected.size());
    for (std::size_t row = 0U; row < expected.size(); ++row) {
      EXPECT_EQ(read_i64(output, 1U, row), expected[row]);
      EXPECT_EQ(output.cell({2U, row}).value().boolean().value(), expected[row].has_value());
    }
    step = PhysicalOperatorStep::end();
    join.reset();
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
}

} // namespace
} // namespace chronos::query
