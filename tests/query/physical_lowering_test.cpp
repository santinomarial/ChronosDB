#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/evaluator.hpp"
#include "chronos/query/executor.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/snapshot.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/table_schema.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> schema(const bool deduplicated = true) {
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(3U), "ts",
                                                     type(schema::LogicalTypeKind::kTimestampNs),
                                                     false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(4U), "value",
                                                     type(schema::LogicalTypeKind::kInt64), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(5U), "flag",
                                                     type(schema::LogicalTypeKind::kBool), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(6U), "label",
                                                     type(schema::LogicalTypeKind::kString), true)
                        .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          id<schema::TableId>(1U), id<schema::SchemaId>(2U), schema::SchemaVersion::initial(),
          std::nullopt, std::move(columns),
          {.event_time_column = id<schema::ColumnId>(3U),
           .physical_ordering_key = {id<schema::ColumnId>(3U)},
           .partition_columns = {id<schema::ColumnId>(3U)},
           .shard_key = {id<schema::ColumnId>(3U)},
           .deduplication_key = deduplicated ? std::vector{id<schema::ColumnId>(3U)}
                                             : std::vector<schema::ColumnId>{}})
          .value());
}

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> catalog() {
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = schema()}};
  return std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
}

[[nodiscard]] BoundSqlSelect bind(const std::string_view sql) {
  return bind_sql_v1_select(parse_sql_v1_select(sql).value(), catalog()).value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn
signed_column(const schema::LogicalTypeKind kind, const std::span<const std::int64_t> values) {
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
             {.type = type(kind),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
             std::move(buffers))
      .value();
}

[[nodiscard]] columnar::OwnedPhysicalColumn bool_column(const std::span<const bool> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(columnar::bitmap_size(static_cast<std::uint32_t>(values.size())));
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (values[row])
      buffers.values[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kBool),
              .nullable = false,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = 0U},
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
uuid_column(const std::span<const common::Uuid> values) {
  columnar::ColumnVectorBuffers buffers;
  buffers.values.resize(values.size() * common::Uuid::kSize);
  for (std::size_t row = 0U; row < values.size(); ++row) {
    std::ranges::copy(values[row].bytes(), buffers.values.begin() + static_cast<std::ptrdiff_t>(
                                                                        row * common::Uuid::kSize));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kUuid),
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
  const auto append_offset = [&buffers](const std::uint32_t value) {
    for (std::size_t byte = 0U; byte < sizeof(value); ++byte)
      buffers.offsets.push_back(static_cast<std::byte>((value >> (byte * 8U)) & 0xffU));
  };
  append_offset(0U);
  std::uint32_t null_count = 0U;
  for (std::size_t row = 0U; row < values.size(); ++row) {
    if (!values[row].has_value()) {
      ++null_count;
    } else {
      buffers.validity[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
      for (const char byte : values[row].value()) // NOLINT(bugprone-unchecked-optional-access)
        buffers.values.push_back(static_cast<std::byte>(byte));
    }
    append_offset(static_cast<std::uint32_t>(buffers.values.size()));
  }
  return columnar::OwnedPhysicalColumn::create(
             {.type = type(schema::LogicalTypeKind::kString),
              .nullable = true,
              .row_count = static_cast<std::uint32_t>(values.size()),
              .null_count = null_count},
             std::move(buffers))
      .value();
}

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

class OneSnapshotProvider final : public ScalarSnapshotProvider {
public:
  explicit OneSnapshotProvider(std::shared_ptr<const ScalarTableSnapshot> snapshot)
      : snapshot_(std::move(snapshot)) {}

  [[nodiscard]] common::Result<std::shared_ptr<const ScalarTableSnapshot>>
  resolve(const std::shared_ptr<const schema::TableSchema>&,
          const std::optional<std::int64_t>) const override {
    return snapshot_;
  }

private:
  std::shared_ptr<const ScalarTableSnapshot> snapshot_;
};

[[nodiscard]] AccountedVectorChunk input(const QueryResourceContext& resources) {
  constexpr std::array<std::int64_t, 4> kTimestamp{1, 2, 3, 4};
  constexpr std::array<std::int64_t, 4> kValue{0, 3, 5, 8};
  constexpr std::array<bool, 4> kFlag{true, true, false, true};
  const std::array<std::optional<std::string>, 4> labels{"alpha", std::nullopt, "ccc", "delta"};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(signed_column(schema::LogicalTypeKind::kTimestampNs, kTimestamp));
  columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, kValue));
  columns.push_back(bool_column(kFlag));
  columns.push_back(string_column(labels));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(4U).value()).value();
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(4'096U).value(),
                                      resources)
      .value();
}

[[nodiscard]] std::shared_ptr<const ScalarTableSnapshot> input_scalar_snapshot() {
  constexpr std::array<std::int64_t, 4> kTimestamp{1, 2, 3, 4};
  constexpr std::array<std::int64_t, 4> kValue{0, 3, 5, 8};
  constexpr std::array<bool, 4> kFlag{true, true, false, true};
  const std::array<std::optional<std::string>, 4> labels{"alpha", std::nullopt, "ccc", "delta"};
  common::Uuid::Bytes wal_bytes{};
  wal_bytes.front() = std::byte{1U};
  const common::Uuid wal{wal_bytes};
  std::vector<ScalarInputRow> rows;
  rows.reserve(kTimestamp.size());
  for (std::size_t row = 0U; row < kTimestamp.size(); ++row) {
    std::vector<ScalarValue> columns;
    columns.push_back(
        ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), kTimestamp[row])
            .value());
    columns.push_back(
        ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), kValue[row]).value());
    columns.push_back(ScalarValue::boolean(kFlag[row]).value());
    if (labels[row].has_value()) {
      columns.push_back(
          ScalarValue::text(type(schema::LogicalTypeKind::kString),
                            labels[row].value()) // NOLINT(bugprone-unchecked-optional-access)
              .value());
    } else {
      columns.push_back(ScalarValue::null(type(schema::LogicalTypeKind::kString)));
    }
    rows.push_back({.columns = std::move(columns),
                    .generated_logical_identity = {},
                    .wal_id = wal,
                    .record_sequence = row + 1U,
                    .system_commit_position = row + 1U,
                    .row_ordinal = 0U});
  }
  return std::make_shared<const ScalarTableSnapshot>(
      ScalarTableSnapshot::create(schema(), 10U, std::move(rows)).value());
}

[[nodiscard]] AccountedVectorChunk ordered_input(const QueryResourceContext& resources) {
  constexpr std::array<std::int64_t, 6> kTimestamp{2, 1, 1, 1, 3, 4};
  constexpr std::array<std::int64_t, 6> kValue{7, 7, 7, 7, 2, 9};
  constexpr std::array<bool, 6> kFlag{true, false, true, false, true, true};
  const std::array<std::optional<std::string>, 6> labels{"late", "seq2",       "row1",
                                                         "row0", std::nullopt, "high"};
  const common::Uuid wal{common::Uuid::Bytes{
      std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
      std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
      std::byte{0}, std::byte{0}, std::byte{0}, std::byte{1}}};
  const std::array<common::Uuid, 6> wal_ids{wal, wal, wal, wal, wal, wal};
  constexpr std::array<std::uint64_t, 6> kRecordSequence{2, 2, 1, 1, 3, 4};
  constexpr std::array<std::uint32_t, 6> kRowOrdinal{0, 1, 1, 0, 0, 0};
  constexpr std::array<std::uint8_t, 6> kOperation{1, 1, 1, 1, 1, 1};
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(signed_column(schema::LogicalTypeKind::kTimestampNs, kTimestamp));
  columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, kValue));
  columns.push_back(bool_column(kFlag));
  columns.push_back(string_column(labels));
  columns.push_back(uuid_column(wal_ids));
  columns.push_back(unsigned_column(schema::LogicalTypeKind::kUInt64,
                                    std::span<const std::uint64_t>{kRecordSequence}));
  columns.push_back(unsigned_column(schema::LogicalTypeKind::kUInt32,
                                    std::span<const std::uint32_t>{kRowOrdinal}));
  columns.push_back(
      unsigned_column(schema::LogicalTypeKind::kUInt8, std::span<const std::uint8_t>{kOperation}));
  VectorChunk chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(6U).value()).value();
  return AccountedVectorChunk::create(std::move(chunk), resources.reserve(8'192U).value(),
                                      resources)
      .value();
}

[[nodiscard]] std::shared_ptr<const ScalarTableSnapshot> ordered_scalar_snapshot() {
  constexpr std::array<std::int64_t, 6> kTimestamp{2, 1, 1, 1, 3, 4};
  constexpr std::array<std::int64_t, 6> kValue{7, 7, 7, 7, 2, 9};
  constexpr std::array<bool, 6> kFlag{true, false, true, false, true, true};
  const std::array<std::optional<std::string>, 6> labels{"late", "seq2",       "row1",
                                                         "row0", std::nullopt, "high"};
  constexpr std::array<std::uint64_t, 6> kRecordSequence{2, 2, 1, 1, 3, 4};
  constexpr std::array<std::uint32_t, 6> kRowOrdinal{0, 1, 1, 0, 0, 0};
  const common::Uuid wal{common::Uuid::Bytes{
      std::byte{1}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
      std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0}, std::byte{0},
      std::byte{0}, std::byte{0}, std::byte{0}, std::byte{1}}};
  std::vector<ScalarInputRow> rows;
  rows.reserve(kTimestamp.size());
  for (std::size_t row = 0U; row < kTimestamp.size(); ++row) {
    std::vector<ScalarValue> columns;
    columns.push_back(
        ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), kTimestamp[row])
            .value());
    columns.push_back(
        ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), kValue[row]).value());
    columns.push_back(ScalarValue::boolean(kFlag[row]).value());
    const auto& label = labels[row];
    if (label.has_value()) {
      columns.push_back(
          ScalarValue::text(type(schema::LogicalTypeKind::kString), label.value()).value());
    } else {
      columns.push_back(ScalarValue::null(type(schema::LogicalTypeKind::kString)));
    }
    rows.push_back({.columns = std::move(columns),
                    .generated_logical_identity = {},
                    .wal_id = wal,
                    .record_sequence = kRecordSequence[row],
                    .system_commit_position = kRecordSequence[row],
                    .row_ordinal = kRowOrdinal[row]});
  }
  return std::make_shared<const ScalarTableSnapshot>(
      ScalarTableSnapshot::create(schema(), 10U, std::move(rows)).value());
}

[[nodiscard]] std::int64_t cell_i64(const VectorChunk& chunk, const std::size_t column,
                                    const std::size_t row) {
  const common::ByteView bytes =
      chunk.cell({.column_ordinal = column, .selected_row = row}).value().bytes().value();
  std::uint64_t bits = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bits |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[index])) << (index * 8U);
  return std::bit_cast<std::int64_t>(bits);
}

[[nodiscard]] ScalarValue cell_value(const VectorChunk& chunk, const std::size_t column,
                                     const std::size_t row) {
  const columnar::PhysicalColumnView* physical = chunk.column(column);
  EXPECT_NE(physical, nullptr);
  return ScalarValue::from_column_cell(
             physical->type(), chunk.cell({.column_ordinal = column, .selected_row = row}).value())
      .value();
}

[[nodiscard]] std::string cell_text(const VectorChunk& chunk, const std::size_t column,
                                    const std::size_t row) {
  const common::ByteView bytes =
      chunk.cell({.column_ordinal = column, .selected_row = row}).value().bytes().value();
  std::string result;
  result.reserve(bytes.size());
  for (const std::byte byte : bytes)
    result.push_back(static_cast<char>(std::to_integer<unsigned char>(byte)));
  return result;
}

TEST(PhysicalSelectLoweringTest, LowersWhereProjectionAndLimitIntoExactStageOrder) {
  BoundSqlSelect select = bind("SELECT value + 2 AS adjusted, 7 AS constant FROM metrics "
                               "WHERE flag AND value BETWEEN 1 AND 9 LIMIT 2");
  SqlResult<PhysicalPipelinePlan> plan = lower_bound_sql_select(select);
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();
  ASSERT_EQ(plan->input_columns().size(), 4U);
  ASSERT_EQ(plan->output_columns().size(), 2U);
  EXPECT_EQ(plan->output_columns()[0].type.kind(), schema::LogicalTypeKind::kInt64);
  ASSERT_EQ(plan->stages().size(), 4U);
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan->stages()[0]));
  EXPECT_TRUE(std::holds_alternative<BooleanFilterStage>(plan->stages()[1]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan->stages()[2]));
  EXPECT_TRUE(std::holds_alternative<LimitStage>(plan->stages()[3]));

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline = plan->instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step.chunk()->chunk().selected_row_count(), 2U);
  EXPECT_EQ(cell_i64(step.chunk()->chunk(), 0U, 0U), 5);
  EXPECT_EQ(cell_i64(step.chunk()->chunk(), 0U, 1U), 10);
  EXPECT_EQ(cell_i64(step.chunk()->chunk(), 1U, 0U), 7);
  EXPECT_EQ(cell_i64(step.chunk()->chunk(), 1U, 1U), 7);
}

TEST(PhysicalAsofLoweringTest, LowersAliasesHiddenOrderKeysWhereAndLimitInExactOrder) {
  BoundSqlSelect select = bind("SELECT l.value AS lv, r.label AS rl FROM metrics AS l "
                               "ASOF LEFT JOIN metrics AS r ON l.value = r.value AND r.ts <= l.ts "
                               "WHERE l.flag ORDER BY r.ts DESC NULLS LAST, lv ASC LIMIT 3");
  SqlResult<PhysicalAsofPlan> lowered = lower_bound_sql_asof_select(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  PhysicalAsofPlan plan = std::move(*lowered);
  ASSERT_EQ(plan.source_count(), 2U);
  ASSERT_EQ(plan.joins().size(), 1U);
  EXPECT_TRUE(plan.joins()[0].definition.left_outer);
  EXPECT_EQ(plan.joins()[0].definition.equality_keys.size(), 1U);
  ASSERT_EQ(plan.final_pipeline().output_columns().size(), 2U);
  EXPECT_TRUE(plan.final_pipeline().output_columns()[1].nullable);
  const auto stages = plan.final_pipeline().stages();
  ASSERT_EQ(stages.size(), 6U);
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(stages[0]));
  EXPECT_TRUE(std::holds_alternative<BooleanFilterStage>(stages[1]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(stages[2]));
  EXPECT_TRUE(std::holds_alternative<SortStage>(stages[3]));
  EXPECT_TRUE(std::holds_alternative<ColumnSubsetStage>(stages[4]));
  EXPECT_TRUE(std::holds_alternative<LimitStage>(stages[5]));
  const auto& sort = std::get<SortStage>(stages[3]);
  ASSERT_EQ(sort.keys.size(), 11U);
  EXPECT_EQ(sort.keys[0].direction, PhysicalSortDirection::kDescending);
  EXPECT_EQ(sort.keys[0].null_placement, ScalarNullPlacement::kLast);

  QueryResourceContext resources = QueryResourceContext::create(32U << 20U).value();
  std::vector<std::unique_ptr<PhysicalOperator>> sources;
  sources.push_back(std::make_unique<OneChunkSource>(ordered_input(resources)));
  sources.push_back(std::make_unique<OneChunkSource>(ordered_input(resources)));
  auto pipeline = plan.instantiate(std::move(sources));
  ASSERT_TRUE(pipeline.has_value()) << pipeline.error().message();
  auto step = (*pipeline)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().message();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step->chunk()->chunk();
  ASSERT_EQ(output.column_count(), 2U);
  ASSERT_EQ(output.selected_row_count(), 3U);
  EXPECT_EQ(cell_i64(output, 0U, 0U), 9);
  EXPECT_EQ(cell_text(output, 1U, 0U), "high");
  EXPECT_EQ(cell_i64(output, 0U, 1U), 2);
  EXPECT_TRUE(output.cell({1U, 1U}).value().is_null());
  EXPECT_EQ(cell_i64(output, 0U, 2U), 7);
  EXPECT_EQ(cell_text(output, 1U, 2U), "late");
  OneSnapshotProvider scalar_source{ordered_scalar_snapshot()};
  SqlResult<ScalarQueryResult> scalar = execute_sql_v1_select(select, scalar_source);
  ASSERT_TRUE(scalar.has_value()) << scalar.error().status().to_string();
  ASSERT_EQ(scalar->rows().size(), output.selected_row_count());
  for (std::size_t row = 0U; row < scalar->rows().size(); ++row) {
    ASSERT_EQ(scalar->rows()[row].size(), output.column_count());
    for (std::size_t column = 0U; column < scalar->rows()[row].size(); ++column)
      EXPECT_EQ(compare_scalar_values(cell_value(output, column, row), scalar->rows()[row][column],
                                      ScalarNullPlacement::kLast)
                    .value(),
                0);
  }
  step = PhysicalOperatorStep::end();
  (*pipeline).reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalAsofLoweringTest, LowersGroupedAggregateAndMultiplePriorSourceJoin) {
  auto grouped_parsed =
      parse_sql_v1_select("SELECT r.value AS grp, count(*) AS n, max(r.label) AS high "
                          "FROM metrics AS l ASOF LEFT JOIN metrics AS r "
                          "ON l.value = r.value AND r.ts <= l.ts "
                          "GROUP BY r.value ORDER BY n DESC, grp ASC");
  ASSERT_TRUE(grouped_parsed.has_value()) << grouped_parsed.error().status().to_string();
  SqlResult<BoundSqlSelect> grouped_bound =
      bind_sql_v1_select(std::move(*grouped_parsed), catalog());
  ASSERT_TRUE(grouped_bound.has_value()) << grouped_bound.error().status().to_string();
  BoundSqlSelect grouped = std::move(*grouped_bound);
  SqlResult<PhysicalAsofPlan> grouped_lowered = lower_bound_sql_asof_select(grouped);
  ASSERT_TRUE(grouped_lowered.has_value()) << grouped_lowered.error().status().to_string();
  const auto grouped_stages = grouped_lowered->final_pipeline().stages();
  ASSERT_EQ(grouped_stages.size(), 5U);
  EXPECT_TRUE(std::holds_alternative<GroupedAggregateStage>(grouped_stages[1]));
  EXPECT_TRUE(std::holds_alternative<SortStage>(grouped_stages[3]));

  QueryResourceContext resources = QueryResourceContext::create(32U << 20U).value();
  std::vector<std::unique_ptr<PhysicalOperator>> sources;
  sources.push_back(std::make_unique<OneChunkSource>(ordered_input(resources)));
  sources.push_back(std::make_unique<OneChunkSource>(ordered_input(resources)));
  auto pipeline = grouped_lowered->instantiate(std::move(sources)).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step.chunk()->chunk();
  ASSERT_EQ(output.column_count(), 3U);
  ASSERT_EQ(output.selected_row_count(), 3U);
  EXPECT_EQ(cell_i64(output, 0U, 0U), 7);
  EXPECT_EQ(cell_i64(output, 1U, 0U), 4);
  EXPECT_EQ(cell_text(output, 2U, 0U), "seq2");
  step = PhysicalOperatorStep::end();
  pipeline.reset();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  auto chained_parsed =
      parse_sql_v1_select("SELECT x.label FROM metrics AS l "
                          "ASOF JOIN metrics AS r ON l.value = r.value AND r.ts <= l.ts "
                          "ASOF JOIN metrics AS x ON r.value = x.value AND x.ts <= r.ts");
  ASSERT_TRUE(chained_parsed.has_value()) << chained_parsed.error().status().to_string();
  SqlResult<BoundSqlSelect> chained_bound =
      bind_sql_v1_select(std::move(*chained_parsed), catalog());
  ASSERT_TRUE(chained_bound.has_value()) << chained_bound.error().status().to_string();
  BoundSqlSelect chained = std::move(*chained_bound);
  SqlResult<PhysicalAsofPlan> chained_lowered = lower_bound_sql_asof_select(chained);
  ASSERT_TRUE(chained_lowered.has_value()) << chained_lowered.error().status().to_string();
  EXPECT_EQ(chained_lowered->source_count(), 3U);
  EXPECT_EQ(chained_lowered->joins().size(), 2U);

  BoundSqlSelect computed = bind("SELECT r.ts FROM metrics AS l ASOF JOIN metrics AS r "
                                 "ON l.value + 0 = r.value + 0 AND l.flag = r.flag "
                                 "AND time_bucket(INTERVAL '1 nanosecond', r.ts) <= "
                                 "time_bucket(INTERVAL '1 nanosecond', l.ts)");
  SqlResult<PhysicalAsofPlan> computed_lowered = lower_bound_sql_asof_select(computed);
  ASSERT_TRUE(computed_lowered.has_value()) << computed_lowered.error().status().to_string();
  ASSERT_EQ(computed_lowered->joins()[0].definition.equality_keys.size(), 2U);
  EXPECT_FALSE(computed_lowered->final_pipeline().output_columns()[0].nullable);
}

TEST(PhysicalAsofLoweringTest, RejectsUnavailableJoinedIdentityAndHostileLimits) {
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = schema(false)}};
  auto no_identity_catalog = std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
  BoundSqlSelect no_identity =
      bind_sql_v1_select(
          parse_sql_v1_select("SELECT r.value FROM metrics AS l ASOF JOIN metrics AS r "
                              "ON l.value = r.value AND r.ts <= l.ts ORDER BY r.value")
              .value(),
          no_identity_catalog)
          .value();
  SqlResult<PhysicalAsofPlan> rejected = lower_bound_sql_asof_select(no_identity);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), SqlDiagnosticCode::kUnsupportedSyntax);

  BoundSqlSelect valid = bind("SELECT r.value FROM metrics AS l ASOF JOIN metrics AS r "
                              "ON l.value = r.value AND r.ts <= l.ts");
  SqlResult<PhysicalAsofPlan> limited =
      lower_bound_sql_asof_select(valid, {.output_limits = {.maximum_rows = 1U,
                                                            .maximum_columns = 1U,
                                                            .maximum_buffer_bytes = 1U,
                                                            .maximum_retained_buffer_bytes = 1U}});
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().code(), SqlDiagnosticCode::kResourceLimit);
}

TEST(PhysicalSelectLoweringTest, PreservesStarOrderAndVariableConstantShape) {
  BoundSqlSelect select = bind("SELECT *, 'fixed' AS fixed_label FROM metrics");
  SqlResult<PhysicalPipelinePlan> plan = lower_bound_sql_select(select);
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();
  ASSERT_EQ(plan->output_columns().size(), 5U);
  EXPECT_EQ(plan->output_columns()[0].type.kind(), schema::LogicalTypeKind::kTimestampNs);
  EXPECT_EQ(plan->output_columns()[1].type.kind(), schema::LogicalTypeKind::kInt64);
  EXPECT_EQ(plan->output_columns()[2].type.kind(), schema::LogicalTypeKind::kBool);
  EXPECT_EQ(plan->output_columns()[3].type.kind(), schema::LogicalTypeKind::kString);
  EXPECT_EQ(plan->output_columns()[4].type.kind(), schema::LogicalTypeKind::kString);

  BoundSqlSelect nullable_in = bind("SELECT NULL IN (1, 2) AS membership FROM metrics");
  SqlResult<PhysicalPipelinePlan> in_plan = lower_bound_sql_select(nullable_in);
  ASSERT_TRUE(in_plan.has_value()) << in_plan.error().status().to_string();
  ASSERT_EQ(in_plan->output_columns().size(), 1U);
  EXPECT_EQ(in_plan->output_columns()[0].type.kind(), schema::LogicalTypeKind::kBool);
  EXPECT_TRUE(in_plan->output_columns()[0].nullable);
}

TEST(PhysicalSelectLoweringTest, ExecutesCheckedCastsLazyCoalesceAndTimeBucket) {
  BoundSqlSelect select =
      bind("SELECT CAST(value AS FLOAT64) AS floating, "
           "coalesce(CAST(NULL AS INT8), CAST(value AS INT64), CAST(1000 AS INT8)) AS chosen, "
           "time_bucket(INTERVAL '1 second', "
           "TIMESTAMP '1969-12-31 23:59:59.500000000Z') AS bucket FROM metrics LIMIT 1");
  SqlResult<PhysicalPipelinePlan> plan = lower_bound_sql_select(select);
  ASSERT_TRUE(plan.has_value()) << plan.error().status().to_string();
  ASSERT_EQ(plan->output_columns().size(), 3U);
  EXPECT_EQ(plan->output_columns()[0].type.kind(), schema::LogicalTypeKind::kFloat64);
  EXPECT_EQ(plan->output_columns()[1].type.kind(), schema::LogicalTypeKind::kInt64);
  EXPECT_EQ(plan->output_columns()[2].type.kind(), schema::LogicalTypeKind::kTimestampNs);

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline = plan->instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step.chunk()->chunk().selected_row_count(), 1U);
  EXPECT_DOUBLE_EQ(std::get<double>(cell_value(step.chunk()->chunk(), 0U, 0U).storage()), 0.0);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 1U, 0U).storage()), 0);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 2U, 0U).storage()),
            -1'000'000'000);
}

TEST(PhysicalSelectLoweringTest, ExecutesTextCastCaseAndLazyCoalesce) {
  BoundSqlSelect select =
      bind("SELECT lower(CAST('ChRoNoS' AS SYMBOL)) AS lowered, "
           "upper(coalesce(CAST(NULL AS STRING), 'fallback')) AS chosen, "
           "lower('A') = 'a' AS compared, upper(CAST(NULL AS STRING)) IS NULL AS missing "
           "FROM metrics LIMIT 1");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
  EXPECT_EQ(plan.output_columns()[0].type.kind(), schema::LogicalTypeKind::kSymbol);
  EXPECT_EQ(plan.output_columns()[1].type.kind(), schema::LogicalTypeKind::kString);
  EXPECT_EQ(plan.output_columns()[2].type.kind(), schema::LogicalTypeKind::kBool);
  EXPECT_EQ(plan.output_columns()[3].type.kind(), schema::LogicalTypeKind::kBool);

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(cell_text(step.chunk()->chunk(), 0U, 0U), "chronos");
  EXPECT_EQ(cell_text(step.chunk()->chunk(), 1U, 0U), "FALLBACK");
  EXPECT_TRUE(std::get<bool>(cell_value(step.chunk()->chunk(), 2U, 0U).storage()));
  EXPECT_TRUE(std::get<bool>(cell_value(step.chunk()->chunk(), 3U, 0U).storage()));
}

TEST(PhysicalSelectLoweringTest, FiltersBorrowedSourceTextThroughBoundSql) {
  BoundSqlSelect select = bind("SELECT label, label IS NULL AS missing FROM metrics "
                               "WHERE upper(label) IN ('ALPHA', 'CCC') OR label IS NULL");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step.chunk()->chunk().selected_row_count(), 3U);
  EXPECT_EQ(cell_text(step.chunk()->chunk(), 0U, 0U), "alpha");
  EXPECT_TRUE(
      step.chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 1U}).value().is_null());
  EXPECT_EQ(cell_text(step.chunk()->chunk(), 0U, 2U), "ccc");
  EXPECT_FALSE(std::get<bool>(cell_value(step.chunk()->chunk(), 1U, 0U).storage()));
  EXPECT_TRUE(std::get<bool>(cell_value(step.chunk()->chunk(), 1U, 1U).storage()));
  EXPECT_FALSE(std::get<bool>(cell_value(step.chunk()->chunk(), 1U, 2U).storage()));
}

TEST(PhysicalSelectLoweringTest, ExecutesEveryGlobalAggregateAfterWhere) {
  BoundSqlSelect select =
      bind("SELECT count(*) AS rows, count(label) AS labels, sum(value) AS total, "
           "avg(value) AS mean, min(value) AS smallest, max(value) AS largest, "
           "var_pop(value) AS population_variance, var_samp(value) AS sample_variance "
           "FROM metrics WHERE flag");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
  ASSERT_EQ(plan.stages().size(), 4U);
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[0]));
  EXPECT_TRUE(std::holds_alternative<BooleanFilterStage>(plan.stages()[1]));
  EXPECT_TRUE(std::holds_alternative<UngroupedAggregateStage>(plan.stages()[2]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[3]));

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& result = step.chunk()->chunk();
  ASSERT_EQ(result.selected_row_count(), 1U);
  EXPECT_FALSE(result.column(0U)->nullable());
  EXPECT_FALSE(result.column(1U)->nullable());
  for (std::size_t column = 2U; column < result.column_count(); ++column) {
    EXPECT_TRUE(result.column(column)->nullable());
    EXPECT_EQ(result.column(column)->null_count(), 0U);
  }
  EXPECT_EQ(std::get<std::int64_t>(cell_value(result, 0U, 0U).storage()), 3);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(result, 1U, 0U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(result, 2U, 0U).storage()), 11);
  EXPECT_DOUBLE_EQ(std::get<double>(cell_value(result, 3U, 0U).storage()), 11.0 / 3.0);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(result, 4U, 0U).storage()), 0);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(result, 5U, 0U).storage()), 8);
  EXPECT_DOUBLE_EQ(std::get<double>(cell_value(result, 6U, 0U).storage()), 98.0 / 9.0);
  EXPECT_DOUBLE_EQ(std::get<double>(cell_value(result, 7U, 0U).storage()), 49.0 / 3.0);
}

TEST(PhysicalSelectLoweringTest, MaterializesComputedAggregateInputsAndFinalExpressions) {
  BoundSqlSelect select = bind("SELECT sum(value + 2) + count(*) AS combined, "
                               "count(lower(label)) AS labels FROM metrics WHERE flag");
  SqlResult<PhysicalPipelinePlan> lowered = lower_bound_sql_select(select);
  if (!lowered.has_value())
    FAIL() << lowered.error().status().to_string();
  PhysicalPipelinePlan plan = std::move(*lowered);
  ASSERT_EQ(plan.stages().size(), 5U);
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[2]));
  EXPECT_TRUE(std::holds_alternative<UngroupedAggregateStage>(plan.stages()[3]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[4]));

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto instantiated = plan.instantiate(std::make_unique<OneChunkSource>(input(resources)));
  if (!instantiated.has_value())
    FAIL() << instantiated.error().to_string();
  auto pipeline = std::move(*instantiated);
  auto pulled = pipeline->next(resources);
  if (!pulled.has_value())
    FAIL() << pulled.error().to_string();
  auto step = std::move(*pulled);
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& result = step.chunk()->chunk();
  EXPECT_TRUE(result.column(0U)->nullable());
  EXPECT_EQ(result.column(0U)->null_count(), 0U);
  EXPECT_FALSE(result.column(1U)->nullable());
  EXPECT_EQ(std::get<std::int64_t>(cell_value(result, 0U, 0U).storage()), 20);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(result, 1U, 0U).storage()), 2);
}

TEST(PhysicalSelectLoweringTest, PreservesEmptyGlobalAggregateAndLimitSemantics) {
  BoundSqlSelect select =
      bind("SELECT count(*) AS rows, sum(value) AS total FROM metrics WHERE value < 0 LIMIT 1");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_FALSE(step.chunk()->chunk().column(0U)->nullable());
  EXPECT_TRUE(step.chunk()->chunk().column(1U)->nullable());
  EXPECT_EQ(step.chunk()->chunk().column(1U)->null_count(), 1U);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 0U, 0U).storage()), 0);
  EXPECT_TRUE(cell_value(step.chunk()->chunk(), 1U, 0U).is_null());

  PhysicalPipelinePlan zero =
      lower_bound_sql_select(bind("SELECT count(*) AS rows FROM metrics LIMIT 0")).value();
  QueryResourceContext zero_resources = QueryResourceContext::create(1U << 20U).value();
  auto zero_pipeline =
      zero.instantiate(std::make_unique<OneChunkSource>(input(zero_resources))).value();
  EXPECT_EQ(zero_pipeline->next(zero_resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(zero_resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalSelectLoweringTest, ExecutesComputedGroupedAggregatesAfterWhereAndBeforeLimit) {
  BoundSqlSelect select = bind(
      "SELECT value % 3 AS bucket, count(*) AS rows, "
      "sum(value + 1) + count(*) AS combined FROM metrics WHERE flag GROUP BY value % 3 LIMIT 2");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
  ASSERT_EQ(plan.stages().size(), 6U);
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[0]));
  EXPECT_TRUE(std::holds_alternative<BooleanFilterStage>(plan.stages()[1]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[2]));
  EXPECT_TRUE(std::holds_alternative<GroupedAggregateStage>(plan.stages()[3]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[4]));
  EXPECT_TRUE(std::holds_alternative<LimitStage>(plan.stages()[5]));

  const auto& grouped = std::get<GroupedAggregateStage>(plan.stages()[3]);
  ASSERT_EQ(grouped.keys.size(), 1U);
  ASSERT_EQ(grouped.definitions.size(), 3U);
  EXPECT_EQ(grouped.keys[0].column_ordinal, 0U);
  ASSERT_TRUE(grouped.definitions[1].input.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  EXPECT_EQ(grouped.definitions[1].input->column_ordinal, 1U);

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto first = pipeline->next(resources).value();
  ASSERT_EQ(first.kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(first.chunk()->chunk(), 0U, 0U).storage()), 0);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(first.chunk()->chunk(), 1U, 0U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(first.chunk()->chunk(), 2U, 0U).storage()), 7);
  first = PhysicalOperatorStep::end();

  auto second = pipeline->next(resources).value();
  ASSERT_EQ(second.kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(second.chunk()->chunk(), 0U, 0U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(second.chunk()->chunk(), 1U, 0U).storage()), 1);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(second.chunk()->chunk(), 2U, 0U).storage()), 10);
  second = PhysicalOperatorStep::end();
  EXPECT_EQ(pipeline->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalSelectLoweringTest, GroupsDirectNullableVariableKeysAndPreservesEmptySemantics) {
  PhysicalPipelinePlan plan =
      lower_bound_sql_select(
          bind("SELECT label, count(*) AS rows FROM metrics WHERE flag GROUP BY label"))
          .value();
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();

  auto first = pipeline->next(resources).value();
  EXPECT_EQ(cell_text(first.chunk()->chunk(), 0U, 0U), "alpha");
  EXPECT_EQ(std::get<std::int64_t>(cell_value(first.chunk()->chunk(), 1U, 0U).storage()), 1);
  first = PhysicalOperatorStep::end();
  auto second = pipeline->next(resources).value();
  EXPECT_TRUE(cell_value(second.chunk()->chunk(), 0U, 0U).is_null());
  EXPECT_TRUE(second.chunk()->chunk().column(0U)->nullable());
  second = PhysicalOperatorStep::end();
  auto third = pipeline->next(resources).value();
  EXPECT_EQ(cell_text(third.chunk()->chunk(), 0U, 0U), "delta");
  third = PhysicalOperatorStep::end();
  EXPECT_EQ(pipeline->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);

  PhysicalPipelinePlan empty =
      lower_bound_sql_select(bind("SELECT label FROM metrics WHERE value < 0 GROUP BY label"))
          .value();
  QueryResourceContext empty_resources = QueryResourceContext::create(4U << 20U).value();
  auto empty_pipeline =
      empty.instantiate(std::make_unique<OneChunkSource>(input(empty_resources))).value();
  EXPECT_EQ(empty_pipeline->next(empty_resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(empty_resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalSelectLoweringTest, PreservesDeclaredMultipleGroupKeyOrder) {
  PhysicalPipelinePlan plan =
      lower_bound_sql_select(bind("SELECT flag, value % 3 AS bucket, count(*) AS rows "
                                  "FROM metrics GROUP BY flag, value % 3"))
          .value();
  const auto& grouped = std::get<GroupedAggregateStage>(plan.stages()[1]);
  ASSERT_EQ(grouped.keys.size(), 2U);
  EXPECT_EQ(grouped.keys[0].type.kind(), schema::LogicalTypeKind::kBool);
  EXPECT_EQ(grouped.keys[1].type.kind(), schema::LogicalTypeKind::kInt64);

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  std::vector<std::tuple<bool, std::int64_t, std::int64_t>> observed;
  for (;;) {
    auto step = pipeline->next(resources).value();
    if (step.kind() == PhysicalOperatorStepKind::kEnd)
      break;
    observed.emplace_back(
        std::get<bool>(cell_value(step.chunk()->chunk(), 0U, 0U).storage()),
        std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 1U, 0U).storage()),
        std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 2U, 0U).storage()));
  }
  std::ranges::sort(observed);
  const std::vector<std::tuple<bool, std::int64_t, std::int64_t>> expected{
      {false, 2, 1}, {true, 0, 2}, {true, 2, 1}};
  EXPECT_EQ(observed, expected);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalSelectLoweringPropertyTest, GroupedResultsMatchIndependentDeterministicModel) {
  struct ExpectedGroup {
    std::int64_t key;
    std::int64_t rows{};
    std::int64_t sum{};
    bool observed{};
  };
  BoundSqlSelect select =
      bind("SELECT value % 11 AS bucket, count(*) AS rows, sum(value) AS total FROM metrics "
           "WHERE flag GROUP BY value % 11");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();

  std::vector<std::int64_t> timestamps;
  std::vector<std::int64_t> values;
  std::vector<std::optional<std::string>> labels;
  std::array<bool, 257> flags{};
  std::vector<ExpectedGroup> expected;
  timestamps.reserve(257U);
  values.reserve(257U);
  labels.reserve(257U);
  std::uint64_t state = 0xbb67ae8584caa73bULL;
  for (std::size_t row = 0U; row < 257U; ++row) {
    state = state * 2'862'933'555'777'941'757ULL + 3'037'000'493ULL;
    const std::int64_t value = static_cast<std::int64_t>(state % 2'001U) - 1'000;
    const bool selected = (state & 3U) != 0U;
    timestamps.push_back(static_cast<std::int64_t>(row));
    values.push_back(value);
    flags[row] = selected;
    labels.emplace_back("x");
    if (!selected)
      continue;
    const std::int64_t key = value % 11;
    auto group = std::ranges::find_if(
        expected, [key](const ExpectedGroup& candidate) { return candidate.key == key; });
    if (group == expected.end()) {
      expected.push_back({.key = key});
      group = expected.end() - 1;
    }
    ++group->rows;
    group->sum += value;
  }

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(signed_column(schema::LogicalTypeKind::kTimestampNs, timestamps));
  columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, values));
  columns.push_back(bool_column(flags));
  columns.push_back(string_column(labels));
  VectorChunk source_chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(257U).value()).value();
  AccountedVectorChunk accounted =
      AccountedVectorChunk::create(std::move(source_chunk), resources.reserve(32'768U).value(),
                                   resources)
          .value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(std::move(accounted))).value();
  std::size_t actual_groups = 0U;
  for (;;) {
    auto step = pipeline->next(resources).value();
    if (step.kind() == PhysicalOperatorStepKind::kEnd)
      break;
    ++actual_groups;
    const std::int64_t key =
        std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 0U, 0U).storage());
    auto model = std::ranges::find_if(
        expected, [key](const ExpectedGroup& candidate) { return candidate.key == key; });
    ASSERT_NE(model, expected.end());
    EXPECT_FALSE(model->observed);
    model->observed = true;
    EXPECT_EQ(std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 1U, 0U).storage()),
              model->rows);
    EXPECT_EQ(std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 2U, 0U).storage()),
              model->sum);
  }
  EXPECT_EQ(actual_groups, expected.size());
  EXPECT_TRUE(
      std::ranges::all_of(expected, [](const ExpectedGroup& group) { return group.observed; }));
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalSelectLoweringTest, PropagatesRuntimeCastFailureAndCancelsThePipeline) {
  BoundSqlSelect select = bind("SELECT CAST(1000 AS INT8) AS invalid FROM metrics");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  common::Result<PhysicalOperatorStep> step = pipeline->next(resources);
  ASSERT_FALSE(step.has_value());
  EXPECT_EQ(step.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(resources.is_cancelled());
}

TEST(PhysicalSelectLoweringPropertyTest, FixedWidthKernelsMatchTheScalarOracle) {
  BoundSqlSelect select = bind(
      "SELECT CAST(value AS FLOAT64) AS floating, CAST(value AS DECIMAL(18,3)) AS decimal_value, "
      "coalesce(CAST(NULL AS INT8), CAST(value AS INT64)) AS chosen, "
      "time_bucket(INTERVAL '1 second', ts) AS bucket, "
      "UUID '00000000-0000-0000-0000-000000000001' = "
      "UUID '00000000-0000-0000-0000-000000000001' AS uuid_equal FROM metrics");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();

  std::vector<std::int64_t> timestamps;
  std::vector<std::int64_t> values;
  std::vector<std::optional<std::string>> labels;
  std::array<bool, 257> flags{};
  timestamps.reserve(257U);
  values.reserve(257U);
  labels.reserve(257U);
  std::uint64_t state = 0x6a09e667f3bcc909ULL;
  for (std::size_t row = 0U; row < 257U; ++row) {
    state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    timestamps.push_back(static_cast<std::int64_t>(state % 20'000'000'001ULL) - 10'000'000'000LL);
    state = state * 6'364'136'223'846'793'005ULL + 1'442'695'040'888'963'407ULL;
    values.push_back(static_cast<std::int64_t>(state % 2'000'001ULL) - 1'000'000LL);
    flags[row] = (state & 1U) != 0U;
    labels.emplace_back("x");
  }

  QueryResourceContext resources = QueryResourceContext::create(1U << 22U).value();
  std::vector<columnar::OwnedPhysicalColumn> columns;
  columns.push_back(signed_column(schema::LogicalTypeKind::kTimestampNs, timestamps));
  columns.push_back(signed_column(schema::LogicalTypeKind::kInt64, values));
  columns.push_back(bool_column(flags));
  columns.push_back(string_column(labels));
  VectorChunk source_chunk =
      VectorChunk::create(std::move(columns), VectorSelection::all(257U).value()).value();
  AccountedVectorChunk accounted =
      AccountedVectorChunk::create(std::move(source_chunk), resources.reserve(32'768U).value(),
                                   resources)
          .value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(std::move(accounted))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& actual = step.chunk()->chunk();
  ASSERT_EQ(actual.selected_row_count(), values.size());

  for (std::size_t row = 0U; row < values.size(); ++row) {
    std::array<ScalarValue, 4> source_values{
        ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), timestamps[row])
            .value(),
        ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), values[row]).value(),
        ScalarValue::boolean(flags[row]).value(),
        ScalarValue::text(type(schema::LogicalTypeKind::kString), "x").value()};
    const std::array<ScalarSourceRow, 1> sources{
        ScalarSourceRow{std::span<const ScalarValue>{source_values}}};
    const ScalarEvaluationContext context{.sources = sources};
    for (std::size_t output = 0U; output < select.syntax().items().size(); ++output) {
      const SqlExpression* expression = select.syntax().items()[output].expression();
      ASSERT_NE(expression, nullptr);
      SqlResult<ScalarValue> expected = evaluate_sql_v1_expression(select, *expression, context);
      ASSERT_TRUE(expected.has_value()) << expected.error().status().to_string();
      const ScalarValue observed = cell_value(actual, output, row);
      EXPECT_EQ(observed.type(), expected->type());
      EXPECT_EQ(observed.storage(), expected->storage());
    }
  }
}

TEST(PhysicalSelectLoweringTest,
     LowersBaseOrderKeysLogicalIdentityCommitPositionAndLimitBeforeHiddenRemoval) {
  BoundSqlSelect select =
      bind("SELECT label FROM metrics WHERE value >= 2 ORDER BY value ASC, flag DESC LIMIT 4");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
  ASSERT_EQ(plan.input_columns().size(), 8U);
  ASSERT_EQ(plan.output_columns().size(), 1U);
  ASSERT_EQ(plan.stages().size(), 6U);
  ASSERT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[0]));
  ASSERT_TRUE(std::holds_alternative<BooleanFilterStage>(plan.stages()[1]));
  ASSERT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[2]));
  ASSERT_TRUE(std::holds_alternative<SortStage>(plan.stages()[3]));
  ASSERT_TRUE(std::holds_alternative<ColumnSubsetStage>(plan.stages()[4]));
  ASSERT_TRUE(std::holds_alternative<LimitStage>(plan.stages()[5]));
  const auto& sort = std::get<SortStage>(plan.stages()[3]);
  ASSERT_EQ(sort.keys.size(), 6U);
  EXPECT_EQ(sort.keys[0].direction, PhysicalSortDirection::kAscending);
  EXPECT_EQ(sort.keys[1].direction, PhysicalSortDirection::kDescending);
  EXPECT_EQ(sort.keys[1].null_placement, ScalarNullPlacement::kFirst);
  for (std::size_t key = 2U; key < sort.keys.size(); ++key)
    EXPECT_EQ(sort.keys[key].direction, PhysicalSortDirection::kAscending);

  QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
  auto pipeline =
      plan.instantiate(std::make_unique<OneChunkSource>(ordered_input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step.chunk()->chunk();
  ASSERT_EQ(output.column_count(), 1U);
  ASSERT_EQ(output.selected_row_count(), 4U);
  EXPECT_TRUE(cell_value(output, 0U, 0U).is_null());
  EXPECT_EQ(cell_text(output, 0U, 1U), "row1");
  EXPECT_EQ(cell_text(output, 0U, 2U), "late");
  EXPECT_EQ(cell_text(output, 0U, 3U), "row0");
  OneSnapshotProvider provider{ordered_scalar_snapshot()};
  SqlResult<ScalarQueryResult> scalar = execute_sql_v1_select(select, provider);
  ASSERT_TRUE(scalar.has_value()) << scalar.error().status().to_string();
  ASSERT_EQ(scalar->rows().size(), output.selected_row_count());
  for (std::size_t row = 0U; row < scalar->rows().size(); ++row) {
    const ScalarValue physical = cell_value(output, 0U, row);
    EXPECT_EQ(physical.type(), scalar->rows()[row][0].type());
    EXPECT_EQ(physical.storage(), scalar->rows()[row][0].storage());
  }
  step = PhysicalOperatorStep::end();
  EXPECT_EQ(pipeline->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalSelectLoweringTest,
     LowersLatestByComputedTimestampMultipleKeysAndExactVersionTieAgainstScalarOracle) {
  const std::string_view sql =
      "SELECT flag, label AS winner FROM metrics LATEST BY (flag, value) ON "
      "time_bucket(INTERVAL '1 second', ts) "
      "ORDER BY flag ASC, value ASC";
  SqlResult<ParsedSqlSelect> parsed = parse_sql_v1_select(sql);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().status().to_string();
  SqlResult<BoundSqlSelect> bound = bind_sql_v1_select(std::move(*parsed), catalog());
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  BoundSqlSelect select = std::move(*bound);
  SqlResult<PhysicalPipelinePlan> lowered = lower_bound_sql_select(select);
  ASSERT_TRUE(lowered.has_value()) << lowered.error().status().to_string();
  PhysicalPipelinePlan plan = std::move(*lowered);
  ASSERT_EQ(plan.input_columns().size(), 8U);
  ASSERT_EQ(plan.output_columns().size(), 2U);
  ASSERT_EQ(plan.stages().size(), 6U);
  ASSERT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[0]));
  ASSERT_TRUE(std::holds_alternative<LatestByStage>(plan.stages()[1]));
  ASSERT_TRUE(std::holds_alternative<ColumnSubsetStage>(plan.stages()[2]));
  ASSERT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[3]));
  ASSERT_TRUE(std::holds_alternative<SortStage>(plan.stages()[4]));
  ASSERT_TRUE(std::holds_alternative<ColumnSubsetStage>(plan.stages()[5]));
  const LatestByStage& latest = std::get<LatestByStage>(plan.stages()[1]);
  EXPECT_EQ(latest.definition.key_column_ordinals, (std::vector<std::size_t>{2U, 1U}));
  EXPECT_EQ(latest.definition.timestamp_column_ordinal, 8U);
  EXPECT_EQ(latest.definition.physical_ordering_key_ordinals, (std::vector<std::size_t>{0U}));
  EXPECT_EQ(latest.definition.row_version_first_column_ordinal, 4U);

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto instantiated = plan.instantiate(std::make_unique<OneChunkSource>(ordered_input(resources)));
  ASSERT_TRUE(instantiated.has_value()) << instantiated.error().to_string();
  auto pipeline = std::move(*instantiated);
  auto pulled = pipeline->next(resources);
  ASSERT_TRUE(pulled.has_value()) << pulled.error().to_string();
  auto step = std::move(*pulled);
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step.chunk()->chunk();
  ASSERT_EQ(output.column_count(), 2U);
  ASSERT_EQ(output.selected_row_count(), 4U);
  EXPECT_FALSE(std::get<bool>(cell_value(output, 0U, 0U).storage()));
  EXPECT_EQ(cell_text(output, 1U, 0U), "seq2");
  EXPECT_TRUE(cell_value(output, 1U, 1U).is_null());
  EXPECT_EQ(cell_text(output, 1U, 2U), "late");
  EXPECT_EQ(cell_text(output, 1U, 3U), "high");

  OneSnapshotProvider provider{ordered_scalar_snapshot()};
  SqlResult<ScalarQueryResult> scalar = execute_sql_v1_select(select, provider);
  ASSERT_TRUE(scalar.has_value()) << scalar.error().status().to_string();
  ASSERT_EQ(scalar->rows().size(), output.selected_row_count());
  for (std::size_t row = 0U; row < scalar->rows().size(); ++row) {
    for (std::size_t column = 0U; column < scalar->rows()[row].size(); ++column) {
      const ScalarValue physical = cell_value(output, column, row);
      EXPECT_EQ(physical.type(), scalar->rows()[row][column].type());
      EXPECT_EQ(physical.storage(), scalar->rows()[row][column].storage());
    }
  }
  step = PhysicalOperatorStep::end();
  EXPECT_EQ(pipeline->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalSelectLoweringTest, PreservesLatestWhereAggregateOrderLimitStageOrder) {
  BoundSqlSelect select = bind("SELECT flag, count(*) AS rows FROM metrics LATEST BY (flag) ON "
                               "time_bucket(INTERVAL '1 second', ts) WHERE value < 9 GROUP BY flag "
                               "ORDER BY rows DESC, flag ASC LIMIT 1");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
  ASSERT_EQ(plan.output_columns().size(), 2U);
  ASSERT_EQ(plan.stages().size(), 11U);
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[0]));
  EXPECT_TRUE(std::holds_alternative<LatestByStage>(plan.stages()[1]));
  EXPECT_TRUE(std::holds_alternative<ColumnSubsetStage>(plan.stages()[2]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[3]));
  EXPECT_TRUE(std::holds_alternative<BooleanFilterStage>(plan.stages()[4]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[5]));
  EXPECT_TRUE(std::holds_alternative<GroupedAggregateStage>(plan.stages()[6]));
  EXPECT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[7]));
  EXPECT_TRUE(std::holds_alternative<SortStage>(plan.stages()[8]));
  EXPECT_TRUE(std::holds_alternative<ColumnSubsetStage>(plan.stages()[9]));
  EXPECT_TRUE(std::holds_alternative<LimitStage>(plan.stages()[10]));

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto pipeline =
      plan.instantiate(std::make_unique<OneChunkSource>(ordered_input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step.chunk()->chunk();
  ASSERT_EQ(output.selected_row_count(), 1U);
  EXPECT_FALSE(std::get<bool>(cell_value(output, 0U, 0U).storage()));
  EXPECT_EQ(std::get<std::int64_t>(cell_value(output, 1U, 0U).storage()), 1);
}

TEST(PhysicalSelectLoweringTest, PreservesAliasVisibilityAndExplicitOrDefaultNullPlacement) {
  for (const auto& [sql, null_first] : std::array<std::pair<std::string_view, bool>, 4>{
           std::pair{"SELECT label AS name FROM metrics ORDER BY name DESC", true},
           std::pair{"SELECT label AS name FROM metrics ORDER BY name DESC NULLS LAST", false},
           std::pair{"SELECT label AS name FROM metrics ORDER BY name ASC", false},
           std::pair{"SELECT label AS name FROM metrics ORDER BY name ASC NULLS FIRST", true}}) {
    SCOPED_TRACE(sql);
    PhysicalPipelinePlan plan = lower_bound_sql_select(bind(sql)).value();
    const auto& sort = std::get<SortStage>(plan.stages()[1]);
    ASSERT_FALSE(sort.keys.empty());
    EXPECT_EQ(sort.keys.front().null_placement,
              null_first ? ScalarNullPlacement::kFirst : ScalarNullPlacement::kLast);
    QueryResourceContext resources = QueryResourceContext::create(1U << 20U).value();
    auto pipeline =
        plan.instantiate(std::make_unique<OneChunkSource>(ordered_input(resources))).value();
    auto step = pipeline->next(resources).value();
    ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
    const VectorChunk& output = step.chunk()->chunk();
    EXPECT_EQ(cell_value(output, 0U, null_first ? 0U : 5U).is_null(), true);
    step = PhysicalOperatorStep::end();
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
}

TEST(PhysicalSelectLoweringTest,
     LowersAggregateAliasesOrderOnlyAggregatesGroupIdentityAndHiddenRemoval) {
  BoundSqlSelect select =
      bind("SELECT value % 3 AS bucket, count(*) AS rows FROM metrics GROUP BY value % 3 "
           "ORDER BY rows DESC, sum(value) DESC, bucket ASC LIMIT 2");
  PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
  ASSERT_EQ(plan.output_columns().size(), 2U);
  ASSERT_EQ(plan.stages().size(), 6U);
  ASSERT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[0]));
  ASSERT_TRUE(std::holds_alternative<GroupedAggregateStage>(plan.stages()[1]));
  ASSERT_TRUE(std::holds_alternative<ColumnOutputStage>(plan.stages()[2]));
  ASSERT_TRUE(std::holds_alternative<SortStage>(plan.stages()[3]));
  ASSERT_TRUE(std::holds_alternative<ColumnSubsetStage>(plan.stages()[4]));
  ASSERT_TRUE(std::holds_alternative<LimitStage>(plan.stages()[5]));
  const auto& grouped = std::get<GroupedAggregateStage>(plan.stages()[1]);
  ASSERT_EQ(grouped.definitions.size(), 2U);
  const auto& sort = std::get<SortStage>(plan.stages()[3]);
  ASSERT_EQ(sort.keys.size(), 4U);
  EXPECT_EQ(sort.keys.back().direction, PhysicalSortDirection::kAscending);
  EXPECT_EQ(sort.keys.back().null_placement, ScalarNullPlacement::kLast);

  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step.chunk()->chunk();
  ASSERT_EQ(output.column_count(), 2U);
  ASSERT_EQ(output.selected_row_count(), 2U);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(output, 0U, 0U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(output, 1U, 0U).storage()), 2);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(output, 0U, 1U).storage()), 0);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(output, 1U, 1U).storage()), 2);
  step = PhysicalOperatorStep::end();
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(PhysicalSelectLoweringTest, UsesEncodedGroupKeyIdentityForEqualAggregateOrderKeys) {
  PhysicalPipelinePlan plan =
      lower_bound_sql_select(bind("SELECT label, count(*) AS rows FROM metrics GROUP BY label "
                                  "ORDER BY rows DESC"))
          .value();
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  const VectorChunk& output = step.chunk()->chunk();
  ASSERT_EQ(output.selected_row_count(), 4U);
  EXPECT_EQ(cell_text(output, 0U, 0U), "alpha");
  EXPECT_EQ(cell_text(output, 0U, 1U), "ccc");
  EXPECT_EQ(cell_text(output, 0U, 2U), "delta");
  EXPECT_TRUE(cell_value(output, 0U, 3U).is_null());
}

TEST(PhysicalSelectLoweringTest, LowersOrderOnlyGlobalAggregateWithoutExposingIt) {
  PhysicalPipelinePlan plan =
      lower_bound_sql_select(bind("SELECT 1 AS one FROM metrics ORDER BY count(*) DESC")).value();
  ASSERT_EQ(plan.output_columns().size(), 1U);
  ASSERT_TRUE(std::ranges::any_of(plan.stages(), [](const PhysicalPipelineStage& stage) {
    return std::holds_alternative<UngroupedAggregateStage>(stage);
  }));
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto step = pipeline->next(resources).value();
  ASSERT_EQ(step.kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(step.chunk()->chunk().column_count(), 1U);
  EXPECT_EQ(std::get<std::int64_t>(cell_value(step.chunk()->chunk(), 0U, 0U).storage()), 1);
}

TEST(PhysicalSelectLoweringTest, RejectsBaseOrderWhenGeneratedLogicalIdentityIsUnavailable) {
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = schema(false)}};
  auto no_identity_catalog = std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(2U, tables).value());
  BoundSqlSelect select =
      bind_sql_v1_select(parse_sql_v1_select("SELECT value FROM metrics ORDER BY value").value(),
                         std::move(no_identity_catalog))
          .value();
  SqlResult<PhysicalPipelinePlan> lowered = lower_bound_sql_select(select);
  ASSERT_FALSE(lowered.has_value());
  EXPECT_EQ(lowered.error().code(), SqlDiagnosticCode::kUnsupportedSyntax);
  EXPECT_EQ(lowered.error().status().code(), common::StatusCode::kInvalidArgument);
}

TEST(PhysicalSelectLoweringTest, LowersVariableExtremaAgainstTheScalarOracle) {
  for (const std::string_view sql :
       {"SELECT min(label) AS first_label, max(label) AS last_label FROM metrics",
        "SELECT flag, min(label) AS first_label, max(label) AS last_label FROM metrics "
        "GROUP BY flag ORDER BY flag"}) {
    SCOPED_TRACE(sql);
    BoundSqlSelect select = bind(sql);
    PhysicalPipelinePlan plan = lower_bound_sql_select(select).value();
    QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
    auto pipeline = plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
    std::vector<std::vector<ScalarValue>> physical_rows;
    for (;;) {
      auto step = pipeline->next(resources).value();
      if (step.kind() == PhysicalOperatorStepKind::kEnd)
        break;
      const VectorChunk& chunk = step.chunk()->chunk();
      for (std::size_t row = 0U; row < chunk.selected_row_count(); ++row) {
        std::vector<ScalarValue> values;
        values.reserve(chunk.column_count());
        for (std::size_t column = 0U; column < chunk.column_count(); ++column)
          values.push_back(cell_value(chunk, column, row));
        physical_rows.push_back(std::move(values));
      }
    }
    OneSnapshotProvider provider{input_scalar_snapshot()};
    SqlResult<ScalarQueryResult> scalar = execute_sql_v1_select(select, provider);
    ASSERT_TRUE(scalar.has_value()) << scalar.error().status().to_string();
    ASSERT_EQ(physical_rows.size(), scalar->rows().size());
    for (std::size_t row = 0U; row < physical_rows.size(); ++row) {
      ASSERT_EQ(physical_rows[row].size(), scalar->rows()[row].size());
      for (std::size_t column = 0U; column < physical_rows[row].size(); ++column) {
        EXPECT_EQ(physical_rows[row][column].type(), scalar->rows()[row][column].type());
        EXPECT_EQ(physical_rows[row][column].storage(), scalar->rows()[row][column].storage());
      }
    }
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
}

TEST(PhysicalSelectLoweringTest, RejectsUnsupportedRelationalAndScalarSurfaces) {
  BoundSqlSelect text_cast = bind("SELECT CAST('value' AS SYMBOL) AS converted FROM metrics");
  EXPECT_TRUE(lower_bound_sql_select(text_cast).has_value());
  BoundSqlSelect ordered = bind("SELECT value FROM metrics ORDER BY value");
  EXPECT_TRUE(lower_bound_sql_select(ordered).has_value());
  BoundSqlSelect grouped = bind("SELECT sum(value) AS total FROM metrics GROUP BY flag");
  EXPECT_TRUE(lower_bound_sql_select(grouped).has_value());
  BoundSqlSelect variable_extremum = bind("SELECT min(label) AS first_label FROM metrics");
  PhysicalPipelinePlan variable_plan = lower_bound_sql_select(variable_extremum).value();
  QueryResourceContext resources = QueryResourceContext::create(4U << 20U).value();
  auto variable_pipeline =
      variable_plan.instantiate(std::make_unique<OneChunkSource>(input(resources))).value();
  auto variable_step = variable_pipeline->next(resources).value();
  EXPECT_EQ(cell_text(variable_step.chunk()->chunk(), 0U, 0U), "alpha");
  BoundSqlSelect text_comparison = bind("SELECT 'a' = 'b' AS compared FROM metrics");
  EXPECT_TRUE(lower_bound_sql_select(text_comparison).has_value());
  BoundSqlSelect subscribe = bind("SUBSCRIBE SELECT value FROM metrics");
  EXPECT_EQ(lower_bound_sql_select(subscribe).error().code(),
            SqlDiagnosticCode::kUnsupportedSyntax);
  BoundSqlSelect explain = bind("EXPLAIN SELECT value FROM metrics");
  EXPECT_EQ(lower_bound_sql_select(explain).error().code(), SqlDiagnosticCode::kUnsupportedSyntax);
  BoundSqlSelect analyze = bind("EXPLAIN ANALYZE SELECT value FROM metrics");
  EXPECT_EQ(lower_bound_sql_select(analyze).error().code(), SqlDiagnosticCode::kUnsupportedSyntax);
}

TEST(PhysicalSelectLoweringTest, EnforcesExpressionAndPlanLimitsBeforeExecution) {
  BoundSqlSelect select =
      bind("SELECT value + 1 AS adjusted FROM metrics WHERE value IN (1, 2, 3)");
  auto lowered = lower_bound_sql_select(
      select, {.expression_limits = {.maximum_instructions = 2U,
                                     .maximum_retained_configuration_bytes = 4'096U}});
  ASSERT_FALSE(lowered.has_value());
  EXPECT_EQ(lowered.error().code(), SqlDiagnosticCode::kResourceLimit);
  EXPECT_EQ(lowered.error().status().code(), common::StatusCode::kResourceExhausted);

  BoundSqlSelect exact = bind("SELECT value + 1 AS adjusted FROM metrics");
  EXPECT_TRUE(lower_bound_sql_select(
                  exact, {.expression_limits = {.maximum_instructions = 3U,
                                                .maximum_retained_configuration_bytes = 4'096U}})
                  .has_value());

  BoundSqlSelect exact_bucket =
      bind("SELECT time_bucket(INTERVAL '1 second', ts) AS bucket FROM metrics");
  EXPECT_TRUE(
      lower_bound_sql_select(
          exact_bucket, {.expression_limits = {.maximum_instructions = 3U,
                                               .maximum_retained_configuration_bytes = 4'096U}})
          .has_value());
  auto short_bucket = lower_bound_sql_select(
      exact_bucket, {.expression_limits = {.maximum_instructions = 2U,
                                           .maximum_retained_configuration_bytes = 4'096U}});
  ASSERT_FALSE(short_bucket.has_value());
  EXPECT_EQ(short_bucket.error().code(), SqlDiagnosticCode::kResourceLimit);

  BoundSqlSelect aggregates =
      bind("SELECT sum(value), avg(value), min(value), max(value) FROM metrics");
  auto narrow_aggregate =
      lower_bound_sql_select(aggregates, {.aggregate_limits = {.maximum_aggregates = 3U}});
  ASSERT_FALSE(narrow_aggregate.has_value());
  EXPECT_EQ(narrow_aggregate.error().code(), SqlDiagnosticCode::kResourceLimit);

  BoundSqlSelect grouped = bind("SELECT flag, count(*), sum(value) FROM metrics GROUP BY flag");
  auto narrow_grouped =
      lower_bound_sql_select(grouped, {.grouped_aggregate_limits = {.maximum_aggregates = 1U}});
  ASSERT_FALSE(narrow_grouped.has_value());
  EXPECT_EQ(narrow_grouped.error().code(), SqlDiagnosticCode::kResourceLimit);

  BoundSqlSelect ordered = bind("SELECT value FROM metrics ORDER BY value");
  auto narrow_sort = lower_bound_sql_select(ordered, {.sort_limits = {.maximum_keys = 4U}});
  ASSERT_FALSE(narrow_sort.has_value());
  EXPECT_EQ(narrow_sort.error().code(), SqlDiagnosticCode::kResourceLimit);

  auto narrow_hidden_output =
      lower_bound_sql_select(ordered, {.output_limits = {.maximum_columns = 5U}});
  ASSERT_FALSE(narrow_hidden_output.has_value());
  EXPECT_EQ(narrow_hidden_output.error().code(), SqlDiagnosticCode::kResourceLimit);
}

} // namespace
} // namespace chronos::query
