#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/evaluator.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/table_schema.hpp"

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

[[nodiscard]] std::shared_ptr<const schema::TableSchema> schema() {
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
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = id<schema::ColumnId>(3U),
                                   .physical_ordering_key = {id<schema::ColumnId>(3U)},
                                   .partition_columns = {id<schema::ColumnId>(3U)},
                                   .shard_key = {id<schema::ColumnId>(3U)},
                                   .deduplication_key = {}})
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

TEST(PhysicalSelectLoweringTest, RejectsUnsupportedRelationalAndScalarSurfaces) {
  BoundSqlSelect text_cast = bind("SELECT CAST('value' AS SYMBOL) AS converted FROM metrics");
  EXPECT_TRUE(lower_bound_sql_select(text_cast).has_value());
  BoundSqlSelect ordered = bind("SELECT value FROM metrics ORDER BY value");
  EXPECT_EQ(lower_bound_sql_select(ordered).error().code(), SqlDiagnosticCode::kUnsupportedSyntax);
  BoundSqlSelect aggregate = bind("SELECT sum(value) AS total FROM metrics");
  EXPECT_EQ(lower_bound_sql_select(aggregate).error().code(),
            SqlDiagnosticCode::kUnsupportedSyntax);
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
}

} // namespace
} // namespace chronos::query
