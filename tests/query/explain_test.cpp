#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/explain.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/snapshot.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

struct ExplainFixtures {
  std::shared_ptr<const schema::TableSchema> schema;
  std::shared_ptr<const QueryCatalogSnapshot> catalog;
};

[[nodiscard]] ExplainFixtures fixtures() {
  const schema::ColumnId timestamp = id<schema::ColumnId>(3U);
  const schema::ColumnId value = id<schema::ColumnId>(4U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        timestamp, "ts", type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(
      schema::ColumnDefinition::create(value, "value", type(schema::LogicalTypeKind::kInt64), true)
          .value());
  auto table = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = timestamp,
                                   .physical_ordering_key = {timestamp},
                                   .partition_columns = {timestamp},
                                   .shard_key = {timestamp},
                                   .deduplication_key = {timestamp}})
          .value());
  const std::array input{QueryCatalogTableInput{.name = "t", .quoted = false, .schema = table}};
  auto catalog =
      std::make_shared<const QueryCatalogSnapshot>(QueryCatalogSnapshot::create(7U, input).value());
  return {.schema = std::move(table), .catalog = std::move(catalog)};
}

[[nodiscard]] BoundSqlSelect bind(const std::string_view sql, const ExplainFixtures& data) {
  return bind_sql_v1_select(parse_sql_v1_select(sql).value(), data.catalog).value();
}

[[nodiscard]] ScalarInputRow row(const std::int64_t timestamp, const std::int64_t value,
                                 const std::uint64_t sequence) {
  common::Uuid::Bytes wal{};
  wal.front() = std::byte{8U};
  return ScalarInputRow{
      .columns = {ScalarValue::signed_value(type(schema::LogicalTypeKind::kTimestampNs), timestamp)
                      .value(),
                  ScalarValue::signed_value(type(schema::LogicalTypeKind::kInt64), value).value()},
      .wal_id = common::Uuid{wal},
      .record_sequence = sequence,
      .system_commit_position = sequence,
      .row_ordinal = 0U,
  };
}

class ExplainProvider final : public ScalarSnapshotProvider {
public:
  explicit ExplainProvider(std::shared_ptr<const ScalarTableSnapshot> snapshot)
      : snapshot_(std::move(snapshot)) {}

  [[nodiscard]] common::Result<std::shared_ptr<const ScalarTableSnapshot>>
  resolve(const std::shared_ptr<const schema::TableSchema>&,
          std::optional<std::int64_t>) const override {
    return snapshot_;
  }

private:
  std::shared_ptr<const ScalarTableSnapshot> snapshot_;
};

TEST(SqlExplainTest, EmitsAnExactStableLogicalAndScalarPhysicalGolden) {
  const ExplainFixtures data = fixtures();
  BoundSqlSelect plan =
      bind("EXPLAIN SELECT value AS v FROM t WHERE value > 0 ORDER BY v LIMIT 2", data);
  SqlResult<std::string> explained = explain_sql_v1_select(plan);
  ASSERT_TRUE(explained.has_value()) << explained.error().status().to_string();
  EXPECT_EQ(*explained,
            "chronos_sql_v1_plan=1\n"
            "mode=explain\n"
            "catalog_generation=7\n"
            "logical.sources=1\n"
            "logical.source.0=name_hex:74,quoted:0,table:01000000000000000000000000000000,"
            "schema:02000000000000000000000000000000,version:1\n"
            "logical.system_time_ns=current\n"
            "logical.latest_keys=0\n"
            "logical.asof_joins=0\n"
            "logical.filter=1\n"
            "logical.aggregate=0\n"
            "logical.group_keys=0\n"
            "logical.outputs=1\n"
            "logical.output.0=name_hex:76,quoted:0,type:INT64,nullable:1,aggregate:0\n"
            "logical.order_keys=1\n"
            "logical.limit=2\n"
            "physical.engine=scalar_reference\n"
            "physical.operators=scan,filter,project,sort,limit\n");
}

TEST(SqlExplainTest, ExecutesAnalyzeOnceAndReportsMeasuredOperatorCounters) {
  const ExplainFixtures data = fixtures();
  BoundSqlSelect plan = bind("EXPLAIN ANALYZE SELECT count(*) AS n FROM t WHERE value > 0", data);
  auto snapshot = std::make_shared<const ScalarTableSnapshot>(
      ScalarTableSnapshot::create(data.schema, 3U, {row(1, -1, 1U), row(2, 2, 2U), row(3, 3, 3U)})
          .value());
  const ExplainProvider provider{std::move(snapshot)};
  SqlResult<ScalarExplainAnalyzeResult> analyzed = execute_sql_v1_explain_analyze(plan, provider);
  ASSERT_TRUE(analyzed.has_value()) << analyzed.error().status().to_string();
  EXPECT_NE(analyzed->plan.find("mode=explain_analyze\n"), std::string::npos);
  EXPECT_NE(analyzed->plan.find("physical.operators=scan,filter,aggregate,project\n"),
            std::string::npos);
  EXPECT_EQ(analyzed->execution.metrics.source_rows, 3U);
  EXPECT_EQ(analyzed->execution.metrics.rows_after_latest, 3U);
  EXPECT_EQ(analyzed->execution.metrics.asof_candidate_comparisons, 0U);
  EXPECT_EQ(analyzed->execution.metrics.rows_after_where, 2U);
  EXPECT_EQ(analyzed->execution.metrics.groups, 1U);
  EXPECT_EQ(analyzed->execution.metrics.output_rows, 1U);
  ASSERT_EQ(analyzed->execution.result.rows().size(), 1U);
  EXPECT_EQ(std::get<std::int64_t>(analyzed->execution.result.rows()[0][0].storage()), 2);

  EXPECT_EQ(execute_sql_v1_select(plan, provider).error().status().code(),
            common::StatusCode::kNotSupported);
}

TEST(SqlExplainTest, RejectsUnsupportedModeAndWrongAnalyzeEntryPoint) {
  const ExplainFixtures data = fixtures();
  BoundSqlSelect subscribe = bind("SUBSCRIBE SELECT value FROM t", data);
  EXPECT_EQ(explain_sql_v1_select(subscribe).error().status().code(),
            common::StatusCode::kNotSupported);
  BoundSqlSelect select = bind("SELECT value FROM t", data);
  auto snapshot = std::make_shared<const ScalarTableSnapshot>(
      ScalarTableSnapshot::create(data.schema, 1U, {row(1, 1, 1U)}).value());
  const ExplainProvider provider{std::move(snapshot)};
  EXPECT_EQ(execute_sql_v1_explain_analyze(select, provider).error().status().code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::query
