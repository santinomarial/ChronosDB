#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/statement_binder.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

template <typename Value>
[[nodiscard]] const Value* optional_pointer(const std::optional<Value>& value) noexcept {
  return value.has_value() ? std::addressof(*value) : nullptr;
}

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> empty_catalog() {
  QueryCatalogSnapshot catalog = QueryCatalogSnapshot::create(9U, {}).value();
  return std::make_shared<const QueryCatalogSnapshot>(std::move(catalog));
}

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> insert_catalog() {
  std::vector<schema::ColumnDefinition> columns;
  for (const auto& [name, kind, nullable] : {
           std::tuple{std::string_view{"ts"}, schema::LogicalTypeKind::kTimestampNs, false},
           std::tuple{std::string_view{"symbol"}, schema::LogicalTypeKind::kSymbol, false},
           std::tuple{std::string_view{"quantity"}, schema::LogicalTypeKind::kInt64, false},
           std::tuple{std::string_view{"price"}, schema::LogicalTypeKind::kFloat64, true},
           std::tuple{std::string_view{"note"}, schema::LogicalTypeKind::kString, true},
       }) {
    columns.push_back(schema::ColumnDefinition::create(
                          id<schema::ColumnId>(static_cast<std::uint8_t>(columns.size() + 3U)),
                          std::string{name}, schema::LogicalType::create(kind).value(), nullable)
                          .value());
  }
  const schema::ColumnId event_time = columns[0].id();
  const schema::ColumnId symbol = columns[1].id();
  auto table = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = event_time,
                                   .physical_ordering_key = {event_time},
                                   .partition_columns = {event_time},
                                   .shard_key = {symbol},
                                   .deduplication_key = {symbol}})
          .value());
  const std::array inputs{
      QueryCatalogTableInput{.name = "trades", .quoted = false, .schema = std::move(table)}};
  QueryCatalogSnapshot catalog = QueryCatalogSnapshot::create(10U, inputs).value();
  return std::make_shared<const QueryCatalogSnapshot>(std::move(catalog));
}

[[nodiscard]] SqlResult<BoundSqlCreateTable> bind_create(const std::string_view sql) {
  SqlResult<ParsedSqlCreateTable> parsed = parse_sql_v1_create_table(sql);
  if (!parsed.has_value())
    return std::unexpected(parsed.error());
  return bind_sql_v1_create_table(std::move(*parsed), empty_catalog());
}

[[nodiscard]] SqlResult<BoundSqlInsert> bind_insert(const std::string_view sql,
                                                    const SqlInsertBinderLimits limits = {}) {
  SqlResult<ParsedSqlInsert> parsed = parse_sql_v1_insert(sql);
  if (!parsed.has_value())
    return std::unexpected(parsed.error());
  return bind_sql_v1_insert(std::move(*parsed), insert_catalog(), limits);
}

constexpr std::string_view kCreate =
    "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price "
    "DECIMAL(20, 8) NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) "
    "PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY "
    "(symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL "
    "'7 days' ALLOWED LATENESS INTERVAL '0 seconds'";

TEST(SqlStatementBinderTest, BindsCanonicalCreatePolicyRolesAndMaterializesExactIdentities) {
  SqlResult<BoundSqlCreateTable> bound = bind_create(kCreate);
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  EXPECT_EQ(bound->event_time_ordinal(), 0U);
  EXPECT_TRUE(
      std::ranges::equal(bound->ordering_key_ordinals(), std::array<std::size_t, 2>{1U, 0U}));
  EXPECT_TRUE(std::ranges::equal(bound->shard_key_ordinals(), std::array<std::size_t, 1>{1U}));
  EXPECT_TRUE(
      std::ranges::equal(bound->deduplication_key_ordinals(), std::array<std::size_t, 2>{1U, 0U}));
  EXPECT_EQ(bound->policy().partition_interval_ns, 86'400'000'000'000LL);
  EXPECT_EQ(bound->policy().retention_ns, 2'592'000'000'000'000LL);
  EXPECT_EQ(bound->policy().system_history_retention_ns, 604'800'000'000'000LL);
  EXPECT_EQ(bound->policy().allowed_lateness_ns, 0);

  const std::array column_ids{id<schema::ColumnId>(3U), id<schema::ColumnId>(4U),
                              id<schema::ColumnId>(5U), id<schema::ColumnId>(6U)};
  SqlResult<schema::TableSchema> table = materialize_sql_v1_table_schema(
      *bound, id<schema::TableId>(1U), id<schema::SchemaId>(2U), column_ids);
  ASSERT_TRUE(table.has_value()) << table.error().status().to_string();
  EXPECT_EQ(table->table_id(), id<schema::TableId>(1U));
  EXPECT_EQ(table->columns()[2].type(), schema::LogicalType::decimal(20U, 8U).value());
  EXPECT_EQ(table->event_time_column(), column_ids[0]);
  EXPECT_TRUE(std::ranges::equal(table->physical_ordering_key(),
                                 std::array<schema::ColumnId, 2>{column_ids[1], column_ids[0]}));
}

TEST(SqlStatementBinderTest, RejectsInvalidCreateTableRelationships) {
  const auto code = [](const std::string_view sql) { return bind_create(sql).error().code(); };
  EXPECT_EQ(code("CREATE TABLE t (ts TIMESTAMP_NS NOT NULL, ts INT64) EVENT TIME ts ORDER KEY "
                 "(ts) PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (ts) RETENTION "
                 "INTERVAL '1 day' SYSTEM HISTORY RETENTION INTERVAL '1 day' ALLOWED LATENESS "
                 "INTERVAL '0 seconds'"),
            SqlDiagnosticCode::kDuplicateOutputName);
  EXPECT_EQ(code("CREATE TABLE t (ts TIMESTAMP_NS) EVENT TIME ts ORDER KEY (ts) PARTITION BY "
                 "time_bucket(INTERVAL '1 day', ts) SHARD KEY (ts) RETENTION INTERVAL '1 day' "
                 "SYSTEM HISTORY RETENTION INTERVAL '1 day' ALLOWED LATENESS INTERVAL '0 seconds'"),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(code("CREATE TABLE t (ts TIMESTAMP_NS NOT NULL, k STRING NOT NULL) EVENT TIME ts "
                 "ORDER KEY (k) PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (k) "
                 "RETENTION INTERVAL '1 day' SYSTEM HISTORY RETENTION INTERVAL '1 day' ALLOWED "
                 "LATENESS INTERVAL '0 seconds'"),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(code("CREATE TABLE t (ts TIMESTAMP_NS NOT NULL, k STRING) EVENT TIME ts ORDER KEY "
                 "(ts) PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (k) RETENTION "
                 "INTERVAL '1 day' SYSTEM HISTORY RETENTION INTERVAL '1 day' ALLOWED LATENESS "
                 "INTERVAL '0 seconds'"),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(code("CREATE TABLE t (ts TIMESTAMP_NS NOT NULL, k STRING NOT NULL) EVENT TIME ts "
                 "ORDER KEY (ts) PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (k) "
                 "DEDUP KEY (ts) RETENTION INTERVAL '1 day' SYSTEM HISTORY RETENTION INTERVAL "
                 "'1 day' ALLOWED LATENESS INTERVAL '0 seconds'"),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(code("CREATE TABLE t (ts TIMESTAMP_NS NOT NULL) EVENT TIME ts ORDER KEY (ts) "
                 "PARTITION BY time_bucket(INTERVAL '0 days', ts) SHARD KEY (ts) RETENTION "
                 "INTERVAL '1 day' SYSTEM HISTORY RETENTION INTERVAL '1 day' ALLOWED LATENESS "
                 "INTERVAL '0 seconds'"),
            SqlDiagnosticCode::kInvalidLiteral);
}

TEST(SqlStatementBinderTest, RequiresExactIdentityCountDuringMaterialization) {
  SqlResult<BoundSqlCreateTable> bound = bind_create(kCreate);
  ASSERT_TRUE(bound.has_value());
  const std::array ids{id<schema::ColumnId>(3U)};
  EXPECT_EQ(materialize_sql_v1_table_schema(*bound, id<schema::TableId>(1U),
                                            id<schema::SchemaId>(2U), ids)
                .error()
                .code(),
            SqlDiagnosticCode::kResourceLimit);
}

TEST(SqlStatementBinderTest, BindsAndMaterializesConstantInsertRowsInSchemaOrder) {
  SqlResult<BoundSqlInsert> bound =
      bind_insert("INSERT INTO trades (ts, symbol, quantity) VALUES "
                  "(TIMESTAMP '1970-01-01 00:00:00.000000001Z', CAST('A' AS SYMBOL), 40 + 2), "
                  "(TIMESTAMP '1970-01-01 00:00:00.000000002Z', CAST(UPPER('b') AS SYMBOL), 7)");
  ASSERT_TRUE(bound.has_value()) << bound.error().status().to_string();
  EXPECT_TRUE(
      std::ranges::equal(bound->target_column_ordinals(), std::array<std::size_t, 3>{0U, 1U, 2U}));
  EXPECT_EQ(bound->schema_ptr()->schema_id(), id<schema::SchemaId>(2U));

  SqlResult<MaterializedSqlInsert> materialized = materialize_sql_v1_insert_rows(*bound);
  ASSERT_TRUE(materialized.has_value()) << materialized.error().status().to_string();
  ASSERT_EQ(materialized->rows().size(), 2U);
  ASSERT_EQ(materialized->rows()[0].size(), 5U);
  EXPECT_EQ(std::get<std::int64_t>(materialized->rows()[0][0].storage()), 1);
  EXPECT_EQ(std::get<std::string>(materialized->rows()[0][1].storage()), "A");
  EXPECT_EQ(std::get<std::int64_t>(materialized->rows()[0][2].storage()), 42);
  EXPECT_TRUE(materialized->rows()[0][3].is_null());
  const schema::LogicalType* price_type = optional_pointer(materialized->rows()[0][3].type());
  ASSERT_NE(price_type, nullptr);
  EXPECT_EQ(price_type->kind(), schema::LogicalTypeKind::kFloat64);
  EXPECT_TRUE(materialized->rows()[0][4].is_null());
  EXPECT_EQ(std::get<std::string>(materialized->rows()[1][1].storage()), "B");

  common::Result<columnar::OwnedColumnarBatch> batch =
      materialize_sql_v1_insert_batch(*materialized);
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_EQ(batch->schema_ptr(), materialized->schema_ptr());
  ASSERT_EQ(batch->row_count(), 2U);
  ASSERT_EQ(batch->columns().size(), 5U);
  const auto scalar = [&batch](const std::size_t column, const std::uint32_t row) {
    return ScalarValue::from_column_cell(
               batch->schema().columns()[column].type(),
               batch->cell({.column_ordinal = column, .row = row}).value())
        .value();
  };
  EXPECT_EQ(std::get<std::int64_t>(scalar(0U, 1U).storage()), 2);
  EXPECT_EQ(std::get<std::string>(scalar(1U, 0U).storage()), "A");
  EXPECT_EQ(std::get<std::int64_t>(scalar(2U, 0U).storage()), 42);
  EXPECT_TRUE(scalar(3U, 0U).is_null());
  EXPECT_TRUE(scalar(4U, 1U).is_null());

  EXPECT_EQ(materialize_sql_v1_insert_batch(*materialized, {.max_rows = 1U}).error().code(),
            common::StatusCode::kResourceExhausted);
}

TEST(SqlStatementBinderTest, RejectsInvalidInsertTargetsAssignmentsAndExpressions) {
  EXPECT_EQ(bind_insert("INSERT INTO missing VALUES (1)").error().code(),
            SqlDiagnosticCode::kUnknownTable);
  EXPECT_EQ(bind_insert("INSERT INTO trades (ts, ts, symbol, quantity) VALUES "
                        "(TIMESTAMP '1970-01-01 00:00:00Z', "
                        "TIMESTAMP '1970-01-01 00:00:00Z', CAST('A' AS SYMBOL), 1)")
                .error()
                .code(),
            SqlDiagnosticCode::kDuplicateOutputName);
  EXPECT_EQ(bind_insert("INSERT INTO trades (ts, symbol) VALUES "
                        "(TIMESTAMP '1970-01-01 00:00:00Z', CAST('A' AS SYMBOL))")
                .error()
                .code(),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(bind_insert("INSERT INTO trades (ts, symbol, quantity) VALUES "
                        "(TIMESTAMP '1970-01-01 00:00:00Z', 'A', 1)")
                .error()
                .code(),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(bind_insert("INSERT INTO trades (ts, symbol, quantity) VALUES "
                        "(TIMESTAMP '1970-01-01 00:00:00Z', CAST('A' AS SYMBOL), quantity)")
                .error()
                .code(),
            SqlDiagnosticCode::kTypeMismatch);
  EXPECT_EQ(bind_insert("INSERT INTO trades (ts, symbol, quantity) VALUES "
                        "(TIMESTAMP '1970-01-01 00:00:00Z', CAST('A' AS SYMBOL), count(*))")
                .error()
                .code(),
            SqlDiagnosticCode::kTypeMismatch);
}

TEST(SqlStatementBinderTest, RejectsMaterializedNullAndEnforcesInsertLimits) {
  SqlResult<BoundSqlInsert> null_value =
      bind_insert("INSERT INTO trades (ts, symbol, quantity) VALUES "
                  "(TIMESTAMP '1970-01-01 00:00:00Z', CAST('A' AS SYMBOL), NULL)");
  ASSERT_TRUE(null_value.has_value()) << null_value.error().status().to_string();
  EXPECT_EQ(materialize_sql_v1_insert_rows(*null_value).error().code(),
            SqlDiagnosticCode::kTypeMismatch);

  EXPECT_EQ(bind_insert("INSERT INTO trades VALUES (TIMESTAMP '1970-01-01 00:00:00Z', "
                        "CAST('A' AS SYMBOL), 1, CAST(1 AS FLOAT64), 'x')",
                        {.maximum_rows = 1U, .maximum_values = 4U})
                .error()
                .code(),
            SqlDiagnosticCode::kResourceLimit);
  EXPECT_EQ(bind_insert("INSERT INTO trades VALUES (TIMESTAMP '1970-01-01 00:00:00Z', "
                        "CAST('A' AS SYMBOL), 1, CAST(1 AS FLOAT64), 'x')",
                        {.maximum_rows = 0U, .maximum_values = 5U})
                .error()
                .code(),
            SqlDiagnosticCode::kResourceLimit);
}

} // namespace
} // namespace chronos::query
