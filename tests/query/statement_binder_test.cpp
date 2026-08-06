#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/statement_binder.hpp"
#include "chronos/schema/identity.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <string_view>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> empty_catalog() {
  QueryCatalogSnapshot catalog = QueryCatalogSnapshot::create(9U, {}).value();
  return std::make_shared<const QueryCatalogSnapshot>(std::move(catalog));
}

[[nodiscard]] SqlResult<BoundSqlCreateTable> bind_create(const std::string_view sql) {
  SqlResult<ParsedSqlCreateTable> parsed = parse_sql_v1_create_table(sql);
  if (!parsed.has_value())
    return std::unexpected(parsed.error());
  return bind_sql_v1_create_table(std::move(*parsed), empty_catalog());
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

} // namespace
} // namespace chronos::query
