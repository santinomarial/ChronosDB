#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/distributed_sql_lowering.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/table_schema.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] BoundSqlSelect bound_select(const std::string_view sql) {
  const schema::LogicalType timestamp =
      schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value();
  const schema::LogicalType integer =
      schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(id<schema::ColumnId>(3U), "ts", timestamp, false).value());
  columns.push_back(
      schema::ColumnDefinition::create(id<schema::ColumnId>(4U), "value", integer, false).value());
  auto table = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = id<schema::ColumnId>(3U),
                                   .physical_ordering_key = {id<schema::ColumnId>(3U)},
                                   .partition_columns = {id<schema::ColumnId>(3U)},
                                   .shard_key = {id<schema::ColumnId>(3U)},
                                   .deduplication_key = {id<schema::ColumnId>(3U)}})
          .value());
  const std::vector<QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = std::move(table)}};
  auto catalog = std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
  return bind_sql_v1_select(parse_sql_v1_select(sql).value(), std::move(catalog)).value();
}

TEST(DistributedSqlLoweringAllocationFailureTest, ClassifiesEveryOwnedAllocationFailure) {
  BoundSqlSelect select =
      bound_select("SELECT value AS v, value AS again FROM metrics "
                   "WHERE ts >= TIMESTAMP '1970-01-01 00:00:00Z' ORDER BY v DESC, ts LIMIT 2");
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::optional<SqlResult<DistributedVectorRowsSqlPlan>> result;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      result.emplace(lower_bound_sql_select_to_distributed_vector_rows(select));
      observed = failure.observed_allocations();
      failure.disable();
    }
    EXPECT_GT(observed, 0U);
    if (result->has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result->error().code(), SqlDiagnosticCode::kResourceLimit);
    EXPECT_EQ(result->error().status().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

TEST(DistributedSqlLoweringAllocationFailureTest,
     ClassifiesEveryGlobalAggregateOwnedAllocationFailure) {
  BoundSqlSelect select = bound_select(
      "SELECT count(*) AS n, sum(value) AS total, avg(value) AS mean_value FROM metrics "
      "WHERE ts BETWEEN TIMESTAMP '1970-01-01 00:00:00Z' AND "
      "TIMESTAMP '1970-01-01 00:00:01Z' LIMIT 1");
  bool reached_success = false;
  for (std::size_t fail_after = 0U; fail_after < 128U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::optional<SqlResult<DistributedVectorAggregateSqlPlan>> result;
    std::size_t observed = 0U;
    {
      ::chronos::test::ScopedAllocationFailure failure{fail_after};
      result.emplace(lower_bound_sql_select_to_distributed_vector_aggregate(select));
      observed = failure.observed_allocations();
      failure.disable();
    }
    EXPECT_GT(observed, 0U);
    if (result->has_value()) {
      reached_success = true;
      break;
    }
    EXPECT_EQ(result->error().code(), SqlDiagnosticCode::kResourceLimit);
    EXPECT_EQ(result->error().status().code(), common::StatusCode::kResourceExhausted);
  }
  EXPECT_TRUE(reached_success);
}

} // namespace
} // namespace chronos::query
