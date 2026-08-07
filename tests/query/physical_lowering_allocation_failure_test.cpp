#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/table_schema.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] std::shared_ptr<const QueryCatalogSnapshot> catalog() {
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        id<schema::ColumnId>(3U), "ts",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        id<schema::ColumnId>(4U), "value",
                        schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value(), false)
                        .value());
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
  return std::make_shared<const QueryCatalogSnapshot>(
      QueryCatalogSnapshot::create(1U, tables).value());
}

TEST(PhysicalSelectLoweringAllocationFailureTest, ClassifiesEveryOwnedAllocationFailure) {
  const std::vector<std::string> statements{
      R"(SELECT coalesce(CAST(NULL AS INT8), CAST(value AS INT64)) AS v, time_bucket(INTERVAL '1 second', ts) AS bucket FROM metrics WHERE value BETWEEN 1 AND 9 LIMIT 2)",
      R"(SELECT sum(value + 2) + count(*) AS total, avg(value) AS mean FROM metrics WHERE value BETWEEN 1 AND 9 LIMIT 1)",
      R"(SELECT value % 3 AS bucket, sum(value + 2) + count(*) AS total FROM metrics WHERE value BETWEEN 1 AND 9 GROUP BY value % 3 LIMIT 2)",
      R"(SELECT value + 1 AS adjusted FROM metrics ORDER BY value DESC LIMIT 2)",
      R"(SELECT value AS adjusted FROM metrics LATEST BY (value) ON time_bucket(INTERVAL '1 second', ts) WHERE value > 0 ORDER BY adjusted DESC LIMIT 2)",
      R"(SELECT value % 3 AS bucket, count(*) AS rows FROM metrics GROUP BY value % 3 ORDER BY rows DESC, sum(value) DESC LIMIT 2)"};
  for (const std::string& statement : statements) {
    SCOPED_TRACE(statement);
    BoundSqlSelect select =
        bind_sql_v1_select(parse_sql_v1_select(statement).value(), catalog()).value();
    bool reached_success = false;
    for (std::size_t fail_after = 0U; fail_after < 256U; ++fail_after) {
      SCOPED_TRACE(fail_after);
      std::optional<SqlResult<PhysicalPipelinePlan>> result;
      std::size_t observed = 0U;
      {
        ::chronos::test::ScopedAllocationFailure failure{fail_after};
        result.emplace(lower_bound_sql_select(select));
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
}

} // namespace
} // namespace chronos::query
