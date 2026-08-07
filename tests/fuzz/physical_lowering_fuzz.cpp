#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] std::shared_ptr<const chronos::query::QueryCatalogSnapshot> build_catalog() {
  using namespace chronos;
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
                                   .deduplication_key = {}})
          .value());
  const std::vector<query::QueryCatalogTableInput> tables{
      {.name = "metrics", .quoted = false, .schema = std::move(table)}};
  return std::make_shared<const query::QueryCatalogSnapshot>(
      query::QueryCatalogSnapshot::create(1U, tables).value());
}

[[nodiscard]] const std::shared_ptr<const chronos::query::QueryCatalogSnapshot>& catalog() {
  static const std::shared_ptr<const chronos::query::QueryCatalogSnapshot> value = build_catalog();
  return value;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  if (size == 0U)
    return 0;
  static constexpr std::array<std::string_view, 25> kSql{
      "SELECT value + 1 AS v FROM metrics",
      "SELECT value FROM metrics WHERE value BETWEEN 1 AND 9 LIMIT 2",
      "SELECT value IN (1, NULL, 3) AS v FROM metrics",
      "SELECT CAST(value AS FLOAT64) AS v FROM metrics",
      "SELECT value FROM metrics ORDER BY value",
      "SELECT sum(value) AS v FROM metrics",
      "SELECT count(*), count(value), sum(value), avg(value), min(value), max(value), "
      "var_pop(value), var_samp(value) FROM metrics WHERE value > 0",
      "SELECT sum(value + 2) + count(*) AS v FROM metrics",
      "SELECT count(*) AS v FROM metrics WHERE value < 0 LIMIT 0",
      "SELECT sum(value) AS v FROM metrics GROUP BY ts",
      "SELECT ts, count(*), sum(value) FROM metrics GROUP BY ts",
      "SELECT value % 3 AS bucket, sum(value + 1) + count(*) AS v FROM metrics "
      "WHERE value > 0 GROUP BY value % 3 LIMIT 2",
      "SELECT coalesce(value, 0) AS v FROM metrics",
      "SELECT CAST(value AS FLOAT64) AS v FROM metrics",
      "SELECT coalesce(CAST(NULL AS INT8), value) AS v FROM metrics",
      "SELECT time_bucket(INTERVAL '1 second', ts) AS v FROM metrics",
      "SELECT UUID '00000000-0000-0000-0000-000000000001' = "
      "UUID '00000000-0000-0000-0000-000000000002' AS v FROM metrics",
      "SELECT lower(CAST('ChRoNoS' AS SYMBOL)) AS v FROM metrics",
      "SELECT upper(coalesce(CAST(NULL AS STRING), 'fallback')) AS v FROM metrics",
      "SELECT lower('a') = 'a' AS v FROM metrics",
      "SELECT upper(CAST(NULL AS STRING)) IS NULL AS v FROM metrics",
      "SELECT lower('B') BETWEEN 'a' AND 'c' AS v FROM metrics",
      "SELECT CAST('x' AS SYMBOL) IN (CAST('a' AS SYMBOL), CAST('x' AS SYMBOL)) AS v FROM metrics",
      "EXPLAIN SELECT value FROM metrics",
      "EXPLAIN ANALYZE SELECT value FROM metrics"};
  auto parsed = chronos::query::parse_sql_v1_select(kSql[data[0] % kSql.size()]);
  if (!parsed.has_value())
    return 0;
  auto bound = chronos::query::bind_sql_v1_select(std::move(*parsed), catalog());
  if (!bound.has_value())
    return 0;
  const std::size_t instructions = size > 1U ? static_cast<std::size_t>(data[1]) + 1U
                                             : chronos::query::kMaximumVectorExpressionInstructions;
  const std::size_t aggregates = size > 2U ? static_cast<std::size_t>(data[2] % 8U) + 1U
                                           : chronos::query::kMaximumUngroupedAggregateWidth;
  auto lowered = chronos::query::lower_bound_sql_select(
      *bound, {.expression_limits = {.maximum_instructions = instructions,
                                     .maximum_retained_configuration_bytes = 256U * 1024U},
               .aggregate_limits = {.maximum_aggregates = aggregates},
               .grouped_aggregate_limits = {.maximum_aggregates = aggregates}});
  if (lowered.has_value() && lowered->output_columns().empty())
    __builtin_trap();
  return 0;
}
