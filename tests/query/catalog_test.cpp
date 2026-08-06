#include "chronos/common/uuid.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"

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
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::TableSchema make_schema(const schema::SchemaVersion version,
                                              const bool add_value) {
  const schema::ColumnId event = id<schema::ColumnId>(3U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event, "ts",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  if (add_value) {
    columns.push_back(schema::ColumnDefinition::create(
                          id<schema::ColumnId>(4U), "value",
                          schema::LogicalType::create(schema::LogicalTypeKind::kFloat64).value(),
                          true)
                          .value());
  }
  return schema::TableSchema::create(
             id<schema::TableId>(1U),
             add_value ? id<schema::SchemaId>(6U) : id<schema::SchemaId>(2U), version,
             add_value ? std::optional{id<schema::SchemaId>(2U)} : std::nullopt, std::move(columns),
             {.event_time_column = event,
              .physical_ordering_key = {event},
              .partition_columns = {event},
              .shard_key = {event},
              .deduplication_key = {}})
      .value();
}

TEST(QueryCatalogTest, RetainsExactSchemaVersionAcrossLiveLineageAdvance) {
  schema::SchemaLineage lineage =
      schema::SchemaLineage::create(make_schema(schema::SchemaVersion::initial(), false)).value();
  const std::shared_ptr<const schema::TableSchema> initial = lineage.current();
  const QueryCatalogTableInput input{.name = "metrics", .quoted = false, .schema = initial};
  QueryCatalogSnapshot catalog = QueryCatalogSnapshot::create(17U, {&input, 1U}).value();

  ASSERT_TRUE(
      lineage.append(make_schema(schema::SchemaVersion::initial().next().value(), true)).is_ok());
  EXPECT_EQ(lineage.current()->version().value(), 2U);
  EXPECT_EQ(catalog.generation(), 17U);
  ASSERT_EQ(catalog.tables().size(), 1U);
  EXPECT_EQ(catalog.tables().front().schema().version().value(), 1U);
  EXPECT_EQ(catalog.tables().front().schema().columns().size(), 1U);

  ParsedSqlSelect select = parse_sql_v1_select("SELECT * FROM metrics").value();
  const QueryCatalogTable* found = catalog.find(select.source().table);
  ASSERT_NE(found, nullptr);
  EXPECT_EQ(found->schema_ptr(), initial);
}

TEST(QueryCatalogTest, PreservesQuotedNameIdentityAndRejectsHostileCatalogs) {
  const auto schema_value = std::make_shared<const schema::TableSchema>(
      make_schema(schema::SchemaVersion::initial(), false));
  const QueryCatalogTableInput unquoted{.name = "metrics", .quoted = false, .schema = schema_value};
  const QueryCatalogTableInput quoted{.name = "Metrics", .quoted = true, .schema = schema_value};

  QueryCatalogSnapshot one = QueryCatalogSnapshot::create(1U, {&unquoted, 1U}).value();
  ParsedSqlSelect folded = parse_sql_v1_select("SELECT * FROM MeTrIcS").value();
  EXPECT_NE(one.find(folded.source().table), nullptr);
  ParsedSqlSelect exact = parse_sql_v1_select("SELECT * FROM \"metrics\"").value();
  EXPECT_EQ(one.find(exact.source().table), nullptr);

  EXPECT_EQ(QueryCatalogSnapshot::create(0U, {}).error().status().code(),
            common::StatusCode::kInvalidArgument);
  QueryCatalogTableInput malformed{.name = "Not-Folded", .quoted = false, .schema = schema_value};
  EXPECT_FALSE(QueryCatalogSnapshot::create(1U, {&malformed, 1U}).has_value());
  QueryCatalogTableInput missing{.name = "missing", .quoted = false, .schema = nullptr};
  EXPECT_FALSE(QueryCatalogSnapshot::create(1U, {&missing, 1U}).has_value());
  const std::vector<QueryCatalogTableInput> duplicate_names{unquoted, unquoted};
  EXPECT_EQ(QueryCatalogSnapshot::create(1U, duplicate_names).error().status().code(),
            common::StatusCode::kAlreadyExists);
  const std::vector<QueryCatalogTableInput> duplicate_identity{unquoted, quoted};
  EXPECT_EQ(QueryCatalogSnapshot::create(1U, duplicate_identity).error().status().code(),
            common::StatusCode::kAlreadyExists);
}

} // namespace
} // namespace chronos::query
