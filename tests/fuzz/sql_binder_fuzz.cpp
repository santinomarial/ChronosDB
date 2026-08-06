#include "chronos/common/uuid.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] std::shared_ptr<const chronos::query::QueryCatalogSnapshot> catalog() {
  using chronos::schema::LogicalTypeKind;
  const auto timestamp = id<chronos::schema::ColumnId>(3U);
  std::vector<chronos::schema::ColumnDefinition> columns;
  columns.push_back(chronos::schema::ColumnDefinition::create(
                        timestamp, "ts",
                        chronos::schema::LogicalType::create(LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  columns.push_back(chronos::schema::ColumnDefinition::create(
                        id<chronos::schema::ColumnId>(4U), "a",
                        chronos::schema::LogicalType::create(LogicalTypeKind::kInt64).value(), true)
                        .value());
  columns.push_back(chronos::schema::ColumnDefinition::create(
                        id<chronos::schema::ColumnId>(5U), "b",
                        chronos::schema::LogicalType::create(LogicalTypeKind::kString).value(),
                        true)
                        .value());
  auto schema = std::make_shared<const chronos::schema::TableSchema>(
      chronos::schema::TableSchema::create(
          id<chronos::schema::TableId>(1U), id<chronos::schema::SchemaId>(2U),
          chronos::schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
          {.event_time_column = timestamp,
           .physical_ordering_key = {timestamp},
           .partition_columns = {timestamp},
           .shard_key = {timestamp},
           .deduplication_key = {}})
          .value());
  const chronos::query::QueryCatalogTableInput input{
      .name = "t", .quoted = false, .schema = std::move(schema)};
  auto snapshot = chronos::query::QueryCatalogSnapshot::create(1U, {&input, 1U}).value();
  return std::make_shared<const chronos::query::QueryCatalogSnapshot>(std::move(snapshot));
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  // libFuzzer exposes bytes; string_view is the parser's non-owning byte interface.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  const std::string_view sql{reinterpret_cast<const char*>(data), size};
  chronos::query::SqlResult<chronos::query::ParsedSqlSelect> parsed =
      chronos::query::parse_sql_v1_select(sql, {.lexer = {.maximum_input_bytes = 4096U,
                                                          .maximum_tokens = 2048U,
                                                          .maximum_token_bytes = 4096U},
                                                .maximum_ast_nodes = 2048U,
                                                .maximum_expression_depth = 64U,
                                                .maximum_list_elements = 512U});
  if (parsed.has_value()) {
    static const std::shared_ptr<const chronos::query::QueryCatalogSnapshot> kCatalog = catalog();
    const auto bound = chronos::query::bind_sql_v1_select(std::move(*parsed), kCatalog,
                                                          {.maximum_sources = 8U,
                                                           .maximum_bound_expressions = 4096U,
                                                           .maximum_output_columns = 512U});
    if (bound.has_value())
      static_cast<void>(bound->outputs().size());
  }
  return 0;
}
