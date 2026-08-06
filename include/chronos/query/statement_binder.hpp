#ifndef CHRONOS_QUERY_STATEMENT_BINDER_HPP_
#define CHRONOS_QUERY_STATEMENT_BINDER_HPP_

#include "chronos/query/ast.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/diagnostic.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace chronos::query {

struct BoundSqlTablePolicy {
  std::int64_t partition_interval_ns{};
  std::int64_t retention_ns{};
  std::int64_t system_history_retention_ns{};
  std::int64_t allowed_lateness_ns{};
};

struct SqlInsertBinderLimits {
  std::size_t maximum_rows{65'536U};
  std::size_t maximum_values{262'144U};
};

// Owns the CREATE TABLE syntax and the canonical column ordinals resolved from every role and
// policy clause. Stable table/schema/column identities are supplied separately by the catalog
// mutation owner; SQL text is never allowed to manufacture durable identities.
class BoundSqlCreateTable {
public:
  BoundSqlCreateTable() = delete;
  BoundSqlCreateTable(const BoundSqlCreateTable&) = delete;
  BoundSqlCreateTable& operator=(const BoundSqlCreateTable&) = delete;
  BoundSqlCreateTable(BoundSqlCreateTable&&) noexcept = default;
  BoundSqlCreateTable& operator=(BoundSqlCreateTable&&) noexcept = default;

  [[nodiscard]] const ParsedSqlCreateTable& syntax() const noexcept;
  [[nodiscard]] const std::shared_ptr<const QueryCatalogSnapshot>& catalog() const noexcept;
  [[nodiscard]] std::size_t event_time_ordinal() const noexcept;
  [[nodiscard]] std::span<const std::size_t> ordering_key_ordinals() const noexcept;
  [[nodiscard]] std::span<const std::size_t> shard_key_ordinals() const noexcept;
  [[nodiscard]] std::span<const std::size_t> deduplication_key_ordinals() const noexcept;
  [[nodiscard]] const BoundSqlTablePolicy& policy() const noexcept;

private:
  BoundSqlCreateTable(ParsedSqlCreateTable syntax,
                      std::shared_ptr<const QueryCatalogSnapshot> catalog,
                      std::size_t event_time_ordinal,
                      std::vector<std::size_t> ordering_key_ordinals,
                      std::vector<std::size_t> shard_key_ordinals,
                      std::vector<std::size_t> deduplication_key_ordinals,
                      BoundSqlTablePolicy policy) noexcept;

  ParsedSqlCreateTable syntax_;
  std::shared_ptr<const QueryCatalogSnapshot> catalog_;
  std::size_t event_time_ordinal_{};
  std::vector<std::size_t> ordering_key_ordinals_;
  std::vector<std::size_t> shard_key_ordinals_;
  std::vector<std::size_t> deduplication_key_ordinals_;
  BoundSqlTablePolicy policy_;

  friend SqlResult<BoundSqlCreateTable>
      bind_sql_v1_create_table(ParsedSqlCreateTable, std::shared_ptr<const QueryCatalogSnapshot>);
};

[[nodiscard]] SqlResult<BoundSqlCreateTable>
bind_sql_v1_create_table(ParsedSqlCreateTable syntax,
                         std::shared_ptr<const QueryCatalogSnapshot> catalog);

// Materializes the schema model only after the control plane allocates all durable identities.
// column_ids must contain exactly one identity per declaration, in declaration order.
[[nodiscard]] SqlResult<schema::TableSchema>
materialize_sql_v1_table_schema(const BoundSqlCreateTable& statement, schema::TableId table_id,
                                schema::SchemaId schema_id,
                                std::span<const schema::ColumnId> column_ids);

class BoundSqlInsert {
public:
  BoundSqlInsert() = delete;
  BoundSqlInsert(const BoundSqlInsert&) = delete;
  BoundSqlInsert& operator=(const BoundSqlInsert&) = delete;
  BoundSqlInsert(BoundSqlInsert&&) noexcept = default;
  BoundSqlInsert& operator=(BoundSqlInsert&&) noexcept = default;

  [[nodiscard]] const ParsedSqlInsert& syntax() const noexcept;
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;
  [[nodiscard]] std::span<const std::size_t> target_column_ordinals() const noexcept;

private:
  BoundSqlInsert(ParsedSqlInsert syntax, std::shared_ptr<const schema::TableSchema> schema,
                 std::vector<std::size_t> target_column_ordinals,
                 BoundSqlSelect expression_plan) noexcept;

  ParsedSqlInsert syntax_;
  std::shared_ptr<const schema::TableSchema> schema_;
  std::vector<std::size_t> target_column_ordinals_;
  BoundSqlSelect expression_plan_;

  friend class detail::SqlStatementBinder;
  friend class MaterializedSqlInsert;
  friend SqlResult<class MaterializedSqlInsert>
  materialize_sql_v1_insert_rows(const BoundSqlInsert&);
};

class MaterializedSqlInsert {
public:
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;
  [[nodiscard]] std::span<const std::vector<ScalarValue>> rows() const noexcept;

private:
  MaterializedSqlInsert(std::shared_ptr<const schema::TableSchema> schema,
                        std::vector<std::vector<ScalarValue>> rows) noexcept;

  std::shared_ptr<const schema::TableSchema> schema_;
  std::vector<std::vector<ScalarValue>> rows_;

  friend SqlResult<MaterializedSqlInsert> materialize_sql_v1_insert_rows(const BoundSqlInsert&);
};

[[nodiscard]] SqlResult<BoundSqlInsert>
bind_sql_v1_insert(ParsedSqlInsert syntax,
                   const std::shared_ptr<const QueryCatalogSnapshot>& catalog,
                   SqlInsertBinderLimits limits = {});

// Evaluates the source-free VALUES expressions and returns full schema-ordinal rows. Omitted
// nullable columns become typed NULL; missing or evaluated NULL non-null columns fail.
[[nodiscard]] SqlResult<MaterializedSqlInsert>
materialize_sql_v1_insert_rows(const BoundSqlInsert& statement);

} // namespace chronos::query

#endif // CHRONOS_QUERY_STATEMENT_BINDER_HPP_
