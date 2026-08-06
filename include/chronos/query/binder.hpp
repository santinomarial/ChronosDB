#ifndef CHRONOS_QUERY_BINDER_HPP_
#define CHRONOS_QUERY_BINDER_HPP_

#include "chronos/query/ast.hpp"
#include "chronos/query/catalog.hpp"
#include "chronos/query/diagnostic.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace chronos::query {

namespace detail {
class SqlBinder;
}

class BoundSqlSource {
public:
  [[nodiscard]] const std::string& exposed_name() const noexcept;
  [[nodiscard]] bool exposed_name_quoted() const noexcept;
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;

private:
  BoundSqlSource(std::string exposed_name, bool exposed_name_quoted,
                 std::shared_ptr<const schema::TableSchema> schema) noexcept;

  std::string exposed_name_;
  bool exposed_name_quoted_{};
  std::shared_ptr<const schema::TableSchema> schema_;

  friend class detail::SqlBinder;
};

struct BoundColumnReference {
  SourceSpan expression_span;
  std::size_t source_ordinal{};
  std::size_t column_ordinal{};
  schema::TableId table_id;
  schema::ColumnId column_id;
  schema::LogicalType type;
  bool nullable{};
};

struct BoundExpressionInfo {
  SourceSpan expression_span;
  std::optional<schema::LogicalType> type;
  std::optional<std::size_t> output_ordinal;
  bool nullable{};
  bool contains_aggregate{};
};

struct BoundOutputColumn {
  std::string name;
  bool name_quoted{};
  schema::LogicalType type;
  std::optional<SourceSpan> expression_span;
  std::optional<std::size_t> source_ordinal;
  std::optional<std::size_t> column_ordinal;
  bool nullable{};
  bool contains_aggregate{};
};

struct SqlBinderLimits {
  std::size_t maximum_sources{64U};
  std::size_t maximum_bound_expressions{262'144U};
  std::size_t maximum_output_columns{4096U};
};

// Owns the parsed tree and retains the exact catalog/schema snapshot used to resolve it. All
// column references and expression types are recorded against exact AST source spans.
class BoundSqlSelect {
public:
  BoundSqlSelect() = delete;
  BoundSqlSelect(const BoundSqlSelect&) = delete;
  BoundSqlSelect& operator=(const BoundSqlSelect&) = delete;
  BoundSqlSelect(BoundSqlSelect&&) noexcept = default;
  BoundSqlSelect& operator=(BoundSqlSelect&&) noexcept = default;

  [[nodiscard]] const ParsedSqlSelect& syntax() const noexcept;
  [[nodiscard]] const std::shared_ptr<const QueryCatalogSnapshot>& catalog() const noexcept;
  [[nodiscard]] std::span<const BoundSqlSource> sources() const noexcept;
  [[nodiscard]] std::span<const BoundColumnReference> column_references() const noexcept;
  [[nodiscard]] std::span<const BoundExpressionInfo> expressions() const noexcept;
  [[nodiscard]] std::span<const BoundOutputColumn> outputs() const noexcept;
  [[nodiscard]] const BoundExpressionInfo* find_expression(const SourceSpan& span) const noexcept;
  [[nodiscard]] const BoundColumnReference*
  find_column_reference(const SourceSpan& span) const noexcept;

private:
  BoundSqlSelect(ParsedSqlSelect syntax, std::shared_ptr<const QueryCatalogSnapshot> catalog,
                 std::vector<BoundSqlSource> sources,
                 std::vector<BoundColumnReference> column_references,
                 std::vector<BoundExpressionInfo> expressions,
                 std::vector<BoundOutputColumn> outputs) noexcept;

  ParsedSqlSelect syntax_;
  std::shared_ptr<const QueryCatalogSnapshot> catalog_;
  std::vector<BoundSqlSource> sources_;
  std::vector<BoundColumnReference> column_references_;
  std::vector<BoundExpressionInfo> expressions_;
  std::vector<BoundOutputColumn> outputs_;

  friend class detail::SqlBinder;
};

[[nodiscard]] SqlResult<BoundSqlSelect>
bind_sql_v1_select(ParsedSqlSelect syntax, std::shared_ptr<const QueryCatalogSnapshot> catalog,
                   SqlBinderLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_BINDER_HPP_
