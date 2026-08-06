#ifndef CHRONOS_QUERY_CATALOG_HPP_
#define CHRONOS_QUERY_CATALOG_HPP_

#include "chronos/query/ast.hpp"
#include "chronos/query/diagnostic.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kMaximumQueryCatalogTables = 65'536U;

struct QueryCatalogTableInput {
  std::string name;
  bool quoted{};
  std::shared_ptr<const schema::TableSchema> schema;
};

class QueryCatalogTable {
public:
  [[nodiscard]] const std::string& name() const noexcept;
  [[nodiscard]] bool quoted() const noexcept;
  [[nodiscard]] const std::shared_ptr<const schema::TableSchema>& schema_ptr() const noexcept;
  [[nodiscard]] const schema::TableSchema& schema() const noexcept;

private:
  QueryCatalogTable(std::string name, bool quoted,
                    std::shared_ptr<const schema::TableSchema> schema) noexcept;

  std::string name_;
  bool quoted_{};
  std::shared_ptr<const schema::TableSchema> schema_;

  friend class QueryCatalogSnapshot;
};

// Immutable schema-version-stable binder input. Advancing the live catalog or a SchemaLineage
// cannot change any schema retained by this snapshot.
class QueryCatalogSnapshot {
public:
  QueryCatalogSnapshot() = delete;
  QueryCatalogSnapshot(const QueryCatalogSnapshot&) = delete;
  QueryCatalogSnapshot& operator=(const QueryCatalogSnapshot&) = delete;
  QueryCatalogSnapshot(QueryCatalogSnapshot&&) noexcept = default;
  QueryCatalogSnapshot& operator=(QueryCatalogSnapshot&&) noexcept = default;

  [[nodiscard]] static SqlResult<QueryCatalogSnapshot>
  create(std::uint64_t generation, std::span<const QueryCatalogTableInput> tables);

  [[nodiscard]] std::uint64_t generation() const noexcept;
  [[nodiscard]] std::span<const QueryCatalogTable> tables() const noexcept;
  [[nodiscard]] const QueryCatalogTable* find(const SqlIdentifier& name) const noexcept;

private:
  QueryCatalogSnapshot(std::uint64_t generation, std::vector<QueryCatalogTable> tables) noexcept;

  std::uint64_t generation_{};
  std::vector<QueryCatalogTable> tables_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_CATALOG_HPP_
