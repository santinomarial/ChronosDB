#include "chronos/query/catalog.hpp"

#include "chronos/common/status.hpp"
#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <cstddef>
#include <new>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] SqlDiagnostic catalog_error(const SqlDiagnosticCode code,
                                          const common::StatusCode status_code,
                                          std::string message) {
  return SqlDiagnostic{code, {}, common::Status{status_code, std::move(message)}};
}

[[nodiscard]] bool ascii_unquoted_identifier(const std::string& value) noexcept {
  if (value.empty())
    return false;
  const auto start = [](const char byte) { return (byte >= 'a' && byte <= 'z') || byte == '_'; };
  const auto continuation = [&](const char byte) {
    return start(byte) || (byte >= '0' && byte <= '9');
  };
  if (!start(value.front()))
    return false;
  return std::ranges::all_of(value.begin() + 1, value.end(), continuation);
}

} // namespace

QueryCatalogTable::QueryCatalogTable(std::string name, const bool quoted,
                                     std::shared_ptr<const schema::TableSchema> schema) noexcept
    : name_(std::move(name)), quoted_(quoted), schema_(std::move(schema)) {}

const std::string& QueryCatalogTable::name() const noexcept {
  return name_;
}
bool QueryCatalogTable::quoted() const noexcept {
  return quoted_;
}
const std::shared_ptr<const schema::TableSchema>& QueryCatalogTable::schema_ptr() const noexcept {
  return schema_;
}
const schema::TableSchema& QueryCatalogTable::schema() const noexcept {
  return *schema_;
}

QueryCatalogSnapshot::QueryCatalogSnapshot(const std::uint64_t generation,
                                           std::vector<QueryCatalogTable> tables) noexcept
    : generation_(generation), tables_(std::move(tables)) {}

SqlResult<QueryCatalogSnapshot>
QueryCatalogSnapshot::create(const std::uint64_t generation,
                             const std::span<const QueryCatalogTableInput> tables) {
  if (generation == 0U) {
    return std::unexpected(catalog_error(SqlDiagnosticCode::kResourceLimit,
                                         common::StatusCode::kInvalidArgument,
                                         "Query catalog generation must be nonzero"));
  }
  if (tables.size() > kMaximumQueryCatalogTables) {
    return std::unexpected(catalog_error(SqlDiagnosticCode::kResourceLimit,
                                         common::StatusCode::kResourceExhausted,
                                         "Query catalog table count exceeds the limit"));
  }
  try {
    std::vector<QueryCatalogTable> copied;
    copied.reserve(tables.size());
    for (const QueryCatalogTableInput& input : tables) {
      if (input.schema == nullptr || input.name.empty() || !schema::is_valid_utf8(input.name) ||
          (!input.quoted && !ascii_unquoted_identifier(input.name))) {
        return std::unexpected(
            catalog_error(SqlDiagnosticCode::kUnknownTable, common::StatusCode::kInvalidArgument,
                          "Query catalog table requires a schema and a valid canonical name"));
      }
      copied.push_back(QueryCatalogTable{input.name, input.quoted, input.schema});
    }
    std::ranges::sort(copied, [](const QueryCatalogTable& left, const QueryCatalogTable& right) {
      if (left.name() != right.name())
        return left.name() < right.name();
      return !left.quoted() && right.quoted();
    });
    for (std::size_t index = 1U; index < copied.size(); ++index) {
      if (copied[index - 1U].name() == copied[index].name() &&
          copied[index - 1U].quoted() == copied[index].quoted()) {
        return std::unexpected(catalog_error(SqlDiagnosticCode::kUnknownTable,
                                             common::StatusCode::kAlreadyExists,
                                             "Query catalog repeats a table name"));
      }
    }
    std::vector<schema::TableId> identities;
    identities.reserve(copied.size());
    for (const QueryCatalogTable& table : copied) {
      identities.push_back(table.schema().table_id());
    }
    std::ranges::sort(identities);
    for (std::size_t index = 1U; index < identities.size(); ++index) {
      if (identities[index - 1U] == identities[index]) {
        return std::unexpected(catalog_error(SqlDiagnosticCode::kUnknownTable,
                                             common::StatusCode::kAlreadyExists,
                                             "Query catalog repeats a table identity"));
      }
    }
    return QueryCatalogSnapshot{generation, std::move(copied)};
  } catch (const std::bad_alloc&) {
    return std::unexpected(catalog_error(SqlDiagnosticCode::kResourceLimit,
                                         common::StatusCode::kResourceExhausted,
                                         "Query catalog snapshot allocation failed"));
  } catch (const std::length_error&) {
    return std::unexpected(catalog_error(SqlDiagnosticCode::kResourceLimit,
                                         common::StatusCode::kResourceExhausted,
                                         "Query catalog snapshot exceeds container limits"));
  }
}

std::uint64_t QueryCatalogSnapshot::generation() const noexcept {
  return generation_;
}
std::span<const QueryCatalogTable> QueryCatalogSnapshot::tables() const noexcept {
  return tables_;
}

const QueryCatalogTable* QueryCatalogSnapshot::find(const SqlIdentifier& name) const noexcept {
  const auto found = std::ranges::lower_bound(tables_, name.text(), {}, &QueryCatalogTable::name);
  if (found == tables_.end() || found->name() != name.text())
    return nullptr;
  for (auto current = found; current != tables_.end() && current->name() == name.text();
       ++current) {
    if (current->quoted() == name.quoted())
      return &*current;
  }
  return nullptr;
}

} // namespace chronos::query
