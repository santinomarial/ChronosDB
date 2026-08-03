#ifndef CHRONOS_SCHEMA_SCHEMA_LINEAGE_HPP_
#define CHRONOS_SCHEMA_SCHEMA_LINEAGE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace chronos::schema {

class ProjectionEntry {
public:
  ProjectionEntry(ColumnId descendant_column_id, std::size_t descendant_ordinal,
                  std::optional<std::size_t> ancestor_ordinal) noexcept;

  [[nodiscard]] constexpr const ColumnId& descendant_column_id() const noexcept {
    return descendant_column_id_;
  }
  [[nodiscard]] constexpr std::size_t descendant_ordinal() const noexcept {
    return descendant_ordinal_;
  }
  [[nodiscard]] constexpr const std::optional<std::size_t>& ancestor_ordinal() const noexcept {
    return ancestor_ordinal_;
  }
  [[nodiscard]] constexpr bool synthesizes_null() const noexcept {
    return !ancestor_ordinal_.has_value();
  }

  friend bool operator==(const ProjectionEntry&, const ProjectionEntry&) = default;

private:
  ColumnId descendant_column_id_;
  std::size_t descendant_ordinal_;
  std::optional<std::size_t> ancestor_ordinal_;
};

class SchemaProjection {
public:
  SchemaProjection() = delete;

  [[nodiscard]] constexpr const SchemaId& ancestor_schema_id() const noexcept {
    return ancestor_schema_id_;
  }
  [[nodiscard]] constexpr const SchemaId& descendant_schema_id() const noexcept {
    return descendant_schema_id_;
  }
  [[nodiscard]] std::span<const ProjectionEntry> entries() const noexcept { return entries_; }

  friend bool operator==(const SchemaProjection&, const SchemaProjection&) = default;

private:
  SchemaProjection(SchemaId ancestor_schema_id, SchemaId descendant_schema_id,
                   std::vector<ProjectionEntry> entries) noexcept;

  SchemaId ancestor_schema_id_;
  SchemaId descendant_schema_id_;
  std::vector<ProjectionEntry> entries_;

  friend class SchemaLineage;
};

// A lineage is an in-memory, single-writer catalog primitive. Returned shared schema pointers keep
// immutable versions alive across later append() calls. The lineage itself is not thread-safe.
class SchemaLineage {
public:
  SchemaLineage() = delete;

  [[nodiscard]] static common::Result<SchemaLineage> create(TableSchema initial_schema);

  [[nodiscard]] common::Status append(TableSchema successor);

  [[nodiscard]] const TableId& table_id() const noexcept;
  [[nodiscard]] std::size_t size() const noexcept;
  [[nodiscard]] std::shared_ptr<const TableSchema> current() const noexcept;
  [[nodiscard]] std::shared_ptr<const TableSchema> at(std::size_t index) const noexcept;
  [[nodiscard]] std::shared_ptr<const TableSchema> find(SchemaId schema_id) const noexcept;
  [[nodiscard]] std::optional<ColumnId>
  historical_column_id(std::string_view name) const noexcept;
  [[nodiscard]] common::Result<SchemaProjection>
  projection(SchemaId ancestor_schema_id, SchemaId descendant_schema_id) const;

private:
  explicit SchemaLineage(std::shared_ptr<const TableSchema> initial_schema);

  std::vector<std::shared_ptr<const TableSchema>> schemas_;
};

} // namespace chronos::schema

#endif // CHRONOS_SCHEMA_SCHEMA_LINEAGE_HPP_
