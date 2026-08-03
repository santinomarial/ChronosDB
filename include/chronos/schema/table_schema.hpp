#ifndef CHRONOS_SCHEMA_TABLE_SCHEMA_HPP_
#define CHRONOS_SCHEMA_TABLE_SCHEMA_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace chronos::schema {

inline constexpr std::size_t kMaximumSchemaColumnCount = 4096;

enum class ColumnRole : std::uint8_t {
  kEventTime,
  kPhysicalOrdering,
  kPartition,
  kShardKey,
  kDeduplicationKey,
};

// The vectors preserve declaration order. partition_columns identifies the columns referenced by
// the v1 time-partition declaration; expression evaluation remains outside this model layer.
struct TableSchemaRoles {
  ColumnId event_time_column;
  std::vector<ColumnId> physical_ordering_key;
  std::vector<ColumnId> partition_columns;
  std::vector<ColumnId> shard_key;
  std::vector<ColumnId> deduplication_key;
};

class TableSchema {
public:
  TableSchema() = delete;

  [[nodiscard]] static common::Result<TableSchema>
  create(TableId table_id, SchemaId schema_id, SchemaVersion version,
         std::optional<SchemaId> parent_schema_id, std::vector<ColumnDefinition> columns,
         TableSchemaRoles roles);

  [[nodiscard]] constexpr const TableId& table_id() const noexcept { return table_id_; }
  [[nodiscard]] constexpr const SchemaId& schema_id() const noexcept { return schema_id_; }
  [[nodiscard]] constexpr SchemaVersion version() const noexcept { return version_; }
  [[nodiscard]] constexpr const std::optional<SchemaId>& parent_schema_id() const noexcept {
    return parent_schema_id_;
  }
  [[nodiscard]] std::span<const ColumnDefinition> columns() const noexcept { return columns_; }
  [[nodiscard]] const ColumnId& event_time_column() const noexcept {
    return roles_.event_time_column;
  }
  [[nodiscard]] std::span<const ColumnId> physical_ordering_key() const noexcept {
    return roles_.physical_ordering_key;
  }
  [[nodiscard]] std::span<const ColumnId> partition_columns() const noexcept {
    return roles_.partition_columns;
  }
  [[nodiscard]] std::span<const ColumnId> shard_key() const noexcept { return roles_.shard_key; }
  [[nodiscard]] std::span<const ColumnId> deduplication_key() const noexcept {
    return roles_.deduplication_key;
  }

  [[nodiscard]] const ColumnDefinition* find_column(ColumnId id) const noexcept;
  [[nodiscard]] const ColumnDefinition* find_column(std::string_view name) const noexcept;
  [[nodiscard]] std::optional<std::size_t> column_ordinal(ColumnId id) const noexcept;
  [[nodiscard]] bool has_role(ColumnId id, ColumnRole role) const noexcept;

  friend bool operator==(const TableSchema&, const TableSchema&) = default;

private:
  TableSchema(TableId table_id, SchemaId schema_id, SchemaVersion version,
              std::optional<SchemaId> parent_schema_id,
              std::vector<ColumnDefinition> columns, TableSchemaRoles roles) noexcept;

  TableId table_id_;
  SchemaId schema_id_;
  SchemaVersion version_;
  std::optional<SchemaId> parent_schema_id_;
  std::vector<ColumnDefinition> columns_;
  TableSchemaRoles roles_;
};

// Validates only one direct v1 transition. Lineage-wide schema/name/identity reuse protection is
// provided by SchemaLineage.
[[nodiscard]] common::Status validate_v1_successor(const TableSchema& predecessor,
                                                    const TableSchema& successor);

} // namespace chronos::schema

#endif // CHRONOS_SCHEMA_TABLE_SCHEMA_HPP_
