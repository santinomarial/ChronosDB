#include "chronos/schema/table_schema.hpp"

#include <algorithm>
#include <cstddef>
#include <string_view>
#include <utility>

namespace chronos::schema {
namespace {

[[nodiscard]] common::Status invalid(std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] const ColumnDefinition*
find_column(const std::vector<ColumnDefinition>& columns, const ColumnId id) noexcept {
  for (const ColumnDefinition& column : columns) {
    if (column.id() == id) {
      return &column;
    }
  }
  return nullptr;
}

[[nodiscard]] bool contains(const std::span<const ColumnId> values, const ColumnId value) noexcept {
  return std::ranges::find(values, value) != values.end();
}

[[nodiscard]] common::Status validate_role_list(const std::vector<ColumnDefinition>& columns,
                                                const std::span<const ColumnId> ids,
                                                const bool require_nonempty,
                                                const bool require_nonnull,
                                                const std::string_view label) {
  if (require_nonempty && ids.empty()) {
    return invalid(std::string{label} + " must not be empty");
  }
  for (std::size_t index = 0; index < ids.size(); ++index) {
    const ColumnDefinition* column = find_column(columns, ids[index]);
    if (column == nullptr) {
      return invalid(std::string{label} + " references an unknown column");
    }
    if (require_nonnull && column->nullable()) {
      return invalid(std::string{label} + " columns must be non-null");
    }
    for (std::size_t previous = 0; previous < index; ++previous) {
      if (ids[previous] == ids[index]) {
        return invalid(std::string{label} + " contains a duplicate column");
      }
    }
  }
  return common::Status::ok();
}

[[nodiscard]] bool equal_ids(const std::span<const ColumnId> left,
                             const std::span<const ColumnId> right) noexcept {
  return std::ranges::equal(left, right);
}

} // namespace

TableSchema::TableSchema(TableId table_id, SchemaId schema_id, SchemaVersion version,
                         std::optional<SchemaId> parent_schema_id,
                         std::vector<ColumnDefinition> columns, TableSchemaRoles roles) noexcept
    : table_id_(std::move(table_id)), schema_id_(std::move(schema_id)), version_(version),
      parent_schema_id_(std::move(parent_schema_id)), columns_(std::move(columns)),
      roles_(std::move(roles)) {}

common::Result<TableSchema>
TableSchema::create(TableId table_id, SchemaId schema_id, const SchemaVersion version,
                    std::optional<SchemaId> parent_schema_id,
                    std::vector<ColumnDefinition> columns, TableSchemaRoles roles) {
  if (version == SchemaVersion::initial()) {
    if (parent_schema_id.has_value()) {
      return common::make_unexpected(invalid("schema version 1 must not have a parent"));
    }
  } else if (!parent_schema_id.has_value()) {
    return common::make_unexpected(invalid("schema version greater than 1 must have a parent"));
  }
  if (parent_schema_id.has_value() && *parent_schema_id == schema_id) {
    return common::make_unexpected(invalid("schema cannot name itself as parent"));
  }
  if (columns.empty()) {
    return common::make_unexpected(invalid("schema must contain at least one column"));
  }
  if (columns.size() > kMaximumSchemaColumnCount) {
    return common::make_unexpected(invalid("schema exceeds the v1 column-count limit"));
  }

  for (std::size_t index = 0; index < columns.size(); ++index) {
    for (std::size_t previous = 0; previous < index; ++previous) {
      if (columns[previous].id() == columns[index].id()) {
        return common::make_unexpected(invalid("schema contains a duplicate column identity"));
      }
      if (columns[previous].name() == columns[index].name()) {
        return common::make_unexpected(invalid("schema contains a duplicate column name"));
      }
    }
  }

  const ColumnDefinition* event_time = find_column(columns, roles.event_time_column);
  if (event_time == nullptr) {
    return common::make_unexpected(invalid("event-time role references an unknown column"));
  }
  if (event_time->type().kind() != LogicalTypeKind::kTimestampNs) {
    return common::make_unexpected(invalid("event-time column must have type TIMESTAMP_NS"));
  }
  if (event_time->nullable()) {
    return common::make_unexpected(invalid("event-time column must be non-null"));
  }

  for (const auto& [ids, required, nonnull, label] : {
           std::tuple{std::span<const ColumnId>{roles.physical_ordering_key}, true, false,
                      std::string_view{"physical ordering key"}},
           std::tuple{std::span<const ColumnId>{roles.partition_columns}, true, false,
                      std::string_view{"partition declaration"}},
           std::tuple{std::span<const ColumnId>{roles.shard_key}, true, true,
                      std::string_view{"shard key"}},
           std::tuple{std::span<const ColumnId>{roles.deduplication_key}, false, true,
                      std::string_view{"deduplication key"}},
       }) {
    const common::Status status = validate_role_list(columns, ids, required, nonnull, label);
    if (!status.is_ok()) {
      return common::make_unexpected(status);
    }
  }

  if (!contains(roles.physical_ordering_key, roles.event_time_column)) {
    return common::make_unexpected(invalid("physical ordering key must contain event time"));
  }
  if (!contains(roles.partition_columns, roles.event_time_column)) {
    return common::make_unexpected(invalid("partition declaration must reference event time"));
  }
  if (!roles.deduplication_key.empty()) {
    for (const ColumnId shard_column : roles.shard_key) {
      if (!contains(roles.deduplication_key, shard_column)) {
        return common::make_unexpected(
            invalid("shard key must be a subset of the deduplication key"));
      }
    }
  }

  return TableSchema{std::move(table_id), std::move(schema_id), version,
                     std::move(parent_schema_id), std::move(columns), std::move(roles)};
}

const ColumnDefinition* TableSchema::find_column(const ColumnId id) const noexcept {
  return schema::find_column(columns_, id);
}

const ColumnDefinition* TableSchema::find_column(const std::string_view name) const noexcept {
  for (const ColumnDefinition& column : columns_) {
    if (column.name() == name) {
      return &column;
    }
  }
  return nullptr;
}

std::optional<std::size_t> TableSchema::column_ordinal(const ColumnId id) const noexcept {
  for (std::size_t index = 0; index < columns_.size(); ++index) {
    if (columns_[index].id() == id) {
      return index;
    }
  }
  return std::nullopt;
}

bool TableSchema::has_role(const ColumnId id, const ColumnRole role) const noexcept {
  switch (role) {
  case ColumnRole::kEventTime:
    return roles_.event_time_column == id;
  case ColumnRole::kPhysicalOrdering:
    return contains(roles_.physical_ordering_key, id);
  case ColumnRole::kPartition:
    return contains(roles_.partition_columns, id);
  case ColumnRole::kShardKey:
    return contains(roles_.shard_key, id);
  case ColumnRole::kDeduplicationKey:
    return contains(roles_.deduplication_key, id);
  }
  return false;
}

common::Status validate_v1_successor(const TableSchema& predecessor,
                                     const TableSchema& successor) {
  if (predecessor.table_id() != successor.table_id()) {
    return invalid("successor must preserve table identity");
  }
  if (!successor.parent_schema_id().has_value() ||
      *successor.parent_schema_id() != predecessor.schema_id()) {
    return invalid("successor must name the current schema as its parent");
  }
  const common::Result<SchemaVersion> expected_version = predecessor.version().next();
  if (!expected_version.has_value()) {
    return expected_version.error();
  }
  if (successor.version() != *expected_version) {
    return invalid("successor schema version must increment by one");
  }
  if (successor.columns().size() < predecessor.columns().size()) {
    return invalid("v1 evolution cannot drop columns");
  }
  for (std::size_t index = 0; index < predecessor.columns().size(); ++index) {
    const ColumnDefinition& before = predecessor.columns()[index];
    const ColumnDefinition& after = successor.columns()[index];
    if (before.id() != after.id()) {
      return invalid("v1 evolution cannot reorder or replace column identities");
    }
    if (before.type() != after.type()) {
      return invalid("v1 evolution cannot change column types or parameters");
    }
    if (before.nullable() != after.nullable()) {
      return invalid("v1 evolution cannot change column nullability");
    }
  }
  for (std::size_t index = predecessor.columns().size(); index < successor.columns().size();
       ++index) {
    if (!successor.columns()[index].nullable()) {
      return invalid("new v1 columns must be nullable and have no default");
    }
  }
  if (predecessor.event_time_column() != successor.event_time_column() ||
      !equal_ids(predecessor.physical_ordering_key(), successor.physical_ordering_key()) ||
      !equal_ids(predecessor.partition_columns(), successor.partition_columns()) ||
      !equal_ids(predecessor.shard_key(), successor.shard_key()) ||
      !equal_ids(predecessor.deduplication_key(), successor.deduplication_key())) {
    return invalid("v1 evolution cannot change roles or keys");
  }
  return common::Status::ok();
}

} // namespace chronos::schema
