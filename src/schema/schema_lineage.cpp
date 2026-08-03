#include "chronos/schema/schema_lineage.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace chronos::schema {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

} // namespace

ProjectionEntry::ProjectionEntry(ColumnId descendant_column_id,
                                 const std::size_t descendant_ordinal,
                                 std::optional<std::size_t> ancestor_ordinal) noexcept
    : descendant_column_id_(descendant_column_id), descendant_ordinal_(descendant_ordinal),
      ancestor_ordinal_(ancestor_ordinal) {}

SchemaProjection::SchemaProjection(ProjectionRequest request,
                                   std::vector<ProjectionEntry> entries) noexcept
    : ancestor_schema_id_(request.ancestor_schema_id),
      descendant_schema_id_(request.descendant_schema_id), entries_(std::move(entries)) {}

SchemaLineage::SchemaLineage(std::shared_ptr<const TableSchema> initial_schema)
    : schemas_{std::move(initial_schema)} {}

common::Result<SchemaLineage> SchemaLineage::create(TableSchema initial_schema) {
  if (initial_schema.version() != SchemaVersion::initial() ||
      initial_schema.parent_schema_id().has_value()) {
    return common::make_unexpected(invalid("lineage must begin with parentless schema version 1"));
  }
  return SchemaLineage{std::make_shared<const TableSchema>(std::move(initial_schema))};
}

common::Status SchemaLineage::append(TableSchema successor) {
  const std::shared_ptr<const TableSchema>& predecessor = schemas_.back();
  common::Status compatibility = validate_v1_successor(*predecessor, successor);
  if (!compatibility.is_ok()) {
    return compatibility;
  }

  for (const std::shared_ptr<const TableSchema>& schema : schemas_) {
    if (schema->schema_id() == successor.schema_id()) {
      return invalid("schema identity was already used in this lineage");
    }
  }

  for (const ColumnDefinition& candidate : successor.columns()) {
    for (const std::shared_ptr<const TableSchema>& historical_schema : schemas_) {
      for (const ColumnDefinition& historical : historical_schema->columns()) {
        if (historical.name() == candidate.name() && historical.id() != candidate.id()) {
          return invalid("historical column name cannot be reassigned to another identity");
        }
      }
    }
  }

  for (std::size_t index = predecessor->columns().size(); index < successor.columns().size();
       ++index) {
    const ColumnId candidate_id = successor.columns()[index].id();
    for (const std::shared_ptr<const TableSchema>& historical_schema : schemas_) {
      if (historical_schema->find_column(candidate_id) != nullptr) {
        return invalid("historical column identity cannot be reused");
      }
    }
  }

  schemas_.push_back(std::make_shared<const TableSchema>(std::move(successor)));
  return common::Status::ok();
}

const TableId& SchemaLineage::table_id() const noexcept {
  return schemas_.front()->table_id();
}

std::size_t SchemaLineage::size() const noexcept {
  return schemas_.size();
}

std::shared_ptr<const TableSchema> SchemaLineage::current() const noexcept {
  return schemas_.back();
}

std::shared_ptr<const TableSchema> SchemaLineage::at(const std::size_t index) const noexcept {
  if (index >= schemas_.size()) {
    return {};
  }
  return schemas_[index];
}

std::shared_ptr<const TableSchema> SchemaLineage::find(const SchemaId schema_id) const noexcept {
  for (const std::shared_ptr<const TableSchema>& schema : schemas_) {
    if (schema->schema_id() == schema_id) {
      return schema;
    }
  }
  return {};
}

std::optional<ColumnId>
SchemaLineage::historical_column_id(const std::string_view name) const noexcept {
  for (const std::shared_ptr<const TableSchema>& schema : schemas_) {
    const ColumnDefinition* column = schema->find_column(name);
    if (column != nullptr) {
      return column->id();
    }
  }
  return std::nullopt;
}

common::Result<SchemaProjection> SchemaLineage::projection(const ProjectionRequest request) const {
  std::optional<std::size_t> ancestor_index;
  std::optional<std::size_t> descendant_index;
  for (std::size_t index = 0; index < schemas_.size(); ++index) {
    if (schemas_[index]->schema_id() == request.ancestor_schema_id) {
      ancestor_index = index;
    }
    if (schemas_[index]->schema_id() == request.descendant_schema_id) {
      descendant_index = index;
    }
  }
  if (!ancestor_index.has_value() || !descendant_index.has_value()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "projection schema is not in the lineage"});
  }
  if (*ancestor_index > *descendant_index) {
    return common::make_unexpected(
        invalid("projection source must be an ancestor of the destination"));
  }

  const TableSchema& ancestor = *schemas_[*ancestor_index];
  const TableSchema& descendant = *schemas_[*descendant_index];
  std::vector<ProjectionEntry> entries;
  entries.reserve(descendant.columns().size());
  for (std::size_t index = 0; index < descendant.columns().size(); ++index) {
    std::optional<std::size_t> source_ordinal;
    if (index < ancestor.columns().size()) {
      source_ordinal = index;
    }
    entries.emplace_back(descendant.columns()[index].id(), index, source_ordinal);
  }
  return SchemaProjection{request, std::move(entries)};
}

} // namespace chronos::schema
