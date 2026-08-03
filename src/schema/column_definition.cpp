#include "chronos/schema/column_definition.hpp"

#include "chronos/schema/utf8.hpp"

#include <utility>

namespace chronos::schema {

ColumnDefinition::ColumnDefinition(ColumnId id, std::string name, LogicalType type,
                                   const bool nullable) noexcept
    : id_(id), name_(std::move(name)), type_(type), nullable_(nullable) {}

common::Result<ColumnDefinition> ColumnDefinition::create(ColumnId id, std::string name,
                                                          LogicalType type, const bool nullable) {
  if (name.empty()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "column name must not be empty"});
  }
  if (!is_valid_utf8(name)) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "column name must be valid UTF-8"});
  }
  if (name.find('\0') != std::string::npos) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "column name must not contain U+0000"});
  }
  return ColumnDefinition{id, std::move(name), type, nullable};
}

} // namespace chronos::schema
