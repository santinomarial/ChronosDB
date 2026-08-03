#ifndef CHRONOS_SCHEMA_COLUMN_DEFINITION_HPP_
#define CHRONOS_SCHEMA_COLUMN_DEFINITION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <string>
#include <string_view>

namespace chronos::schema {

class ColumnDefinition {
public:
  ColumnDefinition() = delete;

  [[nodiscard]] static common::Result<ColumnDefinition>
  create(ColumnId id, std::string name, LogicalType type, bool nullable);

  [[nodiscard]] constexpr const ColumnId& id() const noexcept { return id_; }
  [[nodiscard]] const std::string& name() const noexcept { return name_; }
  [[nodiscard]] constexpr const LogicalType& type() const noexcept { return type_; }
  [[nodiscard]] constexpr bool nullable() const noexcept { return nullable_; }

  friend bool operator==(const ColumnDefinition&, const ColumnDefinition&) = default;

private:
  ColumnDefinition(ColumnId id, std::string name, LogicalType type, bool nullable) noexcept;

  ColumnId id_;
  std::string name_;
  LogicalType type_;
  bool nullable_;
};

} // namespace chronos::schema

#endif // CHRONOS_SCHEMA_COLUMN_DEFINITION_HPP_
