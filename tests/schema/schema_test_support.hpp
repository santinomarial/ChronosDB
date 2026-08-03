#ifndef CHRONOS_TESTS_SCHEMA_SCHEMA_TEST_SUPPORT_HPP_
#define CHRONOS_TESTS_SCHEMA_SCHEMA_TEST_SUPPORT_HPP_

#include "chronos/common/uuid.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

namespace chronos::schema::test {

template <typename IdentifierType> [[nodiscard]] IdentifierType make_id(const std::uint16_t value) {
  common::Uuid::Bytes bytes{};
  bytes[14] = static_cast<std::byte>((value >> 8U) & 0xffU);
  bytes[15] = static_cast<std::byte>(value & 0xffU);
  return IdentifierType::from_bytes(bytes).value();
}

[[nodiscard]] inline LogicalType make_type(const LogicalTypeKind kind) {
  return LogicalType::create(kind).value();
}

[[nodiscard]] inline ColumnDefinition make_column(const std::uint16_t id, std::string name,
                                                  const LogicalTypeKind kind, const bool nullable) {
  return ColumnDefinition::create(make_id<ColumnId>(id), std::move(name), make_type(kind), nullable)
      .value();
}

} // namespace chronos::schema::test

#endif // CHRONOS_TESTS_SCHEMA_SCHEMA_TEST_SUPPORT_HPP_
