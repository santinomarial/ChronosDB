#ifndef CHRONOS_TESTS_COLUMNAR_COLUMNAR_TEST_SUPPORT_HPP_
#define CHRONOS_TESTS_COLUMNAR_COLUMNAR_TEST_SUPPORT_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace chronos::columnar::test {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint16_t value) {
  common::Uuid::Bytes bytes{};
  bytes[14] = static_cast<std::byte>((value >> 8U) & 0xffU);
  bytes[15] = static_cast<std::byte>(value & 0xffU);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] inline schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

inline void append_u32(std::vector<std::byte>& destination, const std::uint32_t value) {
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    destination.push_back(static_cast<std::byte>((value >> (index * 8U)) & 0xffU));
  }
}

[[nodiscard]] inline OwnedColumnVector
fixed_vector(const std::uint16_t column_id, const schema::LogicalType logical_type,
             const bool nullable, const std::uint32_t rows, std::vector<std::byte> validity,
             const std::uint32_t null_count, std::vector<std::byte> values) {
  return OwnedColumnVector::create(
             ColumnVectorMetadata{.column_id = id<schema::ColumnId>(column_id),
                                  .type = logical_type,
                                  .nullable = nullable,
                                  .row_count = rows,
                                  .null_count = null_count},
             ColumnVectorBuffers{
                 .validity = std::move(validity), .offsets = {}, .values = std::move(values)})
      .value();
}

[[nodiscard]] inline std::shared_ptr<const schema::TableSchema> batch_schema() {
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(1), "ts",
                                                     type(schema::LogicalTypeKind::kTimestampNs),
                                                     false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(2), "tag",
                                                     type(schema::LogicalTypeKind::kString), true)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(id<schema::ColumnId>(3), "enabled",
                                                     type(schema::LogicalTypeKind::kBool), false)
                        .value());
  schema::TableSchemaRoles roles{
      .event_time_column = id<schema::ColumnId>(1),
      .physical_ordering_key = {id<schema::ColumnId>(1)},
      .partition_columns = {id<schema::ColumnId>(1)},
      .shard_key = {id<schema::ColumnId>(1)},
      .deduplication_key = {},
  };
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(50), id<schema::SchemaId>(51),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns), std::move(roles))
          .value());
}

[[nodiscard]] inline std::vector<OwnedColumnVector> batch_columns() {
  std::vector<OwnedColumnVector> columns;
  columns.push_back(fixed_vector(1, type(schema::LogicalTypeKind::kTimestampNs), false, 2U, {}, 0U,
                                 std::vector<std::byte>(16U)));

  std::vector<std::byte> offsets;
  append_u32(offsets, 0U);
  append_u32(offsets, 1U);
  append_u32(offsets, 1U);
  columns.push_back(
      OwnedColumnVector::create(ColumnVectorMetadata{.column_id = id<schema::ColumnId>(2),
                                                     .type = type(schema::LogicalTypeKind::kString),
                                                     .nullable = true,
                                                     .row_count = 2U,
                                                     .null_count = 1U},
                                ColumnVectorBuffers{.validity = {std::byte{0x01}},
                                                    .offsets = std::move(offsets),
                                                    .values = {std::byte{'x'}}})
          .value());
  columns.push_back(
      fixed_vector(3, type(schema::LogicalTypeKind::kBool), false, 2U, {}, 0U, {std::byte{0x01}}));
  return columns;
}

} // namespace chronos::columnar::test

#endif // CHRONOS_TESTS_COLUMNAR_COLUMNAR_TEST_SUPPORT_HPP_
