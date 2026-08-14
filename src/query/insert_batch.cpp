#include "chronos/common/checked_math.hpp"
#include "chronos/query/statement_binder.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) noexcept {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

void set_bit(std::vector<std::byte>& bitmap, const std::size_t row) {
  bitmap[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

template <typename Unsigned>
void store_unsigned_le(std::vector<std::byte>& bytes, const std::size_t offset,
                       const Unsigned value) {
  static_assert(std::is_unsigned_v<Unsigned>);
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] =
        static_cast<std::byte>((value >> (index * 8U)) & static_cast<Unsigned>(0xffU));
}

void store_u32_le(std::vector<std::byte>& bytes, const std::size_t offset,
                  const std::uint32_t value) {
  store_unsigned_le(bytes, offset, value);
}

[[nodiscard]] common::Result<void> store_fixed(const ScalarValue& value,
                                               const schema::LogicalTypeKind kind,
                                               std::vector<std::byte>& bytes,
                                               const std::size_t offset) {
  using schema::LogicalTypeKind;
  try {
    switch (kind) {
    case LogicalTypeKind::kInt8:
      store_unsigned_le(bytes, offset,
                        std::bit_cast<std::uint8_t>(
                            static_cast<std::int8_t>(std::get<std::int64_t>(value.storage()))));
      return {};
    case LogicalTypeKind::kInt16:
      store_unsigned_le(bytes, offset,
                        std::bit_cast<std::uint16_t>(
                            static_cast<std::int16_t>(std::get<std::int64_t>(value.storage()))));
      return {};
    case LogicalTypeKind::kInt32:
    case LogicalTypeKind::kDate:
      store_unsigned_le(bytes, offset,
                        std::bit_cast<std::uint32_t>(
                            static_cast<std::int32_t>(std::get<std::int64_t>(value.storage()))));
      return {};
    case LogicalTypeKind::kInt64:
    case LogicalTypeKind::kTimestampNs:
      store_unsigned_le(bytes, offset,
                        std::bit_cast<std::uint64_t>(std::get<std::int64_t>(value.storage())));
      return {};
    case LogicalTypeKind::kUInt8:
      store_unsigned_le(bytes, offset,
                        static_cast<std::uint8_t>(std::get<std::uint64_t>(value.storage())));
      return {};
    case LogicalTypeKind::kUInt16:
      store_unsigned_le(bytes, offset,
                        static_cast<std::uint16_t>(std::get<std::uint64_t>(value.storage())));
      return {};
    case LogicalTypeKind::kUInt32:
      store_unsigned_le(bytes, offset,
                        static_cast<std::uint32_t>(std::get<std::uint64_t>(value.storage())));
      return {};
    case LogicalTypeKind::kUInt64:
      store_unsigned_le(bytes, offset, std::get<std::uint64_t>(value.storage()));
      return {};
    case LogicalTypeKind::kFloat32:
      store_unsigned_le(bytes, offset,
                        std::bit_cast<std::uint32_t>(std::get<float>(value.storage())));
      return {};
    case LogicalTypeKind::kFloat64:
      store_unsigned_le(bytes, offset,
                        std::bit_cast<std::uint64_t>(std::get<double>(value.storage())));
      return {};
    case LogicalTypeKind::kDecimal: {
      const auto& coefficient = std::get<Decimal128Value>(value.storage()).coefficient;
      std::ranges::copy(coefficient, bytes.begin() + static_cast<std::ptrdiff_t>(offset));
      return {};
    }
    case LogicalTypeKind::kUuid: {
      const auto& uuid = std::get<common::Uuid>(value.storage()).bytes();
      std::ranges::copy(uuid, bytes.begin() + static_cast<std::ptrdiff_t>(offset));
      return {};
    }
    case LogicalTypeKind::kBool:
    case LogicalTypeKind::kSymbol:
    case LogicalTypeKind::kString:
    case LogicalTypeKind::kBinary:
      return common::make_unexpected(invalid("INSERT fixed value has a variable physical type"));
    }
  } catch (const std::bad_variant_access&) {
    return common::make_unexpected(invalid("INSERT scalar storage disagrees with its schema type"));
  }
  return common::make_unexpected(invalid("INSERT scalar has an invalid logical type"));
}

[[nodiscard]] common::Result<common::ByteView> variable_bytes(const ScalarValue& value) {
  if (const auto* text = std::get_if<std::string>(&value.storage()); text != nullptr)
    return std::as_bytes(std::span{text->data(), text->size()});
  if (const auto* binary = std::get_if<std::vector<std::byte>>(&value.storage()); binary != nullptr)
    return common::ByteView{binary->data(), binary->size()};
  return common::make_unexpected(
      invalid("INSERT variable scalar storage disagrees with its schema type"));
}

} // namespace

common::Result<columnar::OwnedColumnarBatch>
materialize_sql_v1_insert_batch(const MaterializedSqlInsert& statement,
                                const columnar::ColumnarBatchLimits limits) {
  const std::shared_ptr<const schema::TableSchema>& retained_schema = statement.schema_ptr();
  if (retained_schema == nullptr || statement.rows().empty() ||
      statement.rows().size() > std::numeric_limits<std::uint32_t>::max())
    return common::make_unexpected(
        invalid("INSERT batch requires a schema and finite nonempty rows"));
  const std::uint32_t row_count = static_cast<std::uint32_t>(statement.rows().size());
  if (row_count > limits.max_rows || retained_schema->columns().size() > limits.max_columns)
    return common::make_unexpected(exhausted("INSERT batch shape exceeds columnar limits"));
  try {
    for (const std::vector<ScalarValue>& row : statement.rows()) {
      if (row.size() != retained_schema->columns().size())
        return common::make_unexpected(invalid("INSERT row width disagrees with its schema"));
    }
    std::vector<columnar::OwnedColumnVector> columns;
    columns.reserve(retained_schema->columns().size());
    for (std::size_t ordinal = 0U; ordinal < retained_schema->columns().size(); ++ordinal) {
      const schema::ColumnDefinition& definition = retained_schema->columns()[ordinal];
      columnar::ColumnVectorBuffers buffers;
      if (definition.nullable())
        buffers.validity.resize(columnar::bitmap_size(row_count));
      if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
        buffers.values.resize(columnar::bitmap_size(row_count));
      } else if (definition.type().is_variable_width()) {
        const auto offset_count =
            common::checked_add(static_cast<std::size_t>(row_count), std::size_t{1U});
        const auto offset_bytes =
            offset_count.has_value()
                ? common::checked_multiply(*offset_count, sizeof(std::uint32_t))
                : std::nullopt;
        if (!offset_bytes.has_value())
          return common::make_unexpected(exhausted("INSERT offset bytes exceed limits"));
        buffers.offsets.resize(*offset_bytes);
      } else {
        const auto bytes = common::checked_multiply(static_cast<std::size_t>(row_count),
                                                    fixed_width(definition.type().kind()));
        if (!bytes.has_value())
          return common::make_unexpected(exhausted("INSERT fixed bytes exceed limits"));
        buffers.values.resize(*bytes);
      }

      std::uint32_t null_count{};
      const std::size_t width = fixed_width(definition.type().kind());
      for (std::size_t row = 0U; row < statement.rows().size(); ++row) {
        const ScalarValue& value = statement.rows()[row][ordinal];
        if (value.type() != definition.type())
          return common::make_unexpected(invalid("INSERT scalar type disagrees with its schema"));
        if (value.is_null()) {
          if (!definition.nullable())
            return common::make_unexpected(invalid("INSERT has NULL in a non-null column"));
          ++null_count;
        } else {
          if (definition.nullable())
            set_bit(buffers.validity, row);
          if (definition.type().kind() == schema::LogicalTypeKind::kBool) {
            const auto* boolean = std::get_if<bool>(&value.storage());
            if (boolean == nullptr)
              return common::make_unexpected(invalid("INSERT BOOL scalar storage is invalid"));
            if (*boolean)
              set_bit(buffers.values, row);
          } else if (definition.type().is_variable_width()) {
            auto bytes = variable_bytes(value);
            if (!bytes.has_value())
              return common::make_unexpected(bytes.error());
            if (buffers.values.size() > std::numeric_limits<std::uint32_t>::max() ||
                bytes->size() > std::numeric_limits<std::uint32_t>::max() - buffers.values.size())
              return common::make_unexpected(exhausted("INSERT variable bytes exceed u32 offsets"));
            buffers.values.insert(buffers.values.end(), bytes->begin(), bytes->end());
          } else {
            auto stored = store_fixed(value, definition.type().kind(), buffers.values, row * width);
            if (!stored.has_value())
              return common::make_unexpected(stored.error());
          }
        }
        if (definition.type().is_variable_width())
          store_u32_le(buffers.offsets, (row + 1U) * sizeof(std::uint32_t),
                       static_cast<std::uint32_t>(buffers.values.size()));
      }
      auto column = columnar::OwnedColumnVector::create({.column_id = definition.id(),
                                                         .type = definition.type(),
                                                         .nullable = definition.nullable(),
                                                         .row_count = row_count,
                                                         .null_count = null_count},
                                                        std::move(buffers));
      if (!column.has_value())
        return common::make_unexpected(column.error());
      columns.push_back(std::move(*column));
    }
    return columnar::OwnedColumnarBatch::create(retained_schema, std::move(columns), limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("INSERT batch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("INSERT batch exceeds container limits"));
  }
}

} // namespace chronos::query
