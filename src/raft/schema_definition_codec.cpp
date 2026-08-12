#include "chronos/raft/schema_definition_codec.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::array<std::byte, 8U> kMagic{std::byte{'C'}, std::byte{'H'}, std::byte{'R'},
                                           std::byte{'N'}, std::byte{'S'}, std::byte{'C'},
                                           std::byte{'H'}, std::byte{0U}};
constexpr std::uint16_t kMajor = 1U;
constexpr std::uint16_t kMinor = 0U;
constexpr std::size_t kHeaderCrcOffset = 36U;
constexpr std::size_t kSchemaFixedPayloadSize = 92U;
constexpr std::size_t kColumnFixedSize = 32U;

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}
[[nodiscard]] common::Status unsupported(std::string message) {
  return {common::StatusCode::kNotSupported, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] bool valid_limits(const SchemaDefinitionCodecLimits& limits) noexcept {
  return limits.maximum_definition_bytes >=
             kSchemaDefinitionHeaderSize + kSchemaFixedPayloadSize + kSchemaDefinitionTrailerSize &&
         limits.maximum_definition_bytes <= kMaximumSchemaDefinitionSize &&
         limits.maximum_table_name_bytes > 0U &&
         limits.maximum_table_name_bytes <= kMaximumSchemaDefinitionSize &&
         limits.maximum_column_name_bytes > 0U &&
         limits.maximum_column_name_bytes <= kMaximumSchemaDefinitionSize &&
         limits.maximum_columns > 0U &&
         limits.maximum_columns <= schema::kMaximumSchemaColumnCount &&
         limits.maximum_role_columns > 0U &&
         limits.maximum_role_columns <= schema::kMaximumSchemaColumnCount;
}

[[nodiscard]] bool valid_unquoted_name(const std::string_view name) noexcept {
  if (name.empty())
    return false;
  const auto first = [](const char value) {
    return (value >= 'a' && value <= 'z') || value == '_';
  };
  const auto rest = [&](const char value) {
    return first(value) || (value >= '0' && value <= '9');
  };
  return first(name.front()) && std::ranges::all_of(name.begin() + 1, name.end(), rest);
}

void store_u32(std::span<std::byte> bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

[[nodiscard]] std::uint16_t load_u16(const common::ByteView bytes, const std::size_t offset) {
  return static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset])) |
         static_cast<std::uint16_t>(std::to_integer<std::uint8_t>(bytes[offset + 1U]) << 8U);
}

[[nodiscard]] std::uint32_t load_u32(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    value |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(bytes[offset + index]))
             << (index * 8U);
  }
  return value;
}

[[nodiscard]] common::Result<std::size_t> add_size(const std::size_t left,
                                                   const std::size_t right) {
  auto total = common::checked_add(left, right);
  return total.has_value()
             ? common::Result<std::size_t>{*total}
             : common::make_unexpected(exhausted("schema definition size overflowed"));
}

[[nodiscard]] common::Status write_uuid(common::ByteWriter& writer, const common::Uuid& value) {
  return writer.write_exact(value.bytes());
}

template <typename Identifier>
[[nodiscard]] common::Result<Identifier> read_identifier(common::ByteReader& reader) {
  auto bytes = reader.read_exact(common::Uuid::kSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::Uuid::Bytes owned{};
  std::ranges::copy(*bytes, owned.begin());
  auto value = Identifier::from_bytes(owned);
  if (!value.has_value())
    return common::make_unexpected(corruption("schema definition identity is nil"));
  return *value;
}

[[nodiscard]] common::Result<std::string>
read_string(common::ByteReader& reader, const std::size_t length, const std::size_t maximum) {
  if (length == 0U || length > maximum || length > reader.remaining())
    return common::make_unexpected(corruption("schema definition string length is invalid"));
  auto bytes = reader.read_exact(length);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  if (!schema::is_valid_utf8(*bytes) || std::ranges::find(*bytes, std::byte{0U}) != bytes->end()) {
    return common::make_unexpected(corruption("schema definition string is invalid UTF-8"));
  }
  try {
    std::string value(bytes->size(), '\0');
    std::memcpy(value.data(), bytes->data(), bytes->size());
    return value;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("schema definition string allocation failed"));
  }
}

[[nodiscard]] common::Result<std::vector<schema::ColumnId>>
read_roles(common::ByteReader& reader, const std::uint32_t count,
           const SchemaDefinitionCodecLimits& limits) {
  if (count > limits.maximum_role_columns)
    return common::make_unexpected(exhausted("schema definition role count exceeds limit"));
  try {
    std::vector<schema::ColumnId> values;
    values.reserve(count);
    for (std::uint32_t index = 0U; index < count; ++index) {
      auto value = read_identifier<schema::ColumnId>(reader);
      if (!value.has_value())
        return common::make_unexpected(value.error());
      values.push_back(*value);
    }
    return values;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("schema definition role allocation failed"));
  }
}

} // namespace

common::Result<std::vector<std::byte>>
encode_schema_definition_v1(const CatalogTableDefinition& definition,
                            const SchemaDefinitionCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("schema definition codec limits are invalid"));
  if (definition.schema == nullptr || definition.name.empty() ||
      definition.name.size() > limits.maximum_table_name_bytes ||
      definition.name.size() > std::numeric_limits<std::uint32_t>::max() ||
      !schema::is_valid_utf8(definition.name) || definition.name.find('\0') != std::string::npos ||
      (!definition.quoted && !valid_unquoted_name(definition.name))) {
    return common::make_unexpected(invalid("schema definition table name is invalid"));
  }
  const schema::TableSchema& value = *definition.schema;
  if (value.columns().size() > limits.maximum_columns ||
      value.physical_ordering_key().size() > limits.maximum_role_columns ||
      value.partition_columns().size() > limits.maximum_role_columns ||
      value.shard_key().size() > limits.maximum_role_columns ||
      value.deduplication_key().size() > limits.maximum_role_columns) {
    return common::make_unexpected(exhausted("schema definition count exceeds codec limit"));
  }

  std::size_t payload_size = kSchemaFixedPayloadSize;
  auto next = add_size(payload_size, definition.name.size());
  if (!next.has_value())
    return common::make_unexpected(next.error());
  payload_size = *next;
  for (const schema::ColumnDefinition& column : value.columns()) {
    if (column.name().size() > limits.maximum_column_name_bytes ||
        column.name().size() > std::numeric_limits<std::uint32_t>::max()) {
      return common::make_unexpected(exhausted("schema definition column name exceeds limit"));
    }
    next = add_size(payload_size, kColumnFixedSize + column.name().size());
    if (!next.has_value())
      return common::make_unexpected(next.error());
    payload_size = *next;
  }
  const std::size_t role_count = 1U + value.physical_ordering_key().size() +
                                 value.partition_columns().size() + value.shard_key().size() +
                                 value.deduplication_key().size();
  next = add_size(payload_size, role_count * common::Uuid::kSize);
  if (!next.has_value())
    return common::make_unexpected(next.error());
  payload_size = *next;
  next = add_size(kSchemaDefinitionHeaderSize + kSchemaDefinitionTrailerSize, payload_size);
  if (!next.has_value() || *next > limits.maximum_definition_bytes ||
      *next > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(exhausted("schema definition exceeds codec limit"));
  }

  try {
    std::vector<std::byte> bytes(*next, std::byte{0U});
    common::ByteWriter writer{bytes};
    for (const common::Status& status :
         {writer.write_exact(kMagic), writer.write_u16_le(kMajor), writer.write_u16_le(kMinor),
          writer.write_u32_le(kSchemaDefinitionHeaderSize),
          writer.write_u32_le(static_cast<std::uint32_t>(bytes.size())),
          writer.write_u32_le(static_cast<std::uint32_t>(payload_size)), writer.write_u32_le(0U),
          writer.write_u32_le(0U), writer.write_u32_le(0U), writer.write_u32_le(0U),
          writer.zero_fill(8U)}) {
      if (!status.is_ok())
        return common::make_unexpected(status);
    }
    for (const common::Status& status :
         {write_uuid(writer, value.table_id().uuid()), write_uuid(writer, value.schema_id().uuid()),
          writer.write_u64_le(value.version().value()),
          writer.write_u8(value.parent_schema_id().has_value() ? 1U : 0U),
          writer.write_u8(definition.quoted ? 1U : 0U), writer.zero_fill(2U),
          writer.write_u32_le(static_cast<std::uint32_t>(definition.name.size())),
          writer.write_u32_le(static_cast<std::uint32_t>(value.columns().size())),
          writer.write_u32_le(static_cast<std::uint32_t>(value.physical_ordering_key().size())),
          writer.write_u32_le(static_cast<std::uint32_t>(value.partition_columns().size())),
          writer.write_u32_le(static_cast<std::uint32_t>(value.shard_key().size())),
          writer.write_u32_le(static_cast<std::uint32_t>(value.deduplication_key().size())),
          writer.zero_fill(8U),
          write_uuid(writer, value.parent_schema_id().has_value() ? value.parent_schema_id()->uuid()
                                                                  : common::Uuid{}),
          writer.write_exact(std::as_bytes(std::span{definition.name}))}) {
      if (!status.is_ok())
        return common::make_unexpected(status);
    }
    for (const schema::ColumnDefinition& column : value.columns()) {
      for (const common::Status& status :
           {write_uuid(writer, column.id().uuid()), writer.write_u16_le(column.type().code()),
            writer.write_u16_le(column.type().parameter_0()),
            writer.write_u16_le(column.type().parameter_1()),
            writer.write_u8(column.nullable() ? 1U : 0U), writer.write_u8(0U),
            writer.write_u32_le(static_cast<std::uint32_t>(column.name().size())),
            writer.write_u32_le(0U), writer.write_exact(std::as_bytes(std::span{column.name()}))}) {
        if (!status.is_ok())
          return common::make_unexpected(status);
      }
    }
    if (!write_uuid(writer, value.event_time_column().uuid()).is_ok())
      return common::make_unexpected(corruption("schema definition event time write failed"));
    for (const auto roles : {value.physical_ordering_key(), value.partition_columns(),
                             value.shard_key(), value.deduplication_key()}) {
      for (const schema::ColumnId id : roles) {
        if (auto status = write_uuid(writer, id.uuid()); !status.is_ok())
          return common::make_unexpected(status);
      }
    }
    if (!writer.write_u32_le(0U).is_ok() || !writer.full())
      return common::make_unexpected(corruption("schema definition encoded size is inconsistent"));
    const common::ByteView payload =
        common::ByteView{bytes}.subspan(kSchemaDefinitionHeaderSize, payload_size);
    store_u32(bytes, 24U, common::crc32c(payload));
    store_u32(bytes, kHeaderCrcOffset,
              common::crc32c(common::ByteView{bytes}.first(kSchemaDefinitionHeaderSize)));
    store_u32(
        bytes, bytes.size() - kSchemaDefinitionTrailerSize,
        common::crc32c(common::ByteView{bytes}.first(bytes.size() - kSchemaDefinitionTrailerSize)));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("schema definition allocation failed"));
  }
}

common::Result<CatalogTableDefinition>
decode_schema_definition_v1(const common::ByteView bytes,
                            const SchemaDefinitionCodecLimits limits) {
  if (!valid_limits(limits))
    return common::make_unexpected(invalid("schema definition codec limits are invalid"));
  if (bytes.size() <
      kSchemaDefinitionHeaderSize + kSchemaFixedPayloadSize + kSchemaDefinitionTrailerSize) {
    return common::make_unexpected(corruption("schema definition is shorter than fixed framing"));
  }
  std::array<std::byte, kSchemaDefinitionHeaderSize> header{};
  std::ranges::copy(bytes.first(kSchemaDefinitionHeaderSize), header.begin());
  const std::uint32_t header_crc = load_u32(bytes, kHeaderCrcOffset);
  store_u32(header, kHeaderCrcOffset, 0U);
  if (common::crc32c(header) != header_crc)
    return common::make_unexpected(corruption("schema definition header checksum mismatch"));
  if (!std::ranges::equal(bytes.first(kMagic.size()), kMagic) || load_u16(bytes, 8U) != kMajor)
    return common::make_unexpected(unsupported("schema definition magic or major version unknown"));
  if (load_u16(bytes, 10U) != kMinor || load_u32(bytes, 12U) != kSchemaDefinitionHeaderSize)
    return common::make_unexpected(
        unsupported("schema definition minor or header version unknown"));
  const std::uint32_t total_size = load_u32(bytes, 16U);
  const std::uint32_t payload_size = load_u32(bytes, 20U);
  if (total_size != bytes.size() || total_size > limits.maximum_definition_bytes ||
      payload_size != total_size - kSchemaDefinitionHeaderSize - kSchemaDefinitionTrailerSize ||
      load_u32(bytes, 28U) != 0U || load_u32(bytes, 32U) != 0U ||
      std::ranges::any_of(bytes.subspan(40U, 8U),
                          [](const std::byte byte) { return byte != std::byte{0U}; })) {
    return common::make_unexpected(
        corruption("schema definition header relationships are invalid"));
  }
  const common::ByteView payload = bytes.subspan(kSchemaDefinitionHeaderSize, payload_size);
  if (common::crc32c(payload) != load_u32(bytes, 24U) ||
      common::crc32c(bytes.first(bytes.size() - kSchemaDefinitionTrailerSize)) !=
          load_u32(bytes, bytes.size() - kSchemaDefinitionTrailerSize)) {
    return common::make_unexpected(
        corruption("schema definition payload or trailer checksum mismatch"));
  }

  try {
    common::ByteReader reader{payload};
    auto table_id = read_identifier<schema::TableId>(reader);
    auto schema_id = read_identifier<schema::SchemaId>(reader);
    auto version = reader.read_u64_le();
    auto has_parent = reader.read_u8();
    auto quoted = reader.read_u8();
    auto reserved = reader.read_exact(2U);
    auto table_name_length = reader.read_u32_le();
    auto column_count = reader.read_u32_le();
    auto ordering_count = reader.read_u32_le();
    auto partition_count = reader.read_u32_le();
    auto shard_count = reader.read_u32_le();
    auto dedup_count = reader.read_u32_le();
    auto reserved2 = reader.read_exact(8U);
    if (!table_id.has_value() || !schema_id.has_value() || !version.has_value() ||
        !has_parent.has_value() || !quoted.has_value() || !reserved.has_value() ||
        !table_name_length.has_value() || !column_count.has_value() ||
        !ordering_count.has_value() || !partition_count.has_value() || !shard_count.has_value() ||
        !dedup_count.has_value() || !reserved2.has_value() || *has_parent > 1U || *quoted > 1U ||
        *column_count == 0U || *column_count > limits.maximum_columns ||
        *ordering_count > limits.maximum_role_columns ||
        *partition_count > limits.maximum_role_columns ||
        *shard_count > limits.maximum_role_columns || *dedup_count > limits.maximum_role_columns ||
        std::ranges::any_of(*reserved,
                            [](const std::byte byte) { return byte != std::byte{0U}; }) ||
        std::ranges::any_of(*reserved2,
                            [](const std::byte byte) { return byte != std::byte{0U}; })) {
      return common::make_unexpected(corruption("schema definition fixed payload is invalid"));
    }
    auto parent_bytes = reader.read_exact(common::Uuid::kSize);
    if (!parent_bytes.has_value())
      return common::make_unexpected(parent_bytes.error());
    common::Uuid::Bytes parent_owned{};
    std::ranges::copy(*parent_bytes, parent_owned.begin());
    const common::Uuid parent_uuid{parent_owned};
    std::optional<schema::SchemaId> parent;
    if (*has_parent == 0U) {
      if (!parent_uuid.is_nil())
        return common::make_unexpected(corruption("schema definition absent parent is nonzero"));
    } else {
      auto parsed = schema::SchemaId::from_uuid(parent_uuid);
      if (!parsed.has_value())
        return common::make_unexpected(corruption("schema definition parent is nil"));
      parent = *parsed;
    }
    auto name = read_string(reader, *table_name_length, limits.maximum_table_name_bytes);
    if (!name.has_value() || (*quoted == 0U && !valid_unquoted_name(*name)))
      return common::make_unexpected(name.has_value()
                                         ? corruption("schema definition unquoted name is invalid")
                                         : name.error());

    std::vector<schema::ColumnDefinition> columns;
    columns.reserve(*column_count);
    for (std::uint32_t index = 0U; index < *column_count; ++index) {
      auto id = read_identifier<schema::ColumnId>(reader);
      auto type_code = reader.read_u16_le();
      auto parameter0 = reader.read_u16_le();
      auto parameter1 = reader.read_u16_le();
      auto nullable = reader.read_u8();
      auto column_reserved = reader.read_u8();
      auto column_name_length = reader.read_u32_le();
      auto column_reserved2 = reader.read_u32_le();
      if (!id.has_value() || !type_code.has_value() || !parameter0.has_value() ||
          !parameter1.has_value() || !nullable.has_value() || !column_reserved.has_value() ||
          !column_name_length.has_value() || !column_reserved2.has_value() || *nullable > 1U ||
          *column_reserved != 0U || *column_reserved2 != 0U) {
        return common::make_unexpected(corruption("schema definition column envelope is invalid"));
      }
      auto kind = schema::logical_type_kind_from_code(*type_code);
      if (!kind.has_value())
        return common::make_unexpected(
            unsupported("schema definition logical type is unsupported"));
      auto type = schema::LogicalType::create(*kind, *parameter0, *parameter1);
      auto column_name = read_string(reader, *column_name_length, limits.maximum_column_name_bytes);
      if (!type.has_value() || !column_name.has_value())
        return common::make_unexpected(corruption("schema definition column is invalid"));
      auto column =
          schema::ColumnDefinition::create(*id, std::move(*column_name), *type, *nullable != 0U);
      if (!column.has_value())
        return common::make_unexpected(
            corruption("schema definition column semantics are invalid"));
      columns.push_back(std::move(*column));
    }
    auto event_time = read_identifier<schema::ColumnId>(reader);
    auto ordering = read_roles(reader, *ordering_count, limits);
    auto partition = read_roles(reader, *partition_count, limits);
    auto shard = read_roles(reader, *shard_count, limits);
    auto dedup = read_roles(reader, *dedup_count, limits);
    if (!event_time.has_value() || !ordering.has_value() || !partition.has_value() ||
        !shard.has_value() || !dedup.has_value() || !reader.empty()) {
      return common::make_unexpected(corruption("schema definition roles are invalid"));
    }
    auto schema_version = schema::SchemaVersion::from_value(*version);
    if (!schema_version.has_value())
      return common::make_unexpected(corruption("schema definition version is invalid"));
    auto table_schema = schema::TableSchema::create(*table_id, *schema_id, *schema_version, parent,
                                                    std::move(columns),
                                                    {.event_time_column = *event_time,
                                                     .physical_ordering_key = std::move(*ordering),
                                                     .partition_columns = std::move(*partition),
                                                     .shard_key = std::move(*shard),
                                                     .deduplication_key = std::move(*dedup)});
    if (!table_schema.has_value())
      return common::make_unexpected(corruption("schema definition table semantics are invalid"));
    return CatalogTableDefinition{
        .name = std::move(*name),
        .quoted = *quoted != 0U,
        .schema = std::make_shared<const schema::TableSchema>(std::move(*table_schema))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("schema definition decode allocation failed"));
  }
}

} // namespace chronos::raft
