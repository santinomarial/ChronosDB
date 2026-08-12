#include "chronos/common/crc32c.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"

#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace chronos::raft {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] CatalogTableDefinition definition(const bool quoted = false) {
  const schema::ColumnId timestamp = id<schema::ColumnId>(3U);
  const schema::ColumnId symbol = id<schema::ColumnId>(4U);
  const schema::ColumnId value = id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        timestamp, "ts", type(schema::LogicalTypeKind::kTimestampNs), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(symbol, "symbol",
                                                     type(schema::LogicalTypeKind::kSymbol), false)
                        .value());
  columns.push_back(schema::ColumnDefinition::create(
                        value, "value", schema::LogicalType::decimal(18U, 4U).value(), true)
                        .value());
  auto table = schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                           schema::SchemaVersion::initial(), std::nullopt,
                                           std::move(columns),
                                           {.event_time_column = timestamp,
                                            .physical_ordering_key = {symbol, timestamp},
                                            .partition_columns = {timestamp},
                                            .shard_key = {symbol},
                                            .deduplication_key = {symbol, timestamp}});
  return {.name = quoted ? "Trades" : "trades",
          .quoted = quoted,
          .schema = std::make_shared<const schema::TableSchema>(std::move(*table))};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>(value >> (index * 8U));
}

void refresh_checksums(std::vector<std::byte>& bytes) {
  store_u32(bytes, 24U,
            common::crc32c(common::ByteView{bytes}.subspan(
                kSchemaDefinitionHeaderSize,
                bytes.size() - kSchemaDefinitionHeaderSize - kSchemaDefinitionTrailerSize)));
  store_u32(bytes, 36U, 0U);
  store_u32(bytes, 36U, common::crc32c(common::ByteView{bytes}.first(kSchemaDefinitionHeaderSize)));
  store_u32(bytes, bytes.size() - kSchemaDefinitionTrailerSize, 0U);
  store_u32(
      bytes, bytes.size() - kSchemaDefinitionTrailerSize,
      common::crc32c(common::ByteView{bytes}.first(bytes.size() - kSchemaDefinitionTrailerSize)));
}

TEST(SchemaDefinitionCodecTest, RoundTripsCompleteImmutableSchemaDeterministically) {
  const CatalogTableDefinition input = definition();
  auto encoded = encode_schema_definition_v1(input);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  auto decoded = decode_schema_definition_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_TRUE(*decoded == input);
  EXPECT_EQ(encode_schema_definition_v1(*decoded).value(), *encoded);

  const CatalogTableDefinition exact_case = definition(true);
  auto exact = decode_schema_definition_v1(encode_schema_definition_v1(exact_case).value());
  ASSERT_TRUE(exact.has_value());
  EXPECT_EQ(exact->name, "Trades");
  EXPECT_TRUE(exact->quoted);
}

TEST(SchemaDefinitionCodecTest, RejectsEveryTruncationDamageAndUnknownVersion) {
  const auto encoded = encode_schema_definition_v1(definition()).value();
  for (std::size_t size = 0U; size < encoded.size(); ++size) {
    EXPECT_FALSE(decode_schema_definition_v1(common::ByteView{encoded}.first(size)).has_value())
        << size;
  }
  auto damaged = encoded;
  damaged[kSchemaDefinitionHeaderSize + 20U] ^= std::byte{1U};
  EXPECT_EQ(decode_schema_definition_v1(damaged).error().code(), common::StatusCode::kCorruption);

  auto future = encoded;
  future[8U] = std::byte{2U};
  refresh_checksums(future);
  EXPECT_EQ(decode_schema_definition_v1(future).error().code(), common::StatusCode::kNotSupported);

  auto future_type = encoded;
  constexpr std::size_t kFirstColumnTypeOffset = kSchemaDefinitionHeaderSize + 92U + 6U + 16U;
  future_type[kFirstColumnTypeOffset] = std::byte{0xffU};
  future_type[kFirstColumnTypeOffset + 1U] = std::byte{0xffU};
  refresh_checksums(future_type);
  EXPECT_EQ(decode_schema_definition_v1(future_type).error().code(),
            common::StatusCode::kNotSupported);
}

TEST(SchemaDefinitionCodecTest, EnforcesRuntimeBoundsBeforeRetainingSchema) {
  const auto input = definition();
  EXPECT_EQ(encode_schema_definition_v1(input, {.maximum_table_name_bytes = 3U}).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(encode_schema_definition_v1(input, {.maximum_columns = 2U}).error().code(),
            common::StatusCode::kResourceExhausted);
  const auto encoded = encode_schema_definition_v1(input).value();
  EXPECT_EQ(decode_schema_definition_v1(encoded, {.maximum_column_name_bytes = 2U}).error().code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::raft
