#include "chronos/columnar/columnar_batch_codec.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

namespace chronos::columnar {
namespace {

[[nodiscard]] std::vector<OwnedColumnVector> generated_columns(const std::uint32_t rows,
                                                               std::mt19937& generator) {
  std::vector<std::byte> timestamps(static_cast<std::size_t>(rows) * 8U);
  for (std::byte& value : timestamps) {
    value = static_cast<std::byte>(generator() & 0xffU);
  }

  std::vector<std::byte> validity(bitmap_size(rows), std::byte{0U});
  std::vector<std::byte> offsets;
  std::vector<std::byte> strings;
  test::append_u32(offsets, 0U);
  std::uint32_t nulls = 0U;
  for (std::uint32_t row = 0U; row < rows; ++row) {
    const bool present = (generator() % 5U) != 0U;
    if (present) {
      validity[static_cast<std::size_t>(row) / 8U] |= static_cast<std::byte>(1U << (row % 8U));
      const std::uint32_t length = generator() % 9U;
      for (std::uint32_t index = 0U; index < length; ++index) {
        strings.push_back(static_cast<std::byte>('a' + (generator() % 26U)));
      }
    } else {
      ++nulls;
    }
    test::append_u32(offsets, static_cast<std::uint32_t>(strings.size()));
  }

  std::vector<std::byte> booleans(bitmap_size(rows), std::byte{0U});
  for (std::uint32_t row = 0U; row < rows; ++row) {
    if ((generator() & 1U) != 0U) {
      booleans[static_cast<std::size_t>(row) / 8U] |= static_cast<std::byte>(1U << (row % 8U));
    }
  }

  std::vector<OwnedColumnVector> columns;
  columns.push_back(test::fixed_vector(1, test::type(schema::LogicalTypeKind::kTimestampNs), false,
                                       rows, {}, 0U, std::move(timestamps)));
  columns.push_back(OwnedColumnVector::create(
                        ColumnVectorMetadata{.column_id = test::id<schema::ColumnId>(2),
                                             .type = test::type(schema::LogicalTypeKind::kString),
                                             .nullable = true,
                                             .row_count = rows,
                                             .null_count = nulls},
                        ColumnVectorBuffers{.validity = std::move(validity),
                                            .offsets = std::move(offsets),
                                            .values = std::move(strings)})
                        .value());
  columns.push_back(test::fixed_vector(3, test::type(schema::LogicalTypeKind::kBool), false, rows,
                                       {}, 0U, std::move(booleans)));
  return columns;
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) {
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

TEST(ColumnarBatchCodecPropertyTest, DeterministicGeneratedBatchesRoundTripEveryExactBuffer) {
  constexpr std::uint32_t kSeed = 0x43423150U;
  std::mt19937 generator{kSeed};
  for (std::uint32_t trial = 0U; trial < 150U; ++trial) {
    const std::uint32_t rows = 1U + generator() % 257U;
    const OwnedColumnarBatch batch =
        OwnedColumnarBatch::create(test::batch_schema(), generated_columns(rows, generator))
            .value();
    const auto first = encode_columnar_batch_v1(batch);
    const auto second = encode_columnar_batch_v1(batch);
    ASSERT_TRUE(first.has_value()) << "seed=" << kSeed << " trial=" << trial;
    ASSERT_TRUE(second.has_value());
    EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes()));

    const auto decoded = decode_columnar_batch_v1_exact(first->bytes());
    ASSERT_TRUE(decoded.has_value())
        << "seed=" << kSeed << " trial=" << trial << ' ' << decoded.error().status().to_string();
    ASSERT_EQ(decoded->columns().size(), batch.columns().size());
    for (std::size_t ordinal = 0U; ordinal < batch.columns().size(); ++ordinal) {
      const ColumnVectorView expected = batch.columns()[ordinal].view();
      const ColumnVectorView& actual = decoded->columns()[ordinal];
      EXPECT_TRUE(std::ranges::equal(actual.validity(), expected.validity()));
      EXPECT_TRUE(std::ranges::equal(actual.offsets(), expected.offsets()));
      EXPECT_TRUE(std::ranges::equal(actual.values(), expected.values()));
    }
  }
}

TEST(ColumnarBatchCodecPropertyTest, EverySingleByteGoldenMutationFailsIntegrityValidation) {
  const OwnedColumnarBatch batch =
      OwnedColumnarBatch::create(test::batch_schema(), test::batch_columns()).value();
  const EncodedColumnarBatch encoded = encode_columnar_batch_v1(batch).value();
  std::vector<std::byte> mutated(encoded.bytes().begin(), encoded.bytes().end());
  for (std::size_t offset = 0U; offset < mutated.size(); ++offset) {
    SCOPED_TRACE(offset);
    mutated[offset] ^= std::byte{1U};
    const auto decoded = decode_columnar_batch_v1_exact(mutated);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), ColumnarBatchDecodeErrorKind::kInvalid);
    mutated[offset] ^= std::byte{1U};
  }
}

TEST(ColumnarBatchCodecPropertyTest, EveryFrozenLogicalTypeCodeRoundTripsPhysically) {
  std::vector<schema::ColumnDefinition> definitions;
  std::vector<OwnedColumnVector> vectors;
  for (std::uint16_t code = 1U; code <= 18U; ++code) {
    const schema::LogicalTypeKind kind = schema::logical_type_kind_from_code(code).value();
    const schema::LogicalType type = kind == schema::LogicalTypeKind::kDecimal
                                         ? schema::LogicalType::decimal(38U, 9U).value()
                                         : schema::LogicalType::create(kind).value();
    definitions.push_back(schema::ColumnDefinition::create(test::id<schema::ColumnId>(code),
                                                           std::string{"c"} + std::to_string(code),
                                                           type, false)
                              .value());
    if (type.is_variable_width()) {
      std::vector<std::byte> offsets;
      test::append_u32(offsets, 0U);
      test::append_u32(offsets, 1U);
      vectors.push_back(OwnedColumnVector::create(
                            ColumnVectorMetadata{.column_id = test::id<schema::ColumnId>(code),
                                                 .type = type,
                                                 .nullable = false,
                                                 .row_count = 1U,
                                                 .null_count = 0U},
                            ColumnVectorBuffers{.validity = {},
                                                .offsets = std::move(offsets),
                                                .values = {std::byte{'x'}}})
                            .value());
    } else {
      const std::size_t width = kind == schema::LogicalTypeKind::kBool ? 1U : fixed_width(kind);
      std::vector<std::byte> values(width, std::byte{0U});
      if (kind == schema::LogicalTypeKind::kBool) {
        values[0] = std::byte{1U};
      }
      vectors.push_back(test::fixed_vector(code, type, false, 1U, {}, 0U, std::move(values)));
    }
  }

  const schema::ColumnId event_time = test::id<schema::ColumnId>(13U);
  schema::TableSchemaRoles roles{.event_time_column = event_time,
                                 .physical_ordering_key = {event_time},
                                 .partition_columns = {event_time},
                                 .shard_key = {event_time},
                                 .deduplication_key = {}};
  const auto schema = std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(test::id<schema::TableId>(100U), test::id<schema::SchemaId>(101U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(definitions), std::move(roles))
          .value());
  const OwnedColumnarBatch batch = OwnedColumnarBatch::create(schema, std::move(vectors)).value();
  const EncodedColumnarBatch encoded = encode_columnar_batch_v1(batch).value();
  const auto decoded = decode_columnar_batch_v1_exact(encoded.bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  ASSERT_EQ(decoded->columns().size(), 18U);
  for (std::uint16_t code = 1U; code <= 18U; ++code) {
    EXPECT_EQ(decoded->columns()[code - 1U].type().code(), code);
  }
  EXPECT_TRUE(validate_columnar_batch_schema(*decoded, *schema).is_ok());
}

} // namespace
} // namespace chronos::columnar
