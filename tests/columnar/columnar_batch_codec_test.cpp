#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/crc32c.hpp"
#include "columnar/columnar_test_support.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::columnar {
namespace {

// Independently generated from docs/formats/columnar-batch-v1.md with a small Python struct/CRC32C
// script, not from the production encoder. The fixture is deliberately literal and reviewable.
constexpr std::array<std::uint8_t, 400> kGoldenBatch{
    0x43, 0x48, 0x52, 0x4e, 0x43, 0x42, 0x31, 0x00, 0x01, 0x00, 0x00, 0x00, 0x60, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x50, 0x00, 0x00, 0x00,
    0x90, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xbb, 0xcb, 0x44, 0xc6, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
    0x0d, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x50, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
    0x10, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x60, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x68, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x78, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,
    0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x17, 0xc8, 0xcf,
};

template <std::size_t Size>
[[nodiscard]] std::array<std::byte, Size> as_bytes(const std::array<std::uint8_t, Size>& input) {
  std::array<std::byte, Size> output{};
  std::transform(input.begin(), input.end(), output.begin(),
                 [](const std::uint8_t value) { return static_cast<std::byte>(value); });
  return output;
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> single_timestamp_schema() {
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(test::id<schema::ColumnId>(1U), "ts",
                                       test::type(schema::LogicalTypeKind::kTimestampNs), false)
          .value());
  const schema::ColumnId event_time = test::id<schema::ColumnId>(1U);
  schema::TableSchemaRoles roles{.event_time_column = event_time,
                                 .physical_ordering_key = {event_time},
                                 .partition_columns = {event_time},
                                 .shard_key = {event_time},
                                 .deduplication_key = {}};
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(test::id<schema::TableId>(70U), test::id<schema::SchemaId>(71U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns), std::move(roles))
          .value());
}

TEST(ColumnarBatchLayoutTest, PlansTheCanonicalDescriptorAndBufferOrder) {
  const OwnedColumnarBatch batch =
      OwnedColumnarBatch::create(test::batch_schema(), test::batch_columns()).value();
  const auto layout = plan_columnar_batch_v1_layout(batch);
  ASSERT_TRUE(layout.has_value()) << layout.error().to_string();
  ASSERT_EQ(layout->columns().size(), 3U);
  EXPECT_EQ(layout->total_length(), 400U);
  EXPECT_EQ(layout->columns()[0].values, (BufferLayout{.offset = 336U, .length = 16U}));
  EXPECT_EQ(layout->columns()[1].validity, (BufferLayout{.offset = 352U, .length = 1U}));
  EXPECT_EQ(layout->columns()[1].offsets, (BufferLayout{.offset = 360U, .length = 12U}));
  EXPECT_EQ(layout->columns()[1].values, (BufferLayout{.offset = 376U, .length = 1U}));
  EXPECT_EQ(layout->columns()[2].values, (BufferLayout{.offset = 384U, .length = 1U}));
}

TEST(ColumnarBatchCodecTest, MatchesIndependentGoldenBytesAndDecodesBorrowedViews) {
  const auto schema = test::batch_schema();
  const OwnedColumnarBatch batch =
      OwnedColumnarBatch::create(schema, test::batch_columns()).value();
  const auto encoded = encode_columnar_batch_v1(batch);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  EXPECT_EQ(encoded->size(), kGoldenBatch.size());
  const auto golden = as_bytes(kGoldenBatch);
  EXPECT_TRUE(std::ranges::equal(encoded->bytes(), golden));

  const auto decoded = decode_columnar_batch_v1_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  EXPECT_EQ(decoded->table_id(), schema->table_id());
  EXPECT_EQ(decoded->schema_id(), schema->schema_id());
  EXPECT_EQ(decoded->schema_version(), schema->version());
  EXPECT_EQ(decoded->row_count(), 2U);
  EXPECT_EQ(decoded->columns().size(), 3U);
  EXPECT_EQ(decoded->encoded_bytes().data(), encoded->bytes().data());
  EXPECT_EQ(decoded->columns()[1].values().data(), encoded->bytes().data() + 376U);
  EXPECT_TRUE(decoded->columns()[1].cell(1U)->is_null());
  EXPECT_TRUE(validate_columnar_batch_schema(*decoded, *schema).is_ok());
}

TEST(ColumnarBatchCodecTest, PrefixAndExactDecodingDistinguishTruncationAndTrailingBytes) {
  const auto golden = as_bytes(kGoldenBatch);
  for (std::size_t size = 0U; size < golden.size(); ++size) {
    SCOPED_TRACE(size);
    const auto decoded = decode_columnar_batch_v1_prefix(common::ByteView{golden}.first(size));
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), ColumnarBatchDecodeErrorKind::kIncomplete);
    EXPECT_EQ(decoded.error().required_size(), size < format::kBatchHeaderLength ? 96U : 400U);
  }

  std::array<std::byte, 403> with_suffix{};
  std::copy(golden.begin(), golden.end(), with_suffix.begin() + 1);
  const common::ByteView unaligned = common::ByteView{with_suffix}.subspan(1U, 402U);
  const auto prefix = decode_columnar_batch_v1_prefix(unaligned);
  ASSERT_TRUE(prefix.has_value()) << prefix.error().status().to_string();
  EXPECT_EQ(prefix->encoded_bytes().size(), 400U);
  const auto exact = decode_columnar_batch_v1_exact(unaligned);
  ASSERT_FALSE(exact.has_value());
  EXPECT_EQ(exact.error().kind(), ColumnarBatchDecodeErrorKind::kInvalid);
}

TEST(ColumnarBatchCodecTest, EnforcesConfiguredLimitsBeforeDescriptorAllocation) {
  const auto golden = as_bytes(kGoldenBatch);
  for (const ColumnarBatchDecodeLimits limits : {
           ColumnarBatchDecodeLimits{.max_batch_length = 399U, .max_rows = 2U, .max_columns = 3U},
           ColumnarBatchDecodeLimits{.max_batch_length = 400U, .max_rows = 1U, .max_columns = 3U},
           ColumnarBatchDecodeLimits{.max_batch_length = 400U, .max_rows = 2U, .max_columns = 2U},
       }) {
    const auto decoded = decode_columnar_batch_v1_exact(golden, limits);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().kind(), ColumnarBatchDecodeErrorKind::kResourceLimit);
  }
}

TEST(ColumnarBatchCodecTest, EncodesTheExactMaximumAndRejectsTheNextAlignedLayout) {
  constexpr std::size_t kOneColumnOverhead =
      format::kBatchHeaderLength + format::kColumnDescriptorLength +
      format::kTerminalPaddingLength + format::kBatchTrailerLength;
  constexpr std::size_t kMaximumValues = format::kMaximumEmbeddedBatchLength - kOneColumnOverhead;
  static_assert((kMaximumValues % sizeof(std::uint64_t)) == 0U);
  constexpr std::uint32_t kMaximumRows =
      static_cast<std::uint32_t>(kMaximumValues / sizeof(std::uint64_t));

  const auto schema = single_timestamp_schema();
  std::vector<OwnedColumnVector> columns;
  columns.push_back(test::fixed_vector(1U, test::type(schema::LogicalTypeKind::kTimestampNs), false,
                                       kMaximumRows, {}, 0U,
                                       std::vector<std::byte>(kMaximumValues, std::byte{0U})));
  const OwnedColumnarBatch maximum = OwnedColumnarBatch::create(schema, std::move(columns)).value();
  const EncodedColumnarBatch encoded = encode_columnar_batch_v1(maximum).value();
  EXPECT_EQ(encoded.size(), format::kMaximumEmbeddedBatchLength);
  ASSERT_TRUE(decode_columnar_batch_v1_exact(encoded.bytes()).has_value());

  std::vector<OwnedColumnVector> too_large_columns;
  too_large_columns.push_back(test::fixed_vector(
      1U, test::type(schema::LogicalTypeKind::kTimestampNs), false, kMaximumRows + 1U, {}, 0U,
      std::vector<std::byte>(kMaximumValues + sizeof(std::uint64_t), std::byte{0U})));
  const OwnedColumnarBatch too_large =
      OwnedColumnarBatch::create(schema, std::move(too_large_columns)).value();
  const auto layout = plan_columnar_batch_v1_layout(too_large);
  ASSERT_FALSE(layout.has_value());
  EXPECT_EQ(layout.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(ColumnarBatchCodecTest, SchemaBindingIsAnExactSeparateValidationStage) {
  const auto golden = as_bytes(kGoldenBatch);
  const auto decoded = decode_columnar_batch_v1_exact(golden);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(validate_columnar_batch_schema(*decoded, *test::batch_schema()).is_ok());

  std::shared_ptr<const schema::TableSchema> different = test::batch_schema();
  std::vector<schema::ColumnDefinition> columns{different->columns().begin(),
                                                different->columns().end()};
  schema::TableSchemaRoles roles{
      .event_time_column = test::id<schema::ColumnId>(1),
      .physical_ordering_key = {test::id<schema::ColumnId>(1)},
      .partition_columns = {test::id<schema::ColumnId>(1)},
      .shard_key = {test::id<schema::ColumnId>(1)},
      .deduplication_key = {},
  };
  const schema::TableSchema mismatched =
      schema::TableSchema::create(test::id<schema::TableId>(60), different->schema_id(),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns), std::move(roles))
          .value();
  EXPECT_EQ(validate_columnar_batch_schema(*decoded, mismatched).code(),
            common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::columnar
