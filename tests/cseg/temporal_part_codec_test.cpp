#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/cseg/projected_reader.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/cseg/validator.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <type_traits>
#include <vector>

namespace chronos::cseg {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

[[nodiscard]] CsegColumnDescriptor user_column() {
  return {.column_id = id<schema::ColumnId>(5U),
          .storage_kind = StorageKind::kUser,
          .logical_type = type(schema::LogicalTypeKind::kTimestampNs),
          .nullable = false,
          .event_time = true,
          .schema_ordinal = 0U,
          .ordering_ordinal = 0U};
}

[[nodiscard]] CsegColumnDescriptor system_column(const StorageKind kind,
                                                 const schema::LogicalTypeKind logical_type) {
  return {.column_id = std::nullopt,
          .storage_kind = kind,
          .logical_type = type(logical_type),
          .nullable = false,
          .event_time = false,
          .schema_ordinal = std::nullopt,
          .ordering_ordinal = std::nullopt};
}

[[nodiscard]] EncodedCsegPage encode_page(const schema::LogicalType logical_type,
                                          const std::uint32_t rows, const common::ByteView offsets,
                                          const common::ByteView values) {
  const auto physical = columnar::PhysicalColumnView::create(
      {.type = logical_type, .nullable = false, .row_count = rows, .null_count = 0U},
      {.validity = {}, .offsets = offsets, .values = values});
  return encode_cseg_v1_page(*physical, PageCompression::kNone).value();
}

struct TemporalFixtureValues {
  std::int64_t first_event_time{10};
  std::int64_t second_event_time{20};
  std::uint8_t commit_source{static_cast<std::uint8_t>(temporal_format::CommitSource::kWal)};
  std::uint64_t commit_position{7U};
  std::uint8_t operation{static_cast<std::uint8_t>(temporal_format::Operation::kOriginal)};
  bool empty_first_identity{};
};

struct TemporalPartFixture {
  PartId part_id{id<PartId>(1U)};
  schema::TableId table_id{id<schema::TableId>(2U)};
  schema::TabletId tablet_id{id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(4U)};
  std::vector<CsegColumnDescriptor> columns{
      user_column(),
      system_column(StorageKind::kCommitSource, schema::LogicalTypeKind::kUInt8),
      system_column(StorageKind::kSourceId, schema::LogicalTypeKind::kUuid),
      system_column(StorageKind::kCommitPosition, schema::LogicalTypeKind::kUInt64),
      system_column(StorageKind::kTemporalRowOrdinal, schema::LogicalTypeKind::kUInt32),
      system_column(StorageKind::kTemporalOperation, schema::LogicalTypeKind::kUInt8),
      system_column(StorageKind::kLogicalIdentity, schema::LogicalTypeKind::kBinary),
      system_column(StorageKind::kReceiveTime, schema::LogicalTypeKind::kTimestampNs),
      system_column(StorageKind::kSystemCommitTime, schema::LogicalTypeKind::kTimestampNs),
  };
  std::vector<CsegGranuleDescriptor> granules{{.first_row = 0U,
                                               .row_count = 2U,
                                               .first_page_index = 0U,
                                               .minimum_event_time = 10,
                                               .maximum_event_time = 20}};
  std::vector<EncodedCsegPage> pages;

  explicit TemporalPartFixture(const TemporalFixtureValues values = {}) {
    std::vector<std::byte> event_time;
    append_le(event_time, values.first_event_time);
    append_le(event_time, values.second_event_time);
    const std::vector<std::byte> commit_source{std::byte{values.commit_source},
                                               std::byte{values.commit_source}};
    std::vector<std::byte> source_id;
    const common::Uuid::Bytes source = id<schema::SchemaId>(8U).bytes();
    source_id.insert(source_id.end(), source.begin(), source.end());
    source_id.insert(source_id.end(), source.begin(), source.end());
    std::vector<std::byte> positions;
    append_le(positions, values.commit_position);
    append_le(positions, values.commit_position);
    std::vector<std::byte> ordinals;
    append_le(ordinals, std::uint32_t{0U});
    append_le(ordinals, std::uint32_t{1U});
    const std::vector<std::byte> operations{
        std::byte{values.operation},
        std::byte{static_cast<std::uint8_t>(temporal_format::Operation::kCorrection)}};
    std::vector<std::byte> identity_offsets;
    append_le(identity_offsets, std::uint32_t{0U});
    append_le(identity_offsets,
              values.empty_first_identity ? std::uint32_t{0U} : std::uint32_t{1U});
    append_le(identity_offsets,
              values.empty_first_identity ? std::uint32_t{1U} : std::uint32_t{2U});
    const std::vector<std::byte> identities =
        values.empty_first_identity ? std::vector<std::byte>{std::byte{'b'}}
                                    : std::vector<std::byte>{std::byte{'a'}, std::byte{'b'}};
    std::vector<std::byte> receive_times;
    append_le(receive_times, std::int64_t{100});
    append_le(receive_times, std::int64_t{101});
    std::vector<std::byte> commit_times;
    append_le(commit_times, std::int64_t{200});
    append_le(commit_times, std::int64_t{201});

    pages.reserve(columns.size());
    pages.push_back(encode_page(columns[0].logical_type, 2U, {}, event_time));
    pages.push_back(encode_page(columns[1].logical_type, 2U, {}, commit_source));
    pages.push_back(encode_page(columns[2].logical_type, 2U, {}, source_id));
    pages.push_back(encode_page(columns[3].logical_type, 2U, {}, positions));
    pages.push_back(encode_page(columns[4].logical_type, 2U, {}, ordinals));
    pages.push_back(encode_page(columns[5].logical_type, 2U, {}, operations));
    pages.push_back(encode_page(columns[6].logical_type, 2U, identity_offsets, identities));
    pages.push_back(encode_page(columns[7].logical_type, 2U, {}, receive_times));
    pages.push_back(encode_page(columns[8].logical_type, 2U, {}, commit_times));
  }

  [[nodiscard]] CsegPartEncodeInput input() const {
    return {.part_id = part_id,
            .table_id = table_id,
            .tablet_id = tablet_id,
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .row_count = 2U,
            .event_time_column_ordinal = 0U,
            .ordering_column_count = 1U,
            .minimum_event_time = 10,
            .maximum_event_time = 20,
            .columns = columns,
            .granules = granules,
            .pages = pages};
  }

  [[nodiscard]] std::shared_ptr<const schema::TableSchema> schema_value() const {
    std::vector<schema::ColumnDefinition> definitions;
    definitions.push_back(
        schema::ColumnDefinition::create(id<schema::ColumnId>(5U), "event_time",
                                         type(schema::LogicalTypeKind::kTimestampNs), false)
            .value());
    return std::make_shared<const schema::TableSchema>(
        schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                    std::nullopt, std::move(definitions),
                                    {.event_time_column = id<schema::ColumnId>(5U),
                                     .physical_ordering_key = {id<schema::ColumnId>(5U)},
                                     .partition_columns = {id<schema::ColumnId>(5U)},
                                     .shard_key = {id<schema::ColumnId>(5U)},
                                     .deduplication_key = {id<schema::ColumnId>(5U)}})
            .value());
  }
};

[[nodiscard]] common::Status validate_fixture(const TemporalFixtureValues values = {}) {
  TemporalPartFixture fixture{values};
  const auto encoded = encode_cseg_v2_temporal_part(fixture.input());
  if (!encoded.has_value()) {
    return encoded.error();
  }
  const auto decoded = decode_cseg_v2_temporal_part_exact(encoded->bytes());
  return decoded.has_value() ? validate_cseg_v2_temporal_part_contents(*decoded)
                             : decoded.error().status();
}

[[nodiscard]] std::uint32_t independent_crc32c(const common::ByteView bytes) {
  std::uint32_t crc = 0xffffffffU;
  for (const std::byte byte : bytes) {
    crc ^= std::to_integer<std::uint8_t>(byte);
    for (std::size_t bit = 0U; bit < 8U; ++bit) {
      const std::uint32_t mask = 0U - (crc & 1U);
      crc = (crc >> 1U) ^ (0x82f63b78U & mask);
    }
  }
  return ~crc;
}

TEST(TemporalPartCodecTest, EmitsGoldenCanonicalV2PartWithoutWeakeningV1) {
  TemporalPartFixture fixture;
  const auto first = encode_cseg_v2_temporal_part(fixture.input());
  const auto second = encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  ASSERT_EQ(first->size(), 2'048U);
  constexpr std::array<std::byte, 16U> kGoldenPrefix{
      std::byte{0x43U}, std::byte{0x48U}, std::byte{0x52U}, std::byte{0x4eU},
      std::byte{0x43U}, std::byte{0x53U}, std::byte{0x45U}, std::byte{0x47U},
      std::byte{0x02U}, std::byte{0x00U}, std::byte{0x00U}, std::byte{0x00U},
      std::byte{0x00U}, std::byte{0x01U}, std::byte{0x00U}, std::byte{0x00U}};
  EXPECT_TRUE(std::ranges::equal(first->bytes().first(kGoldenPrefix.size()), kGoldenPrefix));
  // This tableless reflected-Castagnoli oracle is independent of the production CRC routine and
  // fingerprints the complete temporal metadata, page order, stored bytes, and zero padding.
  EXPECT_EQ(independent_crc32c(first->bytes()), 0x3242794cU);
  EXPECT_EQ(common::crc32c(first->bytes()), 0x3242794cU);
  EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes()));

  const auto decoded = decode_cseg_v2_temporal_part_exact(first->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  EXPECT_EQ(decoded->metadata().format_major(), temporal_format::kFormatMajor);
  ASSERT_EQ(decoded->metadata().pages().size(), fixture.pages.size());
  for (std::size_t index = 0U; index < fixture.pages.size(); ++index) {
    const auto page = decoded->decode_page(index);
    ASSERT_TRUE(page.has_value()) << index;
    EXPECT_EQ(page->physical().row_count(), 2U);
  }

  const auto v1 = decode_cseg_v1_part_exact(first->bytes());
  ASSERT_FALSE(v1.has_value());
  EXPECT_EQ(v1.error().kind(), CsegPartDecodeErrorKind::kUnsupported);
  EXPECT_FALSE(encode_cseg_v1_part(fixture.input()).has_value());
}

TEST(TemporalPartCodecTest, AdoptsOnlyAnExactCanonicalV2Image) {
  TemporalPartFixture fixture;
  auto encoded = encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  auto adopted = adopt_cseg_v2_temporal_part(encoded->bytes());
  ASSERT_TRUE(adopted.has_value()) << adopted.error().to_string();
  EXPECT_TRUE(std::ranges::equal(adopted->bytes(), encoded->bytes()));

  std::vector<std::byte> damaged(encoded->bytes().begin(), encoded->bytes().end());
  damaged.back() ^= std::byte{0x80U};
  EXPECT_EQ(adopt_cseg_v2_temporal_part(damaged).error().code(), common::StatusCode::kCorruption);
  damaged.assign(encoded->bytes().begin(), encoded->bytes().end());
  damaged.push_back(std::byte{0U});
  EXPECT_EQ(adopt_cseg_v2_temporal_part(damaged).error().code(), common::StatusCode::kCorruption);
}

TEST(TemporalPartCodecTest, FailsClosedOnTruncationStoredCorruptionAndSuffix) {
  TemporalPartFixture fixture;
  const auto encoded = encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  for (std::size_t size = 0U; size < encoded->size(); ++size) {
    const auto decoded = decode_cseg_v2_temporal_part_prefix(encoded->bytes().first(size));
    ASSERT_FALSE(decoded.has_value()) << size;
    EXPECT_EQ(decoded.error().kind(), CsegPartDecodeErrorKind::kIncomplete) << size;
  }

  const auto canonical = decode_cseg_v2_temporal_part_exact(encoded->bytes());
  ASSERT_TRUE(canonical.has_value());
  std::vector<std::byte> corrupt(encoded->bytes().begin(), encoded->bytes().end());
  const CsegPageDescriptor& identity = canonical->metadata().pages()[6U];
  corrupt[static_cast<std::size_t>(identity.page_offset)] ^= std::byte{1U};
  const auto rejected = decode_cseg_v2_temporal_part_exact(corrupt);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().kind(), CsegPartDecodeErrorKind::kCorruption);

  std::vector<std::byte> suffixed(encoded->bytes().begin(), encoded->bytes().end());
  suffixed.push_back(std::byte{0U});
  EXPECT_TRUE(decode_cseg_v2_temporal_part_prefix(suffixed).has_value());
  EXPECT_FALSE(decode_cseg_v2_temporal_part_exact(suffixed).has_value());
}

TEST(TemporalPartValidatorTest, AcceptsExactTemporalSemanticsAndSchemaBinding) {
  TemporalPartFixture fixture;
  const auto encoded = encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const auto decoded = decode_cseg_v2_temporal_part_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(validate_cseg_v2_temporal_part_contents(*decoded).is_ok());
  EXPECT_TRUE(
      validate_cseg_v2_temporal_part(*decoded, *fixture.schema_value(), fixture.tablet_id).is_ok());
  EXPECT_EQ(validate_cseg_v1_part_contents(*decoded).code(), common::StatusCode::kInvalidArgument);
}

TEST(TemporalPartValidatorTest, RejectsInvalidAndUnsupportedTemporalValues) {
  TemporalFixtureValues values;
  values.commit_source = 0U;
  const common::Status zero_source = validate_fixture(values);
  EXPECT_EQ(zero_source.code(), common::StatusCode::kCorruption);
  EXPECT_EQ(zero_source.message(), "CSEG v2 temporal COMMIT_SOURCE is zero or malformed");
  values.commit_source = 3U;
  const common::Status unknown_source = validate_fixture(values);
  EXPECT_EQ(unknown_source.code(), common::StatusCode::kNotSupported);
  EXPECT_EQ(unknown_source.message(), "CSEG v2 temporal COMMIT_SOURCE is unsupported");

  values = {};
  values.commit_position = 0U;
  EXPECT_EQ(validate_fixture(values).code(), common::StatusCode::kCorruption);
  values = {};
  values.operation = 0U;
  EXPECT_EQ(validate_fixture(values).code(), common::StatusCode::kCorruption);
  values.operation = 5U;
  EXPECT_EQ(validate_fixture(values).code(), common::StatusCode::kNotSupported);
  values = {};
  values.empty_first_identity = true;
  EXPECT_EQ(validate_fixture(values).code(), common::StatusCode::kCorruption);
}

TEST(TemporalPartValidatorTest, RejectsEventExtremaOrderingAndWorkingLimitViolations) {
  TemporalFixtureValues values;
  values.first_event_time = 20;
  values.second_event_time = 10;
  EXPECT_EQ(validate_fixture(values).code(), common::StatusCode::kCorruption);
  values = {};
  values.first_event_time = 11;
  EXPECT_EQ(validate_fixture(values).code(), common::StatusCode::kCorruption);

  TemporalPartFixture fixture;
  const auto encoded = encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const auto decoded = decode_cseg_v2_temporal_part_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(validate_cseg_v2_temporal_part_contents(*decoded, {.max_working_bytes = 1U}).code(),
            common::StatusCode::kResourceExhausted);
}

TEST(TemporalProjectedReaderTest, SelectivelyReadsUserAndAllTemporalSystemPages) {
  TemporalPartFixture fixture;
  const auto encoded = encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const schema::SchemaLineage lineage =
      schema::SchemaLineage::create(*fixture.schema_value()).value();
  const auto reader = open_cseg_v2_temporal_projected_reader_exact(
      encoded->bytes(), lineage, fixture.schema_id, fixture.tablet_id);
  ASSERT_TRUE(reader.has_value()) << reader.error().status().to_string();

  const std::array<std::uint32_t, 1U> projection{0U};
  const auto plan = reader->plan_granule(0U, projection);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(plan->decoded_page_count(), 1U + temporal_format::kSystemColumnCount);
  const auto granule = reader->read_granule(*plan);
  ASSERT_TRUE(granule.has_value()) << granule.error().to_string();
  ASSERT_EQ(granule->columns().size(), 1U);
  EXPECT_EQ(granule->commit_source().row_count(), 2U);
  EXPECT_EQ(granule->logical_identity().cell(0U)->bytes()->size(), 1U);
  EXPECT_EQ(granule->system_commit_time().row_count(), 2U);

  const auto v1 = open_cseg_v1_projected_reader_exact(encoded->bytes(), lineage, fixture.schema_id,
                                                      fixture.tablet_id);
  ASSERT_FALSE(v1.has_value());
  EXPECT_EQ(v1.error().kind(), CsegProjectedReaderOpenErrorKind::kUnsupported);
}

TEST(TemporalProjectedReaderTest, EmptyProjectionStillRejectsInvalidTemporalSystemRows) {
  TemporalFixtureValues values;
  values.commit_source = 0U;
  TemporalPartFixture fixture{values};
  const auto encoded = encode_cseg_v2_temporal_part(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const schema::SchemaLineage lineage =
      schema::SchemaLineage::create(*fixture.schema_value()).value();
  const auto reader = open_cseg_v2_temporal_projected_reader_exact(
      encoded->bytes(), lineage, fixture.schema_id, fixture.tablet_id);
  ASSERT_TRUE(reader.has_value());
  const std::span<const std::uint32_t> empty;
  const auto granule = reader->read_granule(0U, empty);
  ASSERT_FALSE(granule.has_value());
  EXPECT_EQ(granule.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(granule.error().message(), "CSEG v2 temporal COMMIT_SOURCE is zero or malformed");
}

} // namespace
} // namespace chronos::cseg
