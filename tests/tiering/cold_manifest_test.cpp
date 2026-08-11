#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/tiering/cold_manifest.hpp"
#include "chronos/tiering/cold_manifest_format.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <optional>
#include <ranges>
#include <vector>

namespace chronos::tiering {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] ingest::Sha256Digest digest(const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return ingest::Sha256Digest{bytes};
}

struct Fixture {
  manifest::DatabaseId database_id{id<manifest::DatabaseId>(1U)};
  common::Uuid object_store_id{uuid(2U)};
  schema::TableId table_id{id<schema::TableId>(3U)};
  schema::TabletId tablet_id{id<schema::TabletId>(4U)};
  schema::SchemaId schema_id{id<schema::SchemaId>(5U)};
  common::Uuid source_id{uuid(6U)};
  std::vector<manifest::TemporalTabletDescriptor> tablets{{
      .table_id = table_id,
      .tablet_id = tablet_id,
      .recovery_schema_id = schema_id,
      .recovery_schema_version = schema::SchemaVersion::initial(),
      .source_id = source_id,
      .durable_position = 2U,
      .reclaim_position = 1U,
      .first_part_index = 0U,
      .part_count = 2U,
      .durable_version_count = 2U,
      .commit_source = manifest::ManifestCommitSource::kRaft,
  }};
  std::vector<manifest::TemporalPartDescriptor> parts{
      {.part_id = id<cseg::PartId>(10U),
       .table_id = table_id,
       .tablet_id = tablet_id,
       .schema_id = schema_id,
       .schema_version = schema::SchemaVersion::initial(),
       .file_length = 4'096U,
       .row_count = 1U,
       .minimum_commit_position = 1U,
       .maximum_commit_position = 1U,
       .minimum_event_time = 10,
       .maximum_event_time = 10,
       .minimum_system_time = 100,
       .maximum_system_time = 100,
       .source_id = source_id,
       .content_sha256 = digest(20U),
       .commit_source = manifest::ManifestCommitSource::kRaft},
      {.part_id = id<cseg::PartId>(11U),
       .table_id = table_id,
       .tablet_id = tablet_id,
       .schema_id = schema_id,
       .schema_version = schema::SchemaVersion::initial(),
       .file_length = 8'192U,
       .row_count = 1U,
       .minimum_commit_position = 2U,
       .maximum_commit_position = 2U,
       .minimum_event_time = 20,
       .maximum_event_time = 20,
       .minimum_system_time = 200,
       .maximum_system_time = 200,
       .source_id = source_id,
       .content_sha256 = digest(21U),
       .commit_source = manifest::ManifestCommitSource::kRaft}};
  std::vector<ColdPartLocationDescriptor> locations{
      {parts[0].part_id, parts[0].file_length, parts[0].content_sha256, "cold/a"},
      {parts[1].part_id, parts[1].file_length, parts[1].content_sha256, "cold/bb"}};

  [[nodiscard]] ColdLocationManifestEncodeInput cold_input() const {
    return {.generation = 3U,
            .base_manifest_generation = 5U,
            .database_id = database_id,
            .object_store_id = object_store_id,
            .locations = locations};
  }

  [[nodiscard]] manifest::TemporalManifestEncodeInput base_input() const {
    return {.generation = 5U,
            .database_id = database_id,
            .wal_reclaim_checkpoint = std::nullopt,
            .tablets = tablets,
            .parts = parts,
            .retries = {}};
  }
};

void store_u16(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint16_t value) {
  bytes[offset] = std::byte{static_cast<std::uint8_t>(value)};
  bytes[offset + 1U] = std::byte{static_cast<std::uint8_t>(value >> 8U)};
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
}

void refresh_header_and_file_crc(std::vector<std::byte>& bytes) {
  store_u32(
      bytes, cold_manifest_format::kHeaderCrc32cOffset,
      common::crc32c(common::ByteView{bytes}.first(cold_manifest_format::kHeaderCrc32cOffset)));
  const std::size_t file_crc_offset = bytes.size() - cold_manifest_format::kFileCrc32cLength;
  store_u32(bytes, file_crc_offset, common::crc32c(common::ByteView{bytes}.first(file_crc_offset)));
}

TEST(ColdLocationManifestTest, RoundTripsCanonicalAuthorityAndBindsManifestV2) {
  Fixture fixture;
  auto first = encode_cold_location_manifest_v1(fixture.cold_input());
  auto second = encode_cold_location_manifest_v1(fixture.cold_input());
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_EQ(first->size(), 472U);
  EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes()));
  EXPECT_TRUE(std::ranges::equal(first->bytes().first(cold_manifest_format::kMagic.size()),
                                 cold_manifest_format::kMagic));

  auto decoded = decode_cold_location_manifest_v1_exact(first->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  EXPECT_EQ(decoded->generation(), 3U);
  EXPECT_EQ(decoded->previous_generation(), 2U);
  EXPECT_EQ(decoded->base_manifest_generation(), 5U);
  EXPECT_EQ(decoded->database_id(), fixture.database_id);
  EXPECT_EQ(decoded->object_store_id(), fixture.object_store_id);
  EXPECT_TRUE(std::ranges::equal(decoded->locations(), fixture.locations));

  auto base_encoded = manifest::encode_manifest_v2_temporal(fixture.base_input());
  ASSERT_TRUE(base_encoded.has_value()) << base_encoded.error().to_string();
  auto base = manifest::decode_manifest_v2_temporal_exact(base_encoded->bytes());
  ASSERT_TRUE(base.has_value()) << base.error().status().to_string();
  EXPECT_TRUE(validate_cold_location_manifest_binding(*decoded, *base).is_ok());
}

TEST(ColdLocationManifestTest, ClassifiesTruncationDamageUnknownVersionAndLimits) {
  Fixture fixture;
  auto encoded = encode_cold_location_manifest_v1(fixture.cold_input());
  ASSERT_TRUE(encoded.has_value());
  for (std::size_t size = 0U; size < encoded->size(); ++size) {
    auto decoded = decode_cold_location_manifest_v1_prefix(encoded->bytes().first(size));
    ASSERT_FALSE(decoded.has_value()) << size;
    EXPECT_EQ(decoded.error().kind(), ColdLocationManifestDecodeErrorKind::kIncomplete) << size;
    EXPECT_GT(decoded.error().required_size(), size) << size;
  }

  std::vector<std::byte> trailing(encoded->bytes().begin(), encoded->bytes().end());
  trailing.push_back(std::byte{0U});
  EXPECT_TRUE(decode_cold_location_manifest_v1_prefix(trailing).has_value());
  EXPECT_FALSE(decode_cold_location_manifest_v1_exact(trailing).has_value());

  auto limited = decode_cold_location_manifest_v1_exact(
      encoded->bytes(), {.maximum_file_length = encoded->size() - 1U});
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().kind(), ColdLocationManifestDecodeErrorKind::kResourceLimit);

  std::vector<std::byte> unknown(encoded->bytes().begin(), encoded->bytes().end());
  store_u16(unknown, cold_manifest_format::kFormatMajorOffset, 2U);
  refresh_header_and_file_crc(unknown);
  auto unknown_result = decode_cold_location_manifest_v1_exact(unknown);
  ASSERT_FALSE(unknown_result.has_value());
  EXPECT_EQ(unknown_result.error().kind(), ColdLocationManifestDecodeErrorKind::kUnsupported);

  std::vector<std::byte> damaged(encoded->bytes().begin(), encoded->bytes().end());
  const std::size_t descriptor_offset = cold_manifest_format::kHeaderLength;
  damaged[descriptor_offset + cold_manifest_format::kDescriptorFileLengthOffset] ^= std::byte{1U};
  refresh_header_and_file_crc(damaged);
  auto damaged_result = decode_cold_location_manifest_v1_exact(damaged);
  ASSERT_FALSE(damaged_result.has_value());
  EXPECT_EQ(damaged_result.error().kind(), ColdLocationManifestDecodeErrorKind::kCorruption);
}

TEST(ColdLocationManifestTest, RejectsNoncanonicalLocationsAndMismatchedBaseAuthority) {
  Fixture fixture;
  std::ranges::swap(fixture.locations[0], fixture.locations[1]);
  EXPECT_FALSE(encode_cold_location_manifest_v1(fixture.cold_input()).has_value());
  std::ranges::swap(fixture.locations[0], fixture.locations[1]);
  fixture.locations[1].object_key = fixture.locations[0].object_key;
  EXPECT_FALSE(encode_cold_location_manifest_v1(fixture.cold_input()).has_value());

  fixture = Fixture{};
  auto cold_encoded = encode_cold_location_manifest_v1(fixture.cold_input());
  ASSERT_TRUE(cold_encoded.has_value());
  auto cold = decode_cold_location_manifest_v1_exact(cold_encoded->bytes());
  ASSERT_TRUE(cold.has_value());
  fixture.parts[1].content_sha256 = digest(99U);
  auto base_encoded = manifest::encode_manifest_v2_temporal(fixture.base_input());
  ASSERT_TRUE(base_encoded.has_value());
  auto base = manifest::decode_manifest_v2_temporal_exact(base_encoded->bytes());
  ASSERT_TRUE(base.has_value());
  const common::Status mismatch = validate_cold_location_manifest_binding(*cold, *base);
  EXPECT_EQ(mismatch.code(), common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::tiering
