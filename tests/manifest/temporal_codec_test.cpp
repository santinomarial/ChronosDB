#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/format.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_format.hpp"
#include "manifest/manifest_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::manifest {
namespace {

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

[[nodiscard]] wal::WalId wal_id(const std::uint8_t seed) {
  wal::WalId value{};
  value.bytes.front() = std::byte{seed};
  return value;
}

struct Fixture {
  DatabaseId database_id{test::make_id<DatabaseId>(1U)};
  wal::WalId wal{wal_id(2U)};
  schema::TableId table_id{test::make_id<schema::TableId>(3U)};
  schema::TabletId tablet_id{test::make_id<schema::TabletId>(4U)};
  schema::SchemaId schema_id{test::make_id<schema::SchemaId>(5U)};
  std::vector<TemporalTabletDescriptor> tablets{{
      .table_id = table_id,
      .tablet_id = tablet_id,
      .recovery_schema_id = schema_id,
      .recovery_schema_version = schema::SchemaVersion::initial(),
      .source_id = common::Uuid{wal.bytes},
      .durable_position = 5U,
      .reclaim_position = 0U,
      .first_part_index = 0U,
      .part_count = 1U,
      .durable_version_count = 2U,
      .commit_source = ManifestCommitSource::kWal,
  }};
  std::vector<TemporalPartDescriptor> parts{{
      .part_id = test::make_id<cseg::PartId>(6U),
      .table_id = table_id,
      .tablet_id = tablet_id,
      .schema_id = schema_id,
      .schema_version = schema::SchemaVersion::initial(),
      .file_length = 4'096U,
      .row_count = 2U,
      .minimum_commit_position = 4U,
      .maximum_commit_position = 5U,
      .minimum_event_time = 10,
      .maximum_event_time = 20,
      .minimum_system_time = 100,
      .maximum_system_time = 110,
      .source_id = common::Uuid{wal.bytes},
      .content_sha256 = digest(7U),
      .commit_source = ManifestCommitSource::kWal,
  }};
  std::vector<TemporalRetryDescriptor> retries{{
      .client_id = test::make_id<ingest::ClientId>(8U),
      .client_batch_id = test::make_id<ingest::ClientBatchId>(9U),
      .table_id = table_id,
      .tablet_id = tablet_id,
      .request_digest = digest(10U),
      .source_id = common::Uuid{wal.bytes},
      .commit_position = 5U,
      .applied_row_count = 2U,
      .commit_source = ManifestCommitSource::kWal,
  }};

  [[nodiscard]] TemporalManifestEncodeInput input() const {
    return {.generation = 2U,
            .database_id = database_id,
            .wal_reclaim_checkpoint =
                TemporalWalReclaimCheckpoint{
                    wal, {.record_sequence = 5U, .segment_number = 1U, .byte_offset = 128U}},
            .tablets = tablets,
            .parts = parts,
            .retries = retries};
  }
};

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = std::byte{static_cast<std::uint8_t>(value >> (index * 8U))};
  }
}

void refresh_crcs(std::vector<std::byte>& bytes) {
  store_u32(bytes, format::kHeaderCrc32cOffset,
            common::crc32c(common::ByteView{bytes}.first(format::kHeaderCrc32cOffset)));
  const std::size_t file_crc = bytes.size() - format::kFileCrc32cLength;
  store_u32(bytes, file_crc, common::crc32c(common::ByteView{bytes}.first(file_crc)));
}

TEST(TemporalManifestCodecTest, RoundTripsCanonicalSourceBoundGenerationDeterministically) {
  Fixture fixture;
  const auto first = encode_manifest_v2_temporal(fixture.input());
  const auto second = encode_manifest_v2_temporal(fixture.input());
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_TRUE(std::ranges::equal(first->bytes(), second->bytes()));
  EXPECT_EQ(first->size(), 760U);

  const auto decoded = decode_manifest_v2_temporal_exact(first->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  EXPECT_EQ(decoded->generation(), 2U);
  EXPECT_EQ(decoded->previous_generation(), 1U);
  EXPECT_EQ(decoded->wal_reclaim_checkpoint(), fixture.input().wal_reclaim_checkpoint);
  EXPECT_TRUE(std::ranges::equal(decoded->tablets(), fixture.tablets));
  EXPECT_TRUE(std::ranges::equal(decoded->parts(), fixture.parts));
  EXPECT_TRUE(std::ranges::equal(decoded->retries(), fixture.retries));

  const ManifestDecodeResult v1 = decode_manifest_v1_exact(first->bytes());
  ASSERT_FALSE(v1.has_value());
  EXPECT_EQ(v1.error().kind(), ManifestDecodeErrorKind::kUnsupported);
}

TEST(TemporalManifestCodecTest, PrefixClassifiesTruncationAndExactRejectsSuffix) {
  Fixture fixture;
  const auto encoded = encode_manifest_v2_temporal(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  for (std::size_t size = 0U; size < encoded->size(); ++size) {
    const auto decoded = decode_manifest_v2_temporal_prefix(encoded->bytes().first(size));
    ASSERT_FALSE(decoded.has_value()) << size;
    EXPECT_EQ(decoded.error().kind(), ManifestDecodeErrorKind::kIncomplete) << size;
    EXPECT_GT(decoded.error().required_size(), size) << size;
  }
  std::vector<std::byte> suffix(encoded->bytes().begin(), encoded->bytes().end());
  suffix.push_back(std::byte{0U});
  EXPECT_TRUE(decode_manifest_v2_temporal_prefix(suffix).has_value());
  EXPECT_FALSE(decode_manifest_v2_temporal_exact(suffix).has_value());
}

TEST(TemporalManifestCodecTest, SupportsRaftTabletWithoutGlobalWalCheckpoint) {
  Fixture fixture;
  const common::Uuid group = uuid(11U);
  fixture.tablets[0].source_id = group;
  fixture.tablets[0].commit_source = ManifestCommitSource::kRaft;
  fixture.tablets[0].reclaim_position = 3U;
  fixture.parts[0].source_id = group;
  fixture.parts[0].commit_source = ManifestCommitSource::kRaft;
  fixture.retries[0].source_id = group;
  fixture.retries[0].commit_source = ManifestCommitSource::kRaft;
  TemporalManifestEncodeInput input = fixture.input();
  input.wal_reclaim_checkpoint = std::nullopt;
  const auto encoded = encode_manifest_v2_temporal(input);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  const auto decoded = decode_manifest_v2_temporal_exact(encoded->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_FALSE(decoded->wal_reclaim_checkpoint().has_value());
  EXPECT_EQ(decoded->tablets()[0].commit_source, ManifestCommitSource::kRaft);
  EXPECT_EQ(decoded->tablets()[0].reclaim_position, 3U);
}

TEST(TemporalManifestCodecTest, RejectsCrossSourceStateLimitsAndUnknownRegistry) {
  Fixture fixture;
  fixture.parts[0].source_id = uuid(12U);
  EXPECT_FALSE(encode_manifest_v2_temporal(fixture.input()).has_value());

  fixture = Fixture{};
  fixture.tablets[0].source_id = uuid(13U);
  fixture.parts[0].source_id = fixture.tablets[0].source_id;
  fixture.retries[0].source_id = fixture.tablets[0].source_id;
  EXPECT_FALSE(encode_manifest_v2_temporal(fixture.input()).has_value());

  fixture = Fixture{};
  const auto encoded = encode_manifest_v2_temporal(fixture.input());
  ASSERT_TRUE(encoded.has_value());
  const auto limited = decode_manifest_v2_temporal_prefix(
      encoded->bytes(), {.max_file_length = encoded->size() - 1U});
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().kind(), ManifestDecodeErrorKind::kResourceLimit);

  std::vector<std::byte> unknown(encoded->bytes().begin(), encoded->bytes().end());
  const std::size_t source_offset =
      format::kFileHeaderLength + temporal_format::kTabletCommitSourceOffset;
  unknown[source_offset] = std::byte{3U};
  refresh_crcs(unknown);
  const auto rejected = decode_manifest_v2_temporal_exact(unknown);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().kind(), ManifestDecodeErrorKind::kUnsupported);
}

} // namespace
} // namespace chronos::manifest
