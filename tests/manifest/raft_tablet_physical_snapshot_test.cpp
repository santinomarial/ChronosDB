#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
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

struct Fixture {
  DatabaseId database_id{test::make_id<DatabaseId>(1U)};
  schema::TableId table_id{test::make_id<schema::TableId>(2U)};
  schema::TabletId tablet_id{test::make_id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{test::make_id<schema::SchemaId>(4U)};
  cseg::PartId part_id{test::make_id<cseg::PartId>(5U)};
  raft::GroupId group_id{uuid(10U)};
  schema::TabletId other_tablet_id{test::make_id<schema::TabletId>(13U)};
  cseg::PartId other_part_id{test::make_id<cseg::PartId>(15U)};
  raft::GroupId other_group_id{uuid(20U)};
  std::vector<TemporalTabletDescriptor> tablets{
      {.table_id = table_id,
       .tablet_id = tablet_id,
       .recovery_schema_id = schema_id,
       .recovery_schema_version = schema::SchemaVersion::initial(),
       .source_id = group_id,
       .durable_position = 9U,
       .reclaim_position = 5U,
       .first_part_index = 0U,
       .part_count = 1U,
       .durable_version_count = 2U,
       .commit_source = ManifestCommitSource::kRaft},
      {.table_id = table_id,
       .tablet_id = other_tablet_id,
       .recovery_schema_id = schema_id,
       .recovery_schema_version = schema::SchemaVersion::initial(),
       .source_id = other_group_id,
       .durable_position = 4U,
       .reclaim_position = 0U,
       .first_part_index = 1U,
       .part_count = 1U,
       .durable_version_count = 1U,
       .commit_source = ManifestCommitSource::kRaft}};
  std::vector<TemporalPartDescriptor> parts{{.part_id = part_id,
                                             .table_id = table_id,
                                             .tablet_id = tablet_id,
                                             .schema_id = schema_id,
                                             .schema_version = schema::SchemaVersion::initial(),
                                             .file_length = 4096U,
                                             .row_count = 2U,
                                             .minimum_commit_position = 8U,
                                             .maximum_commit_position = 9U,
                                             .minimum_event_time = 10,
                                             .maximum_event_time = 20,
                                             .minimum_system_time = 100,
                                             .maximum_system_time = 110,
                                             .source_id = group_id,
                                             .content_sha256 = digest(6U),
                                             .commit_source = ManifestCommitSource::kRaft},
                                            {.part_id = other_part_id,
                                             .table_id = table_id,
                                             .tablet_id = other_tablet_id,
                                             .schema_id = schema_id,
                                             .schema_version = schema::SchemaVersion::initial(),
                                             .file_length = 2048U,
                                             .row_count = 1U,
                                             .minimum_commit_position = 4U,
                                             .maximum_commit_position = 4U,
                                             .minimum_event_time = 30,
                                             .maximum_event_time = 30,
                                             .minimum_system_time = 120,
                                             .maximum_system_time = 120,
                                             .source_id = other_group_id,
                                             .content_sha256 = digest(16U),
                                             .commit_source = ManifestCommitSource::kRaft}};
  std::vector<TemporalRetryDescriptor> retries{
      {.client_id = test::make_id<ingest::ClientId>(7U),
       .client_batch_id = test::make_id<ingest::ClientBatchId>(8U),
       .table_id = table_id,
       .tablet_id = tablet_id,
       .request_digest = digest(9U),
       .source_id = group_id,
       .commit_position = 9U,
       .applied_row_count = 2U,
       .commit_source = ManifestCommitSource::kRaft},
      {.client_id = test::make_id<ingest::ClientId>(17U),
       .client_batch_id = test::make_id<ingest::ClientBatchId>(18U),
       .table_id = table_id,
       .tablet_id = other_tablet_id,
       .request_digest = digest(19U),
       .source_id = other_group_id,
       .commit_position = 4U,
       .applied_row_count = 1U,
       .commit_source = ManifestCommitSource::kRaft}};

  [[nodiscard]] EncodedTemporalManifest encode() const {
    auto encoded = encode_manifest_v2_temporal({.generation = 7U,
                                                .database_id = database_id,
                                                .wal_reclaim_checkpoint = std::nullopt,
                                                .tablets = tablets,
                                                .parts = parts,
                                                .retries = retries});
    EXPECT_TRUE(encoded.has_value()) << encoded.error().to_string();
    return std::move(*encoded);
  }

  [[nodiscard]] raft::SnapshotMetadata metadata(const ingest::Sha256Digest& checksum) const {
    return {.last_included_index = 9U,
            .last_included_term = 3U,
            .manifest_generation = 7U,
            .part_set_checksum = checksum.bytes(),
            .configuration_index = 2U,
            .voters = {1U, 2U}};
  }
};

TEST(RaftTabletPhysicalSnapshotTest, ProjectsOneRaftTabletAndBindsCanonicalPartSet) {
  Fixture fixture;
  const EncodedTemporalManifest full = fixture.encode();
  auto selected = decode_manifest_v2_temporal_exact(full.bytes());
  ASSERT_TRUE(selected.has_value()) << selected.error().status().to_string();

  auto projected =
      build_raft_tablet_physical_snapshot(*selected, fixture.group_id, fixture.tablet_id, 9U);
  ASSERT_TRUE(projected.has_value()) << projected.error().to_string();
  auto decoded = decode_manifest_v2_temporal_exact(projected->bytes());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().status().to_string();
  ASSERT_EQ(decoded->tablets().size(), 1U);
  ASSERT_EQ(decoded->parts().size(), 1U);
  ASSERT_EQ(decoded->retries().size(), 1U);
  EXPECT_EQ(decoded->tablets().front().tablet_id, fixture.tablet_id);
  EXPECT_EQ(decoded->tablets().front().first_part_index, 0U);
  EXPECT_EQ(decoded->parts().front().part_id, fixture.part_id);
  EXPECT_FALSE(decoded->wal_reclaim_checkpoint().has_value());

  const auto report = validate_raft_tablet_physical_snapshot(
      projected->bytes(), fixture.group_id, fixture.table_id, fixture.tablet_id,
      fixture.metadata(projected->part_set_checksum()));
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(report->database_id, fixture.database_id);
  EXPECT_EQ(report->manifest_generation, 7U);
  EXPECT_EQ(report->applied_position, 9U);
  EXPECT_EQ(report->part_count, 1U);
  EXPECT_EQ(report->retry_count, 1U);
  EXPECT_EQ(report->part_set_checksum, projected->part_set_checksum());
}

TEST(RaftTabletPhysicalSnapshotTest, RejectsWrongSourceBoundaryAndAggregateChecksum) {
  Fixture fixture;
  const EncodedTemporalManifest full = fixture.encode();
  auto selected = decode_manifest_v2_temporal_exact(full.bytes());
  ASSERT_TRUE(selected.has_value()) << selected.error().status().to_string();

  EXPECT_EQ(
      build_raft_tablet_physical_snapshot(*selected, fixture.other_group_id, fixture.tablet_id, 9U)
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);
  EXPECT_EQ(build_raft_tablet_physical_snapshot(*selected, fixture.group_id, fixture.tablet_id, 8U)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  auto projected =
      build_raft_tablet_physical_snapshot(*selected, fixture.group_id, fixture.tablet_id, 9U);
  ASSERT_TRUE(projected.has_value()) << projected.error().to_string();
  raft::SnapshotMetadata wrong = fixture.metadata(projected->part_set_checksum());
  wrong.part_set_checksum.back() ^= std::byte{0x01U};
  EXPECT_EQ(validate_raft_tablet_physical_snapshot(projected->bytes(), fixture.group_id,
                                                   fixture.table_id, fixture.tablet_id, wrong)
                .error()
                .code(),
            common::StatusCode::kCorruption);
}

TEST(RaftTabletPhysicalSnapshotTest, RejectsFullDatabaseManifestAndCorruptedBytes) {
  Fixture fixture;
  const EncodedTemporalManifest full = fixture.encode();
  auto selected = decode_manifest_v2_temporal_exact(full.bytes());
  ASSERT_TRUE(selected.has_value()) << selected.error().status().to_string();
  auto projected =
      build_raft_tablet_physical_snapshot(*selected, fixture.group_id, fixture.tablet_id, 9U);
  ASSERT_TRUE(projected.has_value()) << projected.error().to_string();
  const raft::SnapshotMetadata metadata = fixture.metadata(projected->part_set_checksum());

  EXPECT_EQ(validate_raft_tablet_physical_snapshot(full.bytes(), fixture.group_id, fixture.table_id,
                                                   fixture.tablet_id, metadata)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  std::vector<std::byte> damaged(projected->bytes().begin(), projected->bytes().end());
  damaged.back() ^= std::byte{0x80U};
  EXPECT_EQ(validate_raft_tablet_physical_snapshot(damaged, fixture.group_id, fixture.table_id,
                                                   fixture.tablet_id, metadata)
                .error()
                .code(),
            common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::manifest
