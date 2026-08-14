#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "manifest/manifest_test_support.hpp"

#include <algorithm>
#include <array>
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

  [[nodiscard]] static raft::SnapshotMetadata metadata(const ingest::Sha256Digest& checksum) {
    return {.last_included_index = 9U,
            .last_included_term = 3U,
            .manifest_generation = 7U,
            .part_set_checksum = checksum.bytes(),
            .configuration_index = 2U,
            .voters = {1U, 2U}};
  }
};

[[nodiscard]] schema::SchemaLineage lineage(const Fixture& fixture) {
  const schema::ColumnId event_time = test::make_id<schema::ColumnId>(30U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_time, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  schema::TableSchema value = schema::TableSchema::create(fixture.table_id, fixture.schema_id,
                                                          schema::SchemaVersion::initial(),
                                                          std::nullopt, std::move(columns),
                                                          {.event_time_column = event_time,
                                                           .physical_ordering_key = {event_time},
                                                           .partition_columns = {event_time},
                                                           .shard_key = {event_time},
                                                           .deduplication_key = {event_time}})
                                  .value();
  return schema::SchemaLineage::create(std::move(value)).value();
}

[[nodiscard]] raft::TabletMovementRecord completed_movement(const Fixture& fixture) {
  auto movement = raft::TabletMovement::begin(fixture.tablet_id, 10U, 1U, 3U, {1U, 2U}).value();
  const std::array bytes{std::byte{0xA5U}};
  EXPECT_TRUE(movement.begin_snapshot({7U, 9U, 3U, bytes.size(), common::crc32c(bytes)}).is_ok());
  EXPECT_TRUE(movement.accept_snapshot_chunk(0U, bytes, common::crc32c(bytes)).is_ok());
  EXPECT_TRUE(movement.finish_snapshot().is_ok());
  EXPECT_TRUE(movement.mark_caught_up(9U).is_ok());
  EXPECT_TRUE(movement.promote_target(10U, 11U).is_ok());
  EXPECT_TRUE(movement.remove_source(11U, 12U).is_ok());
  return movement.record();
}

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
      Fixture::metadata(projected->part_set_checksum()));
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
  raft::SnapshotMetadata wrong = Fixture::metadata(projected->part_set_checksum());
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
  const raft::SnapshotMetadata metadata = Fixture::metadata(projected->part_set_checksum());

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

TEST(RaftTabletPhysicalSnapshotTest, BuildsCanonicalDestinationSuccessorForNewTablet) {
  Fixture fixture;
  const EncodedTemporalManifest full = fixture.encode();
  auto source = decode_manifest_v2_temporal_exact(full.bytes());
  ASSERT_TRUE(source.has_value());
  auto projected =
      build_raft_tablet_physical_snapshot(*source, fixture.group_id, fixture.tablet_id, 9U);
  ASSERT_TRUE(projected.has_value());
  const raft::SnapshotMetadata metadata = Fixture::metadata(projected->part_set_checksum());

  TemporalTabletDescriptor retained_tablet = fixture.tablets[1U];
  retained_tablet.first_part_index = 0U;
  const std::array destination_tablets{retained_tablet};
  const std::array destination_parts{fixture.parts[1U]};
  const std::array destination_retries{fixture.retries[1U]};
  EncodedTemporalManifest destination_bytes =
      encode_manifest_v2_temporal({.generation = 1U,
                                   .database_id = fixture.database_id,
                                   .wal_reclaim_checkpoint = std::nullopt,
                                   .tablets = destination_tablets,
                                   .parts = destination_parts,
                                   .retries = destination_retries})
          .value();
  auto destination = decode_manifest_v2_temporal_exact(destination_bytes.bytes());
  ASSERT_TRUE(destination.has_value());
  const schema::SchemaLineage schemas = lineage(fixture);
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = fixture.tablet_id, .lineage = std::cref(schemas)},
      TabletSchemaBinding{.tablet_id = fixture.other_tablet_id, .lineage = std::cref(schemas)}};

  auto candidate =
      build_raft_tablet_destination_manifest(*destination, {.physical_snapshot = projected->bytes(),
                                                            .group_id = fixture.group_id,
                                                            .table_id = fixture.table_id,
                                                            .tablet_id = fixture.tablet_id,
                                                            .raft_snapshot = std::cref(metadata),
                                                            .schema_bindings = bindings,
                                                            .decode_limits = {}});
  ASSERT_TRUE(candidate.has_value()) << candidate.error().to_string();
  auto decoded = decode_manifest_v2_temporal_exact(candidate->bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->generation(), 2U);
  ASSERT_EQ(decoded->tablets().size(), 2U);
  EXPECT_EQ(decoded->tablets()[0U].tablet_id, fixture.tablet_id);
  EXPECT_EQ(decoded->tablets()[1U].tablet_id, fixture.other_tablet_id);
  EXPECT_EQ(decoded->parts()[0U], fixture.parts[0U]);
  EXPECT_EQ(decoded->parts()[1U], fixture.parts[1U]);
  EXPECT_EQ(decoded->retries()[0U], fixture.retries[0U]);
  EXPECT_EQ(decoded->retries()[1U], fixture.retries[1U]);
  EXPECT_TRUE(validate_manifest_v2_temporal_transition(*destination, *decoded, bindings).is_ok());
}

TEST(RaftTabletPhysicalSnapshotTest, RejectsExistingTabletAndForeignDatabaseDestination) {
  Fixture fixture;
  const EncodedTemporalManifest full = fixture.encode();
  auto source = decode_manifest_v2_temporal_exact(full.bytes());
  ASSERT_TRUE(source.has_value());
  auto projected =
      build_raft_tablet_physical_snapshot(*source, fixture.group_id, fixture.tablet_id, 9U);
  ASSERT_TRUE(projected.has_value());
  const raft::SnapshotMetadata metadata = Fixture::metadata(projected->part_set_checksum());
  const schema::SchemaLineage schemas = lineage(fixture);
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = fixture.tablet_id, .lineage = std::cref(schemas)},
      TabletSchemaBinding{.tablet_id = fixture.other_tablet_id, .lineage = std::cref(schemas)}};
  EXPECT_EQ(
      build_raft_tablet_destination_manifest(*source, {.physical_snapshot = projected->bytes(),
                                                       .group_id = fixture.group_id,
                                                       .table_id = fixture.table_id,
                                                       .tablet_id = fixture.tablet_id,
                                                       .raft_snapshot = std::cref(metadata),
                                                       .schema_bindings = bindings,
                                                       .decode_limits = {}})
          .error()
          .code(),
      common::StatusCode::kInvalidArgument);

  EncodedTemporalManifest foreign =
      encode_manifest_v2_temporal({.generation = 1U,
                                   .database_id = test::make_id<DatabaseId>(99U),
                                   .wal_reclaim_checkpoint = std::nullopt,
                                   .tablets = {},
                                   .parts = {},
                                   .retries = {}})
          .value();
  auto foreign_destination = decode_manifest_v2_temporal_exact(foreign.bytes());
  ASSERT_TRUE(foreign_destination.has_value());
  EXPECT_EQ(build_raft_tablet_destination_manifest(*foreign_destination,
                                                   {.physical_snapshot = projected->bytes(),
                                                    .group_id = fixture.group_id,
                                                    .table_id = fixture.table_id,
                                                    .tablet_id = fixture.tablet_id,
                                                    .raft_snapshot = std::cref(metadata),
                                                    .schema_bindings = {},
                                                    .decode_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(RaftTabletPhysicalSnapshotTest, BuildsSourceRetirementAfterCommittedReplicaRemoval) {
  Fixture fixture;
  const EncodedTemporalManifest full = fixture.encode();
  auto source = decode_manifest_v2_temporal_exact(full.bytes());
  ASSERT_TRUE(source.has_value());
  const raft::TabletMovementRecord movement = completed_movement(fixture);
  const raft::TabletPlacementMetadata placement{
      fixture.table_id, fixture.tablet_id, 12U, {2U, 3U}, 3U};

  auto retired = build_raft_tablet_source_retirement_manifest(
      *source, {.group_id = fixture.group_id,
                .table_id = fixture.table_id,
                .tablet_id = fixture.tablet_id,
                .source_node = 1U,
                .completed_movement = std::cref(movement),
                .committed_placement = std::cref(placement)});
  ASSERT_TRUE(retired.has_value()) << retired.error().to_string();
  EXPECT_EQ(retired->predecessor_generation, 7U);
  ASSERT_EQ(retired->retired_parts.size(), 1U);
  EXPECT_EQ(retired->retired_parts.front(), fixture.parts.front());
  auto decoded = decode_manifest_v2_temporal_exact(retired->manifest.bytes());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->generation(), 8U);
  ASSERT_EQ(decoded->tablets().size(), 1U);
  TemporalTabletDescriptor expected_retained_tablet = fixture.tablets.back();
  expected_retained_tablet.first_part_index = 0U;
  EXPECT_EQ(decoded->tablets().front(), expected_retained_tablet);
  ASSERT_EQ(decoded->parts().size(), 1U);
  EXPECT_EQ(decoded->parts().front(), fixture.parts.back());
  ASSERT_EQ(decoded->retries().size(), 1U);
  EXPECT_EQ(decoded->retries().front(), fixture.retries.back());
}

TEST(RaftTabletPhysicalSnapshotTest, RejectsSourceRetirementBeforeFinalPlacement) {
  Fixture fixture;
  const EncodedTemporalManifest full = fixture.encode();
  auto source = decode_manifest_v2_temporal_exact(full.bytes());
  ASSERT_TRUE(source.has_value());
  raft::TabletMovementRecord movement = completed_movement(fixture);
  const raft::TabletPlacementMetadata stale{
      fixture.table_id, fixture.tablet_id, 11U, {1U, 2U, 3U}, 1U};
  EXPECT_EQ(build_raft_tablet_source_retirement_manifest(*source,
                                                         {.group_id = fixture.group_id,
                                                          .table_id = fixture.table_id,
                                                          .tablet_id = fixture.tablet_id,
                                                          .source_node = 1U,
                                                          .completed_movement = std::cref(movement),
                                                          .committed_placement = std::cref(stale)})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  movement.phase = raft::TabletMovementPhase::kTargetPromoted;
  const raft::TabletPlacementMetadata final{fixture.table_id, fixture.tablet_id, 12U, {2U, 3U}, 3U};
  EXPECT_FALSE(build_raft_tablet_source_retirement_manifest(
                   *source, {.group_id = fixture.group_id,
                             .table_id = fixture.table_id,
                             .tablet_id = fixture.tablet_id,
                             .source_node = 1U,
                             .completed_movement = std::cref(movement),
                             .committed_placement = std::cref(final)})
                   .has_value());
}

} // namespace
} // namespace chronos::manifest
