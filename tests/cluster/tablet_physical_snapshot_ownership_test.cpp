#include "chronos/cluster/tablet_physical_snapshot_ownership.hpp"
#include "chronos/cseg/temporal_format.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::cluster {
namespace {

class OwnershipTemporaryDirectory {
public:
  OwnershipTemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-physical-ownership-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~OwnershipTemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}
template <typename Identity> [[nodiscard]] Identity id(const std::uint8_t seed) {
  return Identity::from_uuid(uuid(seed)).value();
}

[[nodiscard]] schema::TableSchema make_schema() {
  const schema::ColumnId event_time = id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_time, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  return schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
                                     schema::SchemaVersion::initial(), std::nullopt,
                                     std::move(columns),
                                     {.event_time_column = event_time,
                                      .physical_ordering_key = {event_time},
                                      .partition_columns = {event_time},
                                      .shard_key = {event_time},
                                      .deduplication_key = {event_time}})
      .value();
}

struct OwnershipFixture {
  manifest::DatabaseId database_id{manifest::DatabaseId::from_uuid(uuid(6U)).value()};
  common::Uuid group_id{uuid(8U)};
  cseg::EncodedCsegPart encoded{cseg::test::make_valid_temporal_part(
      cseg::PageCompression::kNone,
      {.commit_source = cseg::temporal_format::CommitSource::kRaft, .source_id = group_id})};
  schema::TableSchema schema{make_schema()};
  schema::SchemaLineage lineage{schema::SchemaLineage::create(schema).value()};
  manifest::TemporalPartDescriptor descriptor{manifest::describe_manifest_v2_temporal_part_image(
                                                  encoded.bytes(), schema, id<schema::TabletId>(3U),
                                                  manifest::ManifestCommitSource::kRaft, group_id)
                                                  .value()};
  manifest::TemporalTabletDescriptor owner{.table_id = descriptor.table_id,
                                           .tablet_id = descriptor.tablet_id,
                                           .recovery_schema_id = descriptor.schema_id,
                                           .recovery_schema_version = descriptor.schema_version,
                                           .source_id = group_id,
                                           .durable_position = descriptor.maximum_commit_position,
                                           .reclaim_position = 0U,
                                           .first_part_index = 0U,
                                           .part_count = 1U,
                                           .durable_version_count = descriptor.row_count,
                                           .commit_source = manifest::ManifestCommitSource::kRaft};

  [[nodiscard]] manifest::EncodedRaftTabletPhysicalSnapshot projection() const {
    const std::array tablets{owner};
    const std::array parts{descriptor};
    manifest::EncodedTemporalManifest source =
        manifest::encode_manifest_v2_temporal({.generation = 14U,
                                               .database_id = database_id,
                                               .wal_reclaim_checkpoint = std::nullopt,
                                               .tablets = tablets,
                                               .parts = parts,
                                               .retries = {}})
            .value();
    auto decoded = manifest::decode_manifest_v2_temporal_exact(source.bytes());
    return manifest::build_raft_tablet_physical_snapshot(*decoded, group_id, owner.tablet_id,
                                                         owner.durable_position)
        .value();
  }

  [[nodiscard]] raft::SnapshotMetadata
  metadata(const manifest::EncodedRaftTabletPhysicalSnapshot& projection) const {
    return {.last_included_index = owner.durable_position,
            .last_included_term = 3U,
            .manifest_generation = 14U,
            .part_set_checksum = projection.part_set_checksum().bytes(),
            .configuration_index = 2U,
            .voters = {1U, 2U}};
  }
};

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

void establish_layout(const std::filesystem::path& root, const OwnershipFixture& fixture) {
  ASSERT_TRUE(std::filesystem::create_directory(root / manifest::kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(root / manifest::kManifestDirectoryName));
  write_bytes(root / manifest::kManifestDirectoryName / manifest::kManifestLockFileName, {});
  manifest::EncodedTemporalManifest initial =
      manifest::encode_manifest_v2_temporal({.generation = 1U,
                                             .database_id = fixture.database_id,
                                             .wal_reclaim_checkpoint = std::nullopt,
                                             .tablets = {},
                                             .parts = {},
                                             .retries = {}})
          .value();
  write_bytes(root / manifest::kManifestDirectoryName / *manifest::manifest_file_name(1U),
              initial.bytes());
}

[[nodiscard]] std::shared_ptr<const manifest::LoadedTemporalManifestGeneration>
load_initial(manifest::ManifestStorage& storage, const OwnershipFixture& fixture) {
  auto loaded =
      storage.load_selected_temporal_manifest({.expected_database_id = fixture.database_id,
                                               .schema_bindings = {},
                                               .source_bindings = {},
                                               .decode_limits = {},
                                               .part_validation_limits = {}});
  EXPECT_TRUE(loaded.has_value()) << loaded.error().to_string();
  return loaded.has_value() ? std::make_shared<const manifest::LoadedTemporalManifestGeneration>(
                                  std::move(*loaded))
                            : nullptr;
}

[[nodiscard]] common::Uuid nonce(const std::uint8_t seed) {
  return uuid(seed);
}

[[nodiscard]] TabletPhysicalSnapshotOwnershipRequest
request(const OwnershipFixture& fixture,
        const manifest::EncodedRaftTabletPhysicalSnapshot& projection,
        const raft::SnapshotMetadata& metadata,
        const std::span<const manifest::TabletSchemaBinding> schemas,
        const std::span<const manifest::TemporalTabletSourceBinding> sources,
        const common::Uuid manifest_nonce) {
  return {.physical_snapshot = projection.bytes(),
          .group_id = fixture.group_id,
          .table_id = fixture.owner.table_id,
          .tablet_id = fixture.owner.tablet_id,
          .raft_snapshot = std::cref(metadata),
          .schema_bindings = schemas,
          .source_bindings = sources,
          .manifest_nonce = manifest_nonce,
          .decode_limits = {},
          .part_validation_limits = {}};
}

TEST(TabletPhysicalSnapshotOwnershipTest, InstallsReloadsAndAtomicallyPublishesOwnership) {
  OwnershipTemporaryDirectory directory;
  OwnershipFixture fixture;
  establish_layout(directory.path(), fixture);
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  ASSERT_TRUE(storage
                  ->install_temporal_part({.encoded_part = std::cref(fixture.encoded),
                                           .descriptor = fixture.descriptor,
                                           .owner = fixture.owner,
                                           .schema = std::cref(fixture.schema),
                                           .nonce = nonce(20U),
                                           .validation_limits = {}})
                  .has_value());
  auto publisher =
      manifest::TemporalDatabaseStoragePublisher::create(load_initial(*storage, fixture), {});
  ASSERT_TRUE(publisher.has_value());
  auto old = publisher->snapshot();
  ASSERT_TRUE(old.has_value());
  auto projection = fixture.projection();
  const raft::SnapshotMetadata metadata = fixture.metadata(projection);
  const std::array schemas{manifest::TabletSchemaBinding{.tablet_id = fixture.owner.tablet_id,
                                                         .lineage = std::cref(fixture.lineage)}};
  const std::array sources{
      manifest::TemporalTabletSourceBinding{.tablet_id = fixture.owner.tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = fixture.group_id}};

  auto published = install_and_publish_tablet_physical_snapshot(
      *storage, *publisher, request(fixture, projection, metadata, schemas, sources, nonce(21U)));
  ASSERT_TRUE(published.has_value()) << published.error().to_string();
  EXPECT_FALSE(published->manifest_already_durable);
  EXPECT_EQ(published->destination.generation(), 2U);
  ASSERT_EQ(published->destination.tablets().size(), 1U);
  EXPECT_EQ(published->destination.parts().front(), fixture.descriptor);
  EXPECT_EQ(publisher->snapshot()->generation(), 2U);
  EXPECT_EQ(old->generation(), 1U);
}

TEST(TabletPhysicalSnapshotOwnershipTest, ResumesExactAlreadyDurableSuccessor) {
  OwnershipTemporaryDirectory directory;
  OwnershipFixture fixture;
  establish_layout(directory.path(), fixture);
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  ASSERT_TRUE(storage
                  ->install_temporal_part({.encoded_part = std::cref(fixture.encoded),
                                           .descriptor = fixture.descriptor,
                                           .owner = fixture.owner,
                                           .schema = std::cref(fixture.schema),
                                           .nonce = nonce(30U),
                                           .validation_limits = {}})
                  .has_value());
  auto initial = load_initial(*storage, fixture);
  auto publisher = manifest::TemporalDatabaseStoragePublisher::create(initial, {});
  ASSERT_TRUE(publisher.has_value());
  auto projection = fixture.projection();
  const raft::SnapshotMetadata metadata = fixture.metadata(projection);
  const std::array schemas{manifest::TabletSchemaBinding{.tablet_id = fixture.owner.tablet_id,
                                                         .lineage = std::cref(fixture.lineage)}};
  const std::array sources{
      manifest::TemporalTabletSourceBinding{.tablet_id = fixture.owner.tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = fixture.group_id}};
  auto initial_view = manifest::decode_manifest_v2_temporal_exact(initial->encoded_bytes());
  auto candidate = manifest::build_raft_tablet_destination_manifest(
      *initial_view, {.physical_snapshot = projection.bytes(),
                      .group_id = fixture.group_id,
                      .table_id = fixture.owner.table_id,
                      .tablet_id = fixture.owner.tablet_id,
                      .raft_snapshot = std::cref(metadata),
                      .schema_bindings = schemas,
                      .decode_limits = {}});
  ASSERT_TRUE(candidate.has_value());
  ASSERT_TRUE(storage
                  ->install_temporal_manifest({.encoded_manifest = std::cref(*candidate),
                                               .schema_bindings = schemas,
                                               .nonce = nonce(31U),
                                               .decode_limits = {},
                                               .part_validation_limits = {}})
                  .has_value());

  auto resumed = install_and_publish_tablet_physical_snapshot(
      *storage, *publisher, request(fixture, projection, metadata, schemas, sources, nonce(32U)));
  ASSERT_TRUE(resumed.has_value()) << resumed.error().to_string();
  EXPECT_TRUE(resumed->manifest_already_durable);
  EXPECT_EQ(resumed->destination.generation(), 2U);
}

TEST(TabletPhysicalSnapshotOwnershipTest, MissingPartCannotBecomePublishedOwnership) {
  OwnershipTemporaryDirectory directory;
  OwnershipFixture fixture;
  establish_layout(directory.path(), fixture);
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  auto publisher =
      manifest::TemporalDatabaseStoragePublisher::create(load_initial(*storage, fixture), {});
  ASSERT_TRUE(publisher.has_value());
  auto projection = fixture.projection();
  const raft::SnapshotMetadata metadata = fixture.metadata(projection);
  const std::array schemas{manifest::TabletSchemaBinding{.tablet_id = fixture.owner.tablet_id,
                                                         .lineage = std::cref(fixture.lineage)}};
  const std::array sources{
      manifest::TemporalTabletSourceBinding{.tablet_id = fixture.owner.tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = fixture.group_id}};

  EXPECT_EQ(install_and_publish_tablet_physical_snapshot(
                *storage, *publisher,
                request(fixture, projection, metadata, schemas, sources, nonce(40U)))
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(publisher->is_usable());
  EXPECT_EQ(publisher->snapshot()->generation(), 1U);
  EXPECT_EQ(storage->scan_namespace()->generations.back(), 1U);
}

TEST(TabletPhysicalSnapshotOwnershipTest, DifferentAlreadyDurableSuccessorFailsPublisherClosed) {
  OwnershipTemporaryDirectory directory;
  OwnershipFixture fixture;
  establish_layout(directory.path(), fixture);
  auto storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  auto publisher =
      manifest::TemporalDatabaseStoragePublisher::create(load_initial(*storage, fixture), {});
  ASSERT_TRUE(publisher.has_value());
  manifest::EncodedTemporalManifest different =
      manifest::encode_manifest_v2_temporal({.generation = 2U,
                                             .database_id = fixture.database_id,
                                             .wal_reclaim_checkpoint = std::nullopt,
                                             .tablets = {},
                                             .parts = {},
                                             .retries = {}})
          .value();
  ASSERT_TRUE(storage
                  ->install_temporal_manifest({.encoded_manifest = std::cref(different),
                                               .schema_bindings = {},
                                               .nonce = nonce(50U),
                                               .decode_limits = {},
                                               .part_validation_limits = {}})
                  .has_value());
  auto projection = fixture.projection();
  const raft::SnapshotMetadata metadata = fixture.metadata(projection);
  const std::array schemas{manifest::TabletSchemaBinding{.tablet_id = fixture.owner.tablet_id,
                                                         .lineage = std::cref(fixture.lineage)}};
  const std::array sources{
      manifest::TemporalTabletSourceBinding{.tablet_id = fixture.owner.tablet_id,
                                            .commit_source = manifest::ManifestCommitSource::kRaft,
                                            .source_id = fixture.group_id}};

  EXPECT_FALSE(install_and_publish_tablet_physical_snapshot(
                   *storage, *publisher,
                   request(fixture, projection, metadata, schemas, sources, nonce(51U)))
                   .has_value());
  EXPECT_FALSE(publisher->is_usable());
  EXPECT_EQ(publisher->snapshot().error().code(), common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::cluster
