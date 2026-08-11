#include "chronos/common/crc32c.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "manifest/manifest_test_support.hpp"

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

namespace chronos::manifest {
namespace {

class TemporalPublicationDirectory {
public:
  TemporalPublicationDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-temporal-publication-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporalPublicationDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid nonce(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

[[nodiscard]] EncodedTemporalManifest empty_manifest(const DatabaseId database_id,
                                                     const std::uint64_t generation) {
  return encode_manifest_v2_temporal({.generation = generation,
                                      .database_id = database_id,
                                      .wal_reclaim_checkpoint = std::nullopt,
                                      .tablets = {},
                                      .parts = {},
                                      .retries = {}})
      .value();
}

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

struct TemporalRetirementFixture {
  schema::TableId table_id{cseg::test::identifier<schema::TableId>(2U)};
  schema::TabletId tablet_id{cseg::test::identifier<schema::TabletId>(3U)};
  schema::SchemaId schema_id{cseg::test::identifier<schema::SchemaId>(4U)};
  raft::GroupId group_id{common::Uuid{cseg::test::identifier<schema::SchemaId>(8U).bytes()}};
  DatabaseId database_id{test::make_id<DatabaseId>(43U)};
  schema::TableSchema schema_value{make_schema()};
  schema::SchemaLineage lineage{schema::SchemaLineage::create(schema_value).value()};
  cseg::EncodedCsegPart encoded{cseg::test::make_valid_temporal_part(
      cseg::PageCompression::kZstd,
      {.commit_source = cseg::temporal_format::CommitSource::kRaft, .source_id = group_id})};
  TemporalPartDescriptor descriptor{
      describe_manifest_v2_temporal_part_image(encoded.bytes(), schema_value, tablet_id,
                                               ManifestCommitSource::kRaft, group_id)
          .value()};
  TemporalTabletDescriptor owner{.table_id = table_id,
                                 .tablet_id = tablet_id,
                                 .recovery_schema_id = schema_id,
                                 .recovery_schema_version = schema::SchemaVersion::initial(),
                                 .source_id = group_id,
                                 .durable_position = 9U,
                                 .reclaim_position = 0U,
                                 .first_part_index = 0U,
                                 .part_count = 1U,
                                 .durable_version_count = 2U,
                                 .commit_source = ManifestCommitSource::kRaft};

  [[nodiscard]] schema::TableSchema make_schema() const {
    const schema::ColumnId event_id = cseg::test::identifier<schema::ColumnId>(5U);
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {event_id}})
        .value();
  }

  [[nodiscard]] EncodedTemporalManifest manifest(const std::uint64_t generation) const {
    const std::array tablets{owner};
    const std::array parts{descriptor};
    return encode_manifest_v2_temporal({.generation = generation,
                                        .database_id = database_id,
                                        .wal_reclaim_checkpoint = std::nullopt,
                                        .tablets = tablets,
                                        .parts = parts,
                                        .retries = {}})
        .value();
  }

  [[nodiscard]] std::array<TabletSchemaBinding, 1> bindings() const {
    return {TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  }

  [[nodiscard]] std::array<TemporalTabletSourceBinding, 1> source_bindings() const {
    return {TemporalTabletSourceBinding{.tablet_id = tablet_id,
                                        .commit_source = ManifestCommitSource::kRaft,
                                        .source_id = group_id}};
  }

  [[nodiscard]] raft::TabletMovementRecord completed_movement() const {
    raft::TabletMovement movement =
        raft::TabletMovement::begin(tablet_id, 10U, 1U, 3U, {1U, 2U}).value();
    const std::array bytes{std::byte{0xA5U}};
    EXPECT_TRUE(movement.begin_snapshot({1U, 9U, 3U, bytes.size(), common::crc32c(bytes)}).is_ok());
    EXPECT_TRUE(movement.accept_snapshot_chunk(0U, bytes, common::crc32c(bytes)).is_ok());
    EXPECT_TRUE(movement.finish_snapshot().is_ok());
    EXPECT_TRUE(movement.mark_caught_up(9U).is_ok());
    EXPECT_TRUE(movement.promote_target(10U, 11U).is_ok());
    EXPECT_TRUE(movement.remove_source(11U, 12U).is_ok());
    return movement.record();
  }
};

void establish_layout(const std::filesystem::path& root, const DatabaseId database_id) {
  ASSERT_TRUE(std::filesystem::create_directory(root / kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(root / kManifestDirectoryName));
  {
    std::ofstream lock{root / kManifestDirectoryName / kManifestLockFileName, std::ios::binary};
    ASSERT_TRUE(lock.good());
  }
  const EncodedTemporalManifest initial = empty_manifest(database_id, 1U);
  std::ofstream output{root / kManifestDirectoryName / *manifest_file_name(1U), std::ios::binary};
  ASSERT_TRUE(output.good());
  for (const std::byte value : initial.bytes())
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

[[nodiscard]] std::shared_ptr<const LoadedTemporalManifestGeneration>
load_selected(ManifestStorage& storage, const DatabaseId database_id) {
  auto loaded = storage.load_selected_temporal_manifest({.expected_database_id = database_id,
                                                         .schema_bindings = {},
                                                         .source_bindings = {},
                                                         .decode_limits = {},
                                                         .part_validation_limits = {}});
  EXPECT_TRUE(loaded.has_value()) << loaded.error().to_string();
  return loaded.has_value()
             ? std::make_shared<const LoadedTemporalManifestGeneration>(std::move(*loaded))
             : nullptr;
}

TEST(TemporalDatabaseStoragePublisherTest, AtomicallyPublishesDurableSuccessorAndRetainsOldEpoch) {
  TemporalPublicationDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const DatabaseId database_id = test::make_id<DatabaseId>(41U);
  establish_layout(directory.path(), database_id);
  auto storage = ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  auto initial = load_selected(*storage, database_id);
  ASSERT_NE(initial, nullptr);
  auto publisher = TemporalDatabaseStoragePublisher::create(initial, {});
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();
  auto held = publisher->snapshot();
  ASSERT_TRUE(held.has_value());
  EXPECT_EQ(held->generation(), 1U);

  const EncodedTemporalManifest candidate = empty_manifest(database_id, 2U);
  auto installed = storage->install_temporal_manifest({.encoded_manifest = std::cref(candidate),
                                                       .schema_bindings = {},
                                                       .nonce = nonce(1U),
                                                       .decode_limits = {},
                                                       .part_validation_limits = {}});
  ASSERT_TRUE(installed.has_value()) << installed.error().to_string();
  auto successor = load_selected(*storage, database_id);
  ASSERT_NE(successor, nullptr);
  auto published = publisher->publish_manifest(
      {.selected_manifest = successor, .schema_bindings = {}, .decode_limits = {}});
  ASSERT_TRUE(published.has_value()) << published.error().to_string();
  EXPECT_EQ(published->generation(), 2U);
  EXPECT_EQ(publisher->snapshot()->generation(), 2U);
  EXPECT_EQ(held->generation(), 1U);
  EXPECT_TRUE(publisher->is_usable());
}

TEST(TemporalDatabaseStoragePublisherTest, FailsClosedWhenDurableNamespaceSkipsLiveEpoch) {
  TemporalPublicationDirectory directory;
  const DatabaseId database_id = test::make_id<DatabaseId>(42U);
  establish_layout(directory.path(), database_id);
  auto storage = ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value());
  auto publisher =
      TemporalDatabaseStoragePublisher::create(load_selected(*storage, database_id), {});
  ASSERT_TRUE(publisher.has_value());

  const EncodedTemporalManifest generation_two = empty_manifest(database_id, 2U);
  ASSERT_TRUE(storage
                  ->install_temporal_manifest({.encoded_manifest = std::cref(generation_two),
                                               .schema_bindings = {},
                                               .nonce = nonce(2U),
                                               .decode_limits = {},
                                               .part_validation_limits = {}})
                  .has_value());
  const EncodedTemporalManifest generation_three = empty_manifest(database_id, 3U);
  ASSERT_TRUE(storage
                  ->install_temporal_manifest({.encoded_manifest = std::cref(generation_three),
                                               .schema_bindings = {},
                                               .nonce = nonce(3U),
                                               .decode_limits = {},
                                               .part_validation_limits = {}})
                  .has_value());
  auto generation_three_owner = load_selected(*storage, database_id);
  ASSERT_NE(generation_three_owner, nullptr);
  EXPECT_EQ(publisher
                ->publish_manifest({.selected_manifest = generation_three_owner,
                                    .schema_bindings = {},
                                    .decode_limits = {}})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(publisher->is_usable());
  EXPECT_EQ(publisher->snapshot().error().code(), common::StatusCode::kUnavailable);
  EXPECT_FALSE(publisher->poison_status().is_ok());
}

TEST(TemporalDatabaseStoragePublisherTest,
     PublishesAuthorizedSourceRetirementAndPinsExactPredecessor) {
  TemporalPublicationDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const TemporalRetirementFixture fixture;
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / kManifestDirectoryName));
  write_file(directory.path() / kManifestDirectoryName / kManifestLockFileName, {});
  const EncodedTemporalManifest predecessor = fixture.manifest(1U);
  write_file(directory.path() / kManifestDirectoryName / *manifest_file_name(1U),
             predecessor.bytes());
  write_file(directory.path() / kPartsDirectoryName / part_file_name(fixture.descriptor.part_id),
             fixture.encoded.bytes());
  auto storage = ManifestStorage::open_existing({.database_root = directory.path().string()});
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  const auto bindings = fixture.bindings();
  const auto sources = fixture.source_bindings();
  auto loaded =
      storage->load_selected_temporal_manifest({.expected_database_id = fixture.database_id,
                                                .schema_bindings = bindings,
                                                .source_bindings = sources,
                                                .decode_limits = {},
                                                .part_validation_limits = {}});
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  auto initial = std::make_shared<const LoadedTemporalManifestGeneration>(std::move(*loaded));
  auto publisher = TemporalDatabaseStoragePublisher::create(initial, bindings);
  ASSERT_TRUE(publisher.has_value()) << publisher.error().to_string();

  std::optional<TemporalRetiredPartSet> retirement;
  {
    auto earliest_reader = publisher->snapshot();
    ASSERT_TRUE(earliest_reader.has_value());
    const EncodedTemporalManifest retained_manifest = fixture.manifest(2U);
    ASSERT_TRUE(storage
                    ->install_temporal_manifest({.encoded_manifest = std::cref(retained_manifest),
                                                 .schema_bindings = bindings,
                                                 .nonce = nonce(4U),
                                                 .decode_limits = {},
                                                 .part_validation_limits = {}})
                    .has_value());
    auto retained_loaded =
        storage->load_selected_temporal_manifest({.expected_database_id = fixture.database_id,
                                                  .schema_bindings = bindings,
                                                  .source_bindings = sources,
                                                  .decode_limits = {},
                                                  .part_validation_limits = {}});
    ASSERT_TRUE(retained_loaded.has_value()) << retained_loaded.error().to_string();
    auto retained =
        std::make_shared<const LoadedTemporalManifestGeneration>(std::move(*retained_loaded));
    ASSERT_TRUE(
        publisher
            ->publish_manifest(
                {.selected_manifest = retained, .schema_bindings = bindings, .decode_limits = {}})
            .has_value());
    initial.reset();

    const raft::TabletMovementRecord movement = fixture.completed_movement();
    const raft::TabletPlacementMetadata placement{
        fixture.table_id, fixture.tablet_id, 12U, {2U, 3U}, 3U};
    const RaftTabletSourceRetirementRequest authority{.group_id = fixture.group_id,
                                                      .table_id = fixture.table_id,
                                                      .tablet_id = fixture.tablet_id,
                                                      .source_node = 1U,
                                                      .completed_movement = std::cref(movement),
                                                      .committed_placement = std::cref(placement)};
    auto decoded = decode_manifest_v2_temporal_exact(retained_manifest.bytes());
    ASSERT_TRUE(decoded.has_value());
    auto built = build_raft_tablet_source_retirement_manifest(*decoded, authority);
    ASSERT_TRUE(built.has_value()) << built.error().to_string();
    const std::span<const TabletSchemaBinding> no_bindings;
    ASSERT_TRUE(storage
                    ->install_temporal_manifest({.encoded_manifest = std::cref(built->manifest),
                                                 .schema_bindings = no_bindings,
                                                 .nonce = nonce(5U),
                                                 .decode_limits = {},
                                                 .part_validation_limits = {},
                                                 .source_retirement = &authority})
                    .has_value());
    auto successor_loaded =
        storage->load_selected_temporal_manifest({.expected_database_id = fixture.database_id,
                                                  .schema_bindings = no_bindings,
                                                  .source_bindings = {},
                                                  .decode_limits = {},
                                                  .part_validation_limits = {}});
    ASSERT_TRUE(successor_loaded.has_value()) << successor_loaded.error().to_string();
    auto successor =
        std::make_shared<const LoadedTemporalManifestGeneration>(std::move(*successor_loaded));
    {
      auto ordinary = TemporalDatabaseStoragePublisher::create(retained, bindings);
      ASSERT_TRUE(ordinary.has_value());
      EXPECT_EQ(ordinary
                    ->publish_manifest({.selected_manifest = successor,
                                        .schema_bindings = no_bindings,
                                        .decode_limits = {}})
                    .error()
                    .code(),
                common::StatusCode::kInvalidArgument);
      EXPECT_FALSE(ordinary->is_usable());
    }

    {
      auto held_predecessor = publisher->snapshot();
      ASSERT_TRUE(held_predecessor.has_value());
      auto published =
          publisher->publish_source_retirement_manifest({.selected_manifest = successor,
                                                         .schema_bindings = no_bindings,
                                                         .decode_limits = {},
                                                         .source_retirement = &authority});
      ASSERT_TRUE(published.has_value()) << published.error().to_string();
      EXPECT_EQ(published->snapshot.generation(), 3U);
      EXPECT_EQ(publisher->snapshot()->generation(), 3U);
      EXPECT_EQ(published->retirement.predecessor_generation(), 2U);
      ASSERT_EQ(published->retirement.parts().size(), 1U);
      EXPECT_EQ(published->retirement.parts().front(), fixture.descriptor);
      EXPECT_TRUE(published->retirement.is_pinned());
      retirement.emplace(std::move(published->retirement));
      retained.reset();
    }
    EXPECT_TRUE(retirement->is_pinned());
  }
  EXPECT_FALSE(retirement->is_pinned());
  EXPECT_TRUE(std::filesystem::exists(directory.path() / kPartsDirectoryName /
                                      part_file_name(fixture.descriptor.part_id)));
  EXPECT_TRUE(publisher->is_usable());
}

} // namespace
} // namespace chronos::manifest
