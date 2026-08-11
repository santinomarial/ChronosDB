#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_publication.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/tiering/cold_manifest.hpp"
#include "chronos/tiering/cold_manifest_storage.hpp"
#include "chronos/tiering/object_store.hpp"
#include "chronos/tiering/tiered_part_loader.hpp"
#include "chronos/tiering/tiered_publication.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tiered-part-loader-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;
  TemporaryDirectory(TemporaryDirectory&& other) noexcept : path_(std::move(other.path_)) {
    other.path_.clear();
  }
  TemporaryDirectory& operator=(TemporaryDirectory&& other) noexcept {
    if (this != &other) {
      std::error_code ignored;
      std::filesystem::remove_all(path_, ignored);
      path_ = std::move(other.path_);
      other.path_.clear();
    }
    return *this;
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

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  ASSERT_TRUE(output.good());
  for (const std::byte value : bytes)
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  output.close();
  ASSERT_TRUE(output.good());
}

struct PartFixture {
  schema::TableId table_id{cseg::test::identifier<schema::TableId>(2U)};
  schema::TabletId tablet_id{cseg::test::identifier<schema::TabletId>(3U)};
  schema::SchemaId schema_id{cseg::test::identifier<schema::SchemaId>(4U)};
  common::Uuid group_id{uuid(8U)};
  manifest::DatabaseId database_id{manifest::DatabaseId::from_bytes(uuid(11U).bytes()).value()};
  common::Uuid object_store_id{uuid(12U)};
  schema::TableSchema schema_value{make_schema()};
  schema::SchemaLineage lineage{schema::SchemaLineage::create(schema_value).value()};
  cseg::EncodedCsegPart encoded{cseg::test::make_valid_temporal_part(
      cseg::PageCompression::kZstd,
      {.commit_source = cseg::temporal_format::CommitSource::kRaft, .source_id = group_id})};
  manifest::TemporalPartDescriptor descriptor{
      manifest::describe_manifest_v2_temporal_part_image(
          encoded.bytes(), schema_value, tablet_id, manifest::ManifestCommitSource::kRaft, group_id)
          .value()};
  manifest::TemporalTabletDescriptor owner{
      .table_id = table_id,
      .tablet_id = tablet_id,
      .recovery_schema_id = schema_id,
      .recovery_schema_version = schema::SchemaVersion::initial(),
      .source_id = group_id,
      .durable_position = 9U,
      .reclaim_position = 0U,
      .first_part_index = 0U,
      .part_count = 1U,
      .durable_version_count = 2U,
      .commit_source = manifest::ManifestCommitSource::kRaft,
  };
  std::string object_key{"chronos/parts/part.cseg"};

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

  [[nodiscard]] manifest::EncodedTemporalManifest manifest_image() const {
    const std::array tablets{owner};
    const std::array parts{descriptor};
    return manifest::encode_manifest_v2_temporal({.generation = 1U,
                                                  .database_id = database_id,
                                                  .wal_reclaim_checkpoint = std::nullopt,
                                                  .tablets = tablets,
                                                  .parts = parts,
                                                  .retries = {}})
        .value();
  }

  [[nodiscard]] std::array<manifest::TabletSchemaBinding, 1> bindings() const {
    return {manifest::TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  }

  [[nodiscard]] std::array<manifest::TemporalTabletSourceBinding, 1> source_bindings() const {
    return {manifest::TemporalTabletSourceBinding{
        .tablet_id = tablet_id,
        .commit_source = manifest::ManifestCommitSource::kRaft,
        .source_id = group_id,
    }};
  }

  [[nodiscard]] ColdPartLocationDescriptor location() const {
    return {.part_id = descriptor.part_id,
            .file_length = descriptor.file_length,
            .content_sha256 = descriptor.content_sha256,
            .object_key = object_key};
  }
};

struct LiveFixture {
  TemporaryDirectory directory;
  PartFixture part;
  manifest::ManifestStorage manifest_storage;
  std::unique_ptr<ColdLocationManifestStorage> cold_storage;
  std::shared_ptr<const LoadedColdLocationManifest> cold_owner;
  TieredDatabaseStoragePublisher publisher;
  MemoryObjectStore object_store;

  LiveFixture(TemporaryDirectory directory_value, PartFixture part_value,
              manifest::ManifestStorage storage,
              std::unique_ptr<ColdLocationManifestStorage> cold_storage_value,
              std::shared_ptr<const LoadedColdLocationManifest> cold_owner_value,
              TieredDatabaseStoragePublisher publisher_value) noexcept
      : directory(std::move(directory_value)), part(std::move(part_value)),
        manifest_storage(std::move(storage)), cold_storage(std::move(cold_storage_value)),
        cold_owner(std::move(cold_owner_value)), publisher(std::move(publisher_value)) {}

  [[nodiscard]] static std::unique_ptr<LiveFixture> create() {
    auto fixture = std::unique_ptr<LiveFixture>{};
    TemporaryDirectory directory;
    PartFixture part;
    if (directory.path().empty())
      return nullptr;
    EXPECT_TRUE(
        std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
    EXPECT_TRUE(
        std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
    EXPECT_TRUE(std::filesystem::create_directory(directory.path() / "cold-manifest"));
    write_bytes(
        directory.path() / manifest::kManifestDirectoryName / manifest::kManifestLockFileName, {});
    const auto encoded_manifest = part.manifest_image();
    write_bytes(directory.path() / manifest::kManifestDirectoryName /
                    *manifest::manifest_file_name(1U),
                encoded_manifest.bytes());
    write_bytes(directory.path() / manifest::kPartsDirectoryName /
                    manifest::part_file_name(part.descriptor.part_id),
                part.encoded.bytes());

    auto storage =
        manifest::ManifestStorage::open_existing({.database_root = directory.path().string()});
    EXPECT_TRUE(storage.has_value());
    if (!storage.has_value())
      return nullptr;
    const auto bindings = part.bindings();
    const auto sources = part.source_bindings();
    auto loaded =
        storage->load_selected_temporal_manifest({.expected_database_id = part.database_id,
                                                  .schema_bindings = bindings,
                                                  .source_bindings = sources,
                                                  .decode_limits = {},
                                                  .part_validation_limits = {}});
    EXPECT_TRUE(loaded.has_value());
    if (!loaded.has_value())
      return nullptr;
    auto manifest_owner =
        std::make_shared<const manifest::LoadedTemporalManifestGeneration>(std::move(*loaded));
    auto manifest_publisher =
        manifest::TemporalDatabaseStoragePublisher::create(std::move(manifest_owner), bindings);
    EXPECT_TRUE(manifest_publisher.has_value());
    if (!manifest_publisher.has_value())
      return nullptr;
    auto base = manifest_publisher->snapshot();
    EXPECT_TRUE(base.has_value());
    if (!base.has_value())
      return nullptr;

    auto cold = ColdLocationManifestStorage::create(
        {.directory_path = (directory.path() / "cold-manifest").string(),
         .expected_database_id = part.database_id,
         .expected_object_store_id = part.object_store_id});
    EXPECT_TRUE(cold.has_value());
    if (!cold.has_value())
      return nullptr;
    auto decoded_base = manifest::decode_manifest_v2_temporal_exact(base->manifest_bytes());
    EXPECT_TRUE(decoded_base.has_value());
    if (!decoded_base.has_value())
      return nullptr;
    const std::array locations{part.location()};
    auto encoded_cold = encode_cold_location_manifest_v1({.generation = 1U,
                                                          .base_manifest_generation = 1U,
                                                          .database_id = part.database_id,
                                                          .object_store_id = part.object_store_id,
                                                          .locations = locations});
    EXPECT_TRUE(encoded_cold.has_value());
    if (!encoded_cold.has_value())
      return nullptr;
    EXPECT_TRUE(cold->install(std::cref(*encoded_cold), *decoded_base).has_value());
    auto selected_cold = cold->load_selected(*decoded_base);
    EXPECT_TRUE(selected_cold.has_value() && selected_cold->has_value());
    if (!selected_cold.has_value() || !selected_cold->has_value())
      return nullptr;
    auto cold_owner =
        std::make_shared<const LoadedColdLocationManifest>(std::move(**selected_cold));
    auto tiered_publisher = TieredDatabaseStoragePublisher::create(*base, cold_owner);
    EXPECT_TRUE(tiered_publisher.has_value());
    if (!tiered_publisher.has_value())
      return nullptr;

    fixture = std::unique_ptr<LiveFixture>(
        new LiveFixture(std::move(directory), std::move(part), std::move(*storage),
                        std::make_unique<ColdLocationManifestStorage>(std::move(*cold)), cold_owner,
                        std::move(*tiered_publisher)));
    auto uploaded =
        fixture->object_store.put_if_absent(fixture->part.object_key, fixture->part.encoded.bytes(),
                                            fixture->part.descriptor.content_sha256);
    EXPECT_TRUE(uploaded.has_value());
    return fixture;
  }

  [[nodiscard]] std::filesystem::path local_part_path() const {
    return directory.path() / manifest::kPartsDirectoryName /
           manifest::part_file_name(part.descriptor.part_id);
  }
};

TEST(TieredPartLoaderTest, UsesLocalThenExactPinnedRemoteAndRetainsSnapshot) {
  auto fixture = LiveFixture::create();
  ASSERT_NE(fixture, nullptr);
  const auto bindings = fixture->part.bindings();
  const std::array ids{fixture->part.descriptor.part_id};
  const std::string expected_key = fixture->part.object_key;
  std::weak_ptr<const LoadedColdLocationManifest> cold_lifetime = fixture->cold_owner;
  std::optional<TieredTemporalPartImage> held;
  {
    auto snapshot = fixture->publisher.snapshot();
    ASSERT_TRUE(snapshot.has_value());
    {
      auto local = load_tiered_temporal_part_images(*snapshot, fixture->manifest_storage,
                                                    fixture->object_store, ids, bindings);
      ASSERT_TRUE(local.has_value()) << local.error().to_string();
      ASSERT_EQ(local->size(), 1U);
      EXPECT_EQ(local->front().source(), TieredPartSource::kLocal);
      EXPECT_TRUE(std::ranges::equal(local->front().bytes(), fixture->part.encoded.bytes()));
    }

    ASSERT_TRUE(std::filesystem::remove(fixture->local_part_path()));
    auto remote = load_tiered_temporal_part_images(*snapshot, fixture->manifest_storage,
                                                   fixture->object_store, ids, bindings);
    ASSERT_TRUE(remote.has_value()) << remote.error().to_string();
    ASSERT_EQ(remote->size(), 1U);
    EXPECT_EQ(remote->front().source(), TieredPartSource::kRemote);
    EXPECT_TRUE(std::ranges::equal(remote->front().bytes(), fixture->part.encoded.bytes()));
    held.emplace(std::move(remote->front()));
  }
  fixture.reset();
  ASSERT_TRUE(held.has_value());
  EXPECT_FALSE(cold_lifetime.expired());
  EXPECT_EQ(held->snapshot().manifest_generation(), 1U);
  EXPECT_EQ(held->snapshot().find_cold_location(ids.front())->object_key, expected_key);
  held.reset();
  EXPECT_TRUE(cold_lifetime.expired());
}

TEST(TieredPartLoaderTest, LocalCorruptionNeverFallsBackToValidRemote) {
  auto fixture = LiveFixture::create();
  ASSERT_NE(fixture, nullptr);
  std::vector<std::byte> corrupt(fixture->part.encoded.bytes().begin(),
                                 fixture->part.encoded.bytes().end());
  corrupt.front() ^= std::byte{0xFFU};
  write_bytes(fixture->local_part_path(), corrupt);
  auto snapshot = fixture->publisher.snapshot();
  ASSERT_TRUE(snapshot.has_value());
  const auto bindings = fixture->part.bindings();
  const std::array ids{fixture->part.descriptor.part_id};
  auto loaded = load_tiered_temporal_part_images(*snapshot, fixture->manifest_storage,
                                                 fixture->object_store, ids, bindings);
  ASSERT_FALSE(loaded.has_value());
  EXPECT_EQ(loaded.error().code(), common::StatusCode::kCorruption);
}

TEST(TieredPartLoaderTest, RemoteMetadataMismatchFailsClosed) {
  auto fixture = LiveFixture::create();
  ASSERT_NE(fixture, nullptr);
  ASSERT_TRUE(std::filesystem::remove(fixture->local_part_path()));
  MemoryObjectStore wrong_store;
  const std::array wrong_bytes{std::byte{0x01U}};
  auto wrong_digest = ingest::sha256(wrong_bytes);
  ASSERT_TRUE(wrong_digest.has_value());
  ASSERT_TRUE(
      wrong_store.put_if_absent(fixture->part.object_key, wrong_bytes, *wrong_digest).has_value());
  auto snapshot = fixture->publisher.snapshot();
  ASSERT_TRUE(snapshot.has_value());
  const auto bindings = fixture->part.bindings();
  const std::array ids{fixture->part.descriptor.part_id};
  auto loaded = load_tiered_temporal_part_images(*snapshot, fixture->manifest_storage, wrong_store,
                                                 ids, bindings);
  ASSERT_FALSE(loaded.has_value());
  EXPECT_EQ(loaded.error().code(), common::StatusCode::kCorruption);
}

TEST(TieredPartLoaderTest, RejectsUnsortedOrOverBudgetSelectionBeforeIo) {
  auto fixture = LiveFixture::create();
  ASSERT_NE(fixture, nullptr);
  auto snapshot = fixture->publisher.snapshot();
  ASSERT_TRUE(snapshot.has_value());
  const auto bindings = fixture->part.bindings();
  const std::array duplicate_ids{fixture->part.descriptor.part_id,
                                 fixture->part.descriptor.part_id};
  auto duplicate = load_tiered_temporal_part_images(*snapshot, fixture->manifest_storage,
                                                    fixture->object_store, duplicate_ids, bindings);
  ASSERT_FALSE(duplicate.has_value());
  EXPECT_EQ(duplicate.error().code(), common::StatusCode::kInvalidArgument);

  const std::array one_id{fixture->part.descriptor.part_id};
  auto limited = load_tiered_temporal_part_images(
      *snapshot, fixture->manifest_storage, fixture->object_store, one_id, bindings,
      {.maximum_parts = 1U, .maximum_total_bytes = fixture->part.descriptor.file_length - 1U});
  ASSERT_FALSE(limited.has_value());
  EXPECT_EQ(limited.error().code(), common::StatusCode::kResourceExhausted);
}

} // namespace
} // namespace chronos::tiering
