#include "chronos/common/byte_reader.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/query/database_cseg_scan.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
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
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-database-cseg-scan-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  ASSERT_TRUE(output.good());
  // std::ofstream has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(output.good());
}

[[nodiscard]] schema::SchemaLineage lineage() {
  const schema::ColumnId event = cseg::test::identifier<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(
      schema::ColumnDefinition::create(
          event, "event_time", cseg::test::type(schema::LogicalTypeKind::kTimestampNs), false)
          .value());
  return schema::SchemaLineage::create(
             schema::TableSchema::create(cseg::test::identifier<schema::TableId>(2U),
                                         cseg::test::identifier<schema::SchemaId>(4U),
                                         schema::SchemaVersion::initial(), std::nullopt,
                                         std::move(columns),
                                         {.event_time_column = event,
                                          .physical_ordering_key = {event},
                                          .partition_columns = {event},
                                          .shard_key = {event},
                                          .deduplication_key = {}})
                 .value())
      .value();
}

struct LoadedSnapshotPart {
  schema::SchemaLineage schemas;
  std::shared_ptr<const manifest::SnapshotPartImage> image;
};

[[nodiscard]] LoadedSnapshotPart load_snapshot_part() {
  TemporaryDirectory directory;
  EXPECT_FALSE(directory.path().empty());
  EXPECT_TRUE(std::filesystem::create_directory(directory.path() / manifest::kPartsDirectoryName));
  EXPECT_TRUE(
      std::filesystem::create_directory(directory.path() / manifest::kManifestDirectoryName));
  write_bytes(directory.path() / manifest::kManifestDirectoryName /
                  std::string{manifest::kManifestLockFileName},
              {});

  cseg::EncodedCsegPart encoded = cseg::test::make_valid_part(cseg::PageCompression::kNone);
  schema::SchemaLineage schemas = lineage();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  wal::WalId wal_id{};
  wal_id.bytes = cseg::test::identifier<schema::SchemaId>(0x70U).bytes();
  const manifest::PartDescriptor descriptor{.part_id = cseg::test::identifier<cseg::PartId>(1U),
                                            .table_id = cseg::test::identifier<schema::TableId>(2U),
                                            .tablet_id = tablet,
                                            .schema_id =
                                                cseg::test::identifier<schema::SchemaId>(4U),
                                            .schema_version = schema::SchemaVersion::initial(),
                                            .file_length = encoded.size(),
                                            .row_count = 2U,
                                            .minimum_record_sequence = 7U,
                                            .maximum_record_sequence = 7U,
                                            .minimum_event_time = -5,
                                            .maximum_event_time = 10};
  const std::array tablets{
      manifest::TabletDescriptor{.table_id = descriptor.table_id,
                                 .tablet_id = tablet,
                                 .recovery_schema_id = descriptor.schema_id,
                                 .recovery_schema_version = descriptor.schema_version,
                                 .durable_record_sequence = 7U,
                                 .first_part_index = 0U,
                                 .part_count = 1U,
                                 .durable_row_count = 2U}};
  const std::array parts{descriptor};
  const manifest::DatabaseId database_id = cseg::test::identifier<manifest::DatabaseId>(6U);
  manifest::EncodedManifest encoded_manifest =
      manifest::encode_manifest_v1(
          {.generation = 1U,
           .database_id = database_id,
           .wal_id = wal_id,
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = tablets,
           .parts = parts,
           .retries = {}})
          .value();
  write_bytes(directory.path() / manifest::kPartsDirectoryName /
                  manifest::part_file_name(descriptor.part_id),
              encoded.bytes());
  write_bytes(directory.path() / manifest::kManifestDirectoryName /
                  *manifest::manifest_file_name(1U),
              encoded_manifest.bytes());

  manifest::ManifestStorage storage =
      manifest::ManifestStorage::open_existing({.database_root = directory.path().string()})
          .value();
  const std::array bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet, .lineage = std::cref(schemas)}};
  auto selected = std::make_shared<const manifest::LoadedManifestGeneration>(
      storage
          .load_selected_manifest({.expected_database_id = database_id,
                                   .expected_wal_id = wal_id,
                                   .schema_bindings = bindings,
                                   .decode_limits = {},
                                   .part_validation_limits = {}})
          .value());
  manifest::DatabaseStoragePublisher publisher =
      manifest::DatabaseStoragePublisher::create(selected, {}).value();
  const manifest::DatabaseStorageSnapshot snapshot = publisher.snapshot().value();
  const std::array part_ids{descriptor.part_id};
  std::vector<manifest::SnapshotPartImage> images =
      storage.load_snapshot_part_images(snapshot, part_ids, bindings, {}).value();
  return {.schemas = std::move(schemas),
          .image = std::make_shared<const manifest::SnapshotPartImage>(std::move(images.front()))};
}

[[nodiscard]] std::int64_t int64_cell(const VectorChunk& chunk, const std::size_t row) {
  const common::Result<columnar::ColumnCellView> cell =
      chunk.cell({.column_ordinal = 0U, .selected_row = row});
  EXPECT_TRUE(cell.has_value());
  const common::Result<common::ByteView> bytes = cell->bytes();
  EXPECT_TRUE(bytes.has_value());
  common::ByteReader reader{*bytes};
  return reader.read_i64_le().value_or(0);
}

TEST(DatabaseCsegScanTest, SnapshotImageAndChunkRetainTheExactEpochIndependently) {
  LoadedSnapshotPart loaded = load_snapshot_part();
  std::weak_ptr<const manifest::SnapshotPartImage> weak = loaded.image;
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  {
    common::Result<CsegPartPin> pin = pin_snapshot_cseg_part(loaded.image);
    ASSERT_TRUE(pin.has_value()) << pin.error().to_string();
    EXPECT_EQ(pin->bytes().size(), loaded.image->bytes().size());
    EXPECT_EQ(pin->retained_buffer_bytes(), loaded.image->retained_buffer_bytes());
  }

  common::Result<std::unique_ptr<PhysicalOperator>> created = create_snapshot_cseg_scan(
      resources, loaded.image, loaded.schemas, cseg::test::identifier<schema::SchemaId>(4U),
      cseg::test::identifier<schema::TabletId>(3U), {0U});
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  std::unique_ptr<PhysicalOperator> source = std::move(*created);
  loaded.image.reset();

  {
    common::Result<PhysicalOperatorStep> step = source->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
    source.reset();
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(int64_cell(step->chunk()->chunk(), 0U), -5);
    EXPECT_EQ(int64_cell(step->chunk()->chunk(), 1U), 10);
  }
  EXPECT_TRUE(weak.expired());
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(DatabaseCsegScanTest, RejectsIdentityMismatchAndSnapshotChargeBeforeDecode) {
  LoadedSnapshotPart loaded = load_snapshot_part();
  QueryResourceContext ample =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  EXPECT_EQ(create_snapshot_cseg_scan(ample, loaded.image, loaded.schemas,
                                      cseg::test::identifier<schema::SchemaId>(4U),
                                      cseg::test::identifier<schema::TabletId>(0xeeU), {0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(ample.reserved_memory_bytes(), 0U);
  EXPECT_EQ(pin_snapshot_cseg_part({}).error().code(), common::StatusCode::kInvalidArgument);

  QueryResourceContext constrained =
      QueryResourceContext::create(loaded.image->retained_buffer_bytes() - 1U).value();
  const auto exhausted = create_snapshot_cseg_scan(
      constrained, loaded.image, loaded.schemas, cseg::test::identifier<schema::SchemaId>(4U),
      cseg::test::identifier<schema::TabletId>(3U), {0U});
  ASSERT_FALSE(exhausted.has_value());
  EXPECT_EQ(exhausted.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(constrained.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::query
