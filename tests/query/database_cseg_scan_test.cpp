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

#include <algorithm>
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
  schema::SchemaLineage result =
      schema::SchemaLineage::create(
          schema::TableSchema::create(cseg::test::identifier<schema::TableId>(2U),
                                      cseg::test::identifier<schema::SchemaId>(4U),
                                      schema::SchemaVersion::initial(), std::nullopt, columns,
                                      {.event_time_column = event,
                                       .physical_ordering_key = {event},
                                       .partition_columns = {event},
                                       .shard_key = {event},
                                       .deduplication_key = {}})
              .value())
          .value();
  columns.push_back(
      schema::ColumnDefinition::create(cseg::test::identifier<schema::ColumnId>(7U), "added",
                                       cseg::test::type(schema::LogicalTypeKind::kString), true)
          .value());
  EXPECT_TRUE(result
                  .append(schema::TableSchema::create(cseg::test::identifier<schema::TableId>(2U),
                                                      cseg::test::identifier<schema::SchemaId>(6U),
                                                      schema::SchemaVersion::from_value(2U).value(),
                                                      cseg::test::identifier<schema::SchemaId>(4U),
                                                      std::move(columns),
                                                      {.event_time_column = event,
                                                       .physical_ordering_key = {event},
                                                       .partition_columns = {event},
                                                       .shard_key = {event},
                                                       .deduplication_key = {}})
                              .value())
                  .is_ok());
  return result;
}

struct LoadedSnapshotPart {
  schema::SchemaLineage schemas;
  std::shared_ptr<const manifest::SnapshotPartImage> image;
};

struct MultiPartSnapshot {
  TemporaryDirectory directory;
  schema::SchemaLineage schemas{lineage()};
  std::unique_ptr<manifest::ManifestStorage> storage;
  std::unique_ptr<manifest::DatabaseStoragePublisher> publisher;
  std::unique_ptr<manifest::DatabaseStorageSnapshot> snapshot;
  std::vector<manifest::PartDescriptor> descriptors;
};

[[nodiscard]] std::unique_ptr<MultiPartSnapshot>
load_multi_part_snapshot(const std::uint8_t database_seed = 6U) {
  auto fixture = std::make_unique<MultiPartSnapshot>();
  EXPECT_FALSE(fixture->directory.path().empty());
  EXPECT_TRUE(
      std::filesystem::create_directory(fixture->directory.path() / manifest::kPartsDirectoryName));
  EXPECT_TRUE(std::filesystem::create_directory(fixture->directory.path() /
                                                manifest::kManifestDirectoryName));
  write_bytes(fixture->directory.path() / manifest::kManifestDirectoryName /
                  std::string{manifest::kManifestLockFileName},
              {});

  const std::array options{cseg::test::PartFixtureOptions{
                               .part_id_seed = 1U, .first_event_time = -100, .record_sequence = 1U},
                           cseg::test::PartFixtureOptions{
                               .part_id_seed = 2U, .first_event_time = 0, .record_sequence = 2U},
                           cseg::test::PartFixtureOptions{
                               .part_id_seed = 3U, .first_event_time = 100, .record_sequence = 3U}};
  const schema::TableId table = cseg::test::identifier<schema::TableId>(2U);
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  const schema::SchemaId schema_id = cseg::test::identifier<schema::SchemaId>(4U);
  for (const cseg::test::PartFixtureOptions option : options) {
    cseg::EncodedCsegPart encoded =
        cseg::test::make_valid_part_with_rows(10U, 5U, cseg::PageCompression::kNone, option);
    fixture->descriptors.push_back(
        {.part_id = cseg::test::identifier<cseg::PartId>(option.part_id_seed),
         .table_id = table,
         .tablet_id = tablet,
         .schema_id = schema_id,
         .schema_version = schema::SchemaVersion::initial(),
         .file_length = encoded.size(),
         .row_count = 10U,
         .minimum_record_sequence = option.record_sequence,
         .maximum_record_sequence = option.record_sequence,
         .minimum_event_time = option.first_event_time,
         .maximum_event_time = option.first_event_time + 9});
    write_bytes(fixture->directory.path() / manifest::kPartsDirectoryName /
                    manifest::part_file_name(fixture->descriptors.back().part_id),
                encoded.bytes());
  }

  wal::WalId wal_id{};
  wal_id.bytes = cseg::test::identifier<schema::SchemaId>(0x70U).bytes();
  const manifest::DatabaseId database_id =
      cseg::test::identifier<manifest::DatabaseId>(database_seed);
  const std::array tablets{
      manifest::TabletDescriptor{.table_id = table,
                                 .tablet_id = tablet,
                                 .recovery_schema_id = schema_id,
                                 .recovery_schema_version = schema::SchemaVersion::initial(),
                                 .durable_record_sequence = 3U,
                                 .first_part_index = 0U,
                                 .part_count = fixture->descriptors.size(),
                                 .durable_row_count = 30U}};
  manifest::EncodedManifest encoded_manifest =
      manifest::encode_manifest_v1(
          {.generation = 1U,
           .database_id = database_id,
           .wal_id = wal_id,
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = tablets,
           .parts = fixture->descriptors,
           .retries = {}})
          .value();
  write_bytes(fixture->directory.path() / manifest::kManifestDirectoryName /
                  *manifest::manifest_file_name(1U),
              encoded_manifest.bytes());

  fixture->storage = std::make_unique<manifest::ManifestStorage>(
      manifest::ManifestStorage::open_existing(
          {.database_root = fixture->directory.path().string()})
          .value());
  const std::array bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet, .lineage = std::cref(fixture->schemas)}};
  auto selected = std::make_shared<const manifest::LoadedManifestGeneration>(
      fixture->storage
          ->load_selected_manifest({.expected_database_id = database_id,
                                    .expected_wal_id = wal_id,
                                    .schema_bindings = bindings,
                                    .decode_limits = {},
                                    .part_validation_limits = {}})
          .value());
  fixture->publisher = std::make_unique<manifest::DatabaseStoragePublisher>(
      manifest::DatabaseStoragePublisher::create(selected, {}).value());
  fixture->snapshot =
      std::make_unique<manifest::DatabaseStorageSnapshot>(fixture->publisher->snapshot().value());
  return fixture;
}

[[nodiscard]] cseg::EventTimePredicate point_predicate(const std::int64_t value) {
  return {.lower = cseg::EventTimeBound{.value = value, .inclusive = true},
          .upper = cseg::EventTimeBound{.value = value, .inclusive = true}};
}

void corrupt_part_magic(const MultiPartSnapshot& fixture, const std::size_t descriptor_index) {
  const std::filesystem::path path =
      fixture.directory.path() / manifest::kPartsDirectoryName /
      manifest::part_file_name(fixture.descriptors[descriptor_index].part_id);
  std::fstream stream{path, std::ios::binary | std::ios::in | std::ios::out};
  ASSERT_TRUE(stream.good());
  char first{};
  stream.read(&first, 1);
  ASSERT_TRUE(stream.good());
  first = static_cast<char>(static_cast<unsigned char>(first) ^ 1U);
  stream.seekp(0);
  stream.write(&first, 1);
  ASSERT_TRUE(stream.good());
}

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

[[nodiscard]] std::int64_t int64_cell(const VectorChunk& chunk, const std::size_t row,
                                      const std::size_t column_ordinal = 0U) {
  const common::Result<columnar::ColumnCellView> cell =
      chunk.cell({.column_ordinal = column_ordinal, .selected_row = row});
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

TEST(DatabaseCsegPartScanPlanTest, SelectsCanonicalManifestWorkAndReportsExactMetrics) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  common::Result<SnapshotCsegPartScanPlan> all =
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet);
  ASSERT_TRUE(all.has_value()) << all.error().to_string();
  ASSERT_EQ(all->selected_part_count(), 3U);
  EXPECT_EQ(all->skipped_part_count(), 0U);
  EXPECT_EQ(all->selected_rows(), 30U);
  EXPECT_EQ(all->skipped_rows(), 0U);
  EXPECT_GT(all->retained_configuration_bytes(), sizeof(SnapshotCsegPartScanPlan));
  for (std::size_t index = 0U; index < fixture->descriptors.size(); ++index)
    EXPECT_EQ(all->selected_part_ids()[index], fixture->descriptors[index].part_id);

  common::Result<SnapshotCsegPartScanPlan> middle =
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, point_predicate(7));
  ASSERT_TRUE(middle.has_value()) << middle.error().to_string();
  ASSERT_EQ(middle->selected_part_count(), 1U);
  EXPECT_EQ(middle->selected_part_ids().front(), fixture->descriptors[1].part_id);
  EXPECT_EQ(middle->skipped_part_count(), 2U);
  EXPECT_EQ(middle->selected_rows(), 10U);
  EXPECT_EQ(middle->skipped_rows(), 20U);
  EXPECT_EQ(middle->event_time_predicate(), point_predicate(7));

  const cseg::EventTimePredicate empty{.lower = cseg::EventTimeBound{.value = 8, .inclusive = true},
                                       .upper =
                                           cseg::EventTimeBound{.value = 7, .inclusive = true}};
  common::Result<SnapshotCsegPartScanPlan> none =
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, empty);
  ASSERT_TRUE(none.has_value()) << none.error().to_string();
  EXPECT_EQ(none->selected_part_count(), 0U);
  EXPECT_EQ(none->skipped_part_count(), 3U);
  EXPECT_EQ(none->selected_rows(), 0U);
  EXPECT_EQ(none->skipped_rows(), 30U);
}

TEST(DatabaseCsegPartScanPlanTest, RejectsUnknownTabletsAndEveryFinitePlanLimit) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  EXPECT_EQ(plan_snapshot_cseg_part_scan(*fixture->snapshot,
                                         cseg::test::identifier<schema::TabletId>(0xeeU))
                .error()
                .code(),
            common::StatusCode::kNotFound);
  EXPECT_EQ(
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, std::nullopt,
                                   {.maximum_parts = 2U,
                                    .maximum_selected_parts = kDefaultSnapshotCsegPartScanPartLimit,
                                    .maximum_retained_configuration_bytes =
                                        kDefaultSnapshotCsegPartScanConfigurationByteLimit})
          .error()
          .code(),
      common::StatusCode::kResourceExhausted);
  EXPECT_EQ(plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, std::nullopt,
                                         {.maximum_parts = 3U,
                                          .maximum_selected_parts = 2U,
                                          .maximum_retained_configuration_bytes =
                                              kDefaultSnapshotCsegPartScanConfigurationByteLimit})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, std::nullopt,
                                         {.maximum_parts = 3U,
                                          .maximum_selected_parts = 3U,
                                          .maximum_retained_configuration_bytes = 1U})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, std::nullopt,
                                         {.maximum_parts = 0U,
                                          .maximum_selected_parts = 3U,
                                          .maximum_retained_configuration_bytes = 1024U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
}

TEST(DatabaseCsegPartScanPlanPropertyTest, PointSelectionMatchesIndependentPartRangeModel) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  for (std::int64_t point = -120; point <= 120; point += 3) {
    SCOPED_TRACE(point);
    common::Result<SnapshotCsegPartScanPlan> planned =
        plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, point_predicate(point));
    ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
    std::vector<cseg::PartId> expected;
    std::uint64_t expected_rows = 0U;
    for (const manifest::PartDescriptor& descriptor : fixture->descriptors) {
      if (descriptor.minimum_event_time <= point && point <= descriptor.maximum_event_time) {
        expected.push_back(descriptor.part_id);
        expected_rows += descriptor.row_count;
      }
    }
    EXPECT_TRUE(std::ranges::equal(planned->selected_part_ids(), expected));
    EXPECT_EQ(planned->selected_rows(), expected_rows);
    EXPECT_EQ(planned->skipped_rows(), 30U - expected_rows);
  }
}

TEST(DatabaseCsegPartScanTest, PrunesBeforePageWorkThenAppliesExactRowTruth) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  common::Result<SnapshotCsegPartScanPlan> planned =
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, point_predicate(7));
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  corrupt_part_magic(*fixture, 0U);
  corrupt_part_magic(*fixture, 2U);
  common::Result<std::vector<std::shared_ptr<const manifest::SnapshotPartImage>>> loaded =
      load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot, *planned,
                                          fixture->schemas);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  ASSERT_EQ(loaded->size(), 1U);
  EXPECT_EQ(loaded->front()->descriptor().part_id, fixture->descriptors[1].part_id);

  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  common::Result<std::unique_ptr<PhysicalOperator>> created =
      create_snapshot_cseg_part_scan(resources, *planned, *loaded, fixture->schemas,
                                     cseg::test::identifier<schema::SchemaId>(4U), {0U});
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  common::Result<PhysicalOperatorStep> step = (*created)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step->chunk()->chunk().selected_row_count(), 1U);
  EXPECT_EQ(int64_cell(step->chunk()->chunk(), 0U), 7);
  step = (*created)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  EXPECT_EQ(step->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ((*created)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(DatabaseCsegPartScanTest, RemovesAnUnrequestedEventTimeHelperAfterExactFiltering) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  common::Result<SnapshotCsegPartScanPlan> planned =
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, point_predicate(7));
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  auto loaded = load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot, *planned,
                                                    fixture->schemas)
                    .value();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  common::Result<std::unique_ptr<PhysicalOperator>> created =
      create_snapshot_cseg_part_scan(resources, *planned, std::move(loaded), fixture->schemas,
                                     cseg::test::identifier<schema::SchemaId>(6U), {1U});
  ASSERT_TRUE(created.has_value()) << created.error().to_string();

  common::Result<PhysicalOperatorStep> step = (*created)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  EXPECT_EQ(step->chunk()->chunk().column_count(), 1U);
  ASSERT_EQ(step->chunk()->chunk().selected_row_count(), 1U);
  const common::Result<columnar::ColumnCellView> cell =
      step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U});
  ASSERT_TRUE(cell.has_value()) << cell.error().to_string();
  EXPECT_TRUE(cell->is_null());
  step = (*created)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  EXPECT_EQ(step->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(DatabaseCsegPartScanTest, FiltersAtTheRequestedEventTimeOutputPosition) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  common::Result<SnapshotCsegPartScanPlan> planned =
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, point_predicate(7));
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  auto loaded = load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot, *planned,
                                                    fixture->schemas)
                    .value();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  auto created =
      create_snapshot_cseg_part_scan(resources, *planned, std::move(loaded), fixture->schemas,
                                     cseg::test::identifier<schema::SchemaId>(6U), {1U, 0U});
  ASSERT_TRUE(created.has_value()) << created.error().to_string();

  common::Result<PhysicalOperatorStep> step = (*created)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step->chunk()->chunk().column_count(), 2U);
  ASSERT_EQ(step->chunk()->chunk().selected_row_count(), 1U);
  EXPECT_TRUE(step->chunk()->chunk().cell({.column_ordinal = 0U, .selected_row = 0U})->is_null());
  EXPECT_EQ(int64_cell(step->chunk()->chunk(), 0U, 1U), 7);
}

TEST(DatabaseCsegPartScanTest, PreservesOpenAndClosedBoundsThroughPruningAndExactFiltering) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  const cseg::EventTimePredicate predicate{
      .lower = cseg::EventTimeBound{.value = 5, .inclusive = false},
      .upper = cseg::EventTimeBound{.value = 8, .inclusive = true}};
  common::Result<SnapshotCsegPartScanPlan> planned =
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, predicate);
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  auto loaded = load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot, *planned,
                                                    fixture->schemas)
                    .value();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
  auto created =
      create_snapshot_cseg_part_scan(resources, *planned, std::move(loaded), fixture->schemas,
                                     cseg::test::identifier<schema::SchemaId>(4U), {0U});
  ASSERT_TRUE(created.has_value()) << created.error().to_string();

  common::Result<PhysicalOperatorStep> step = (*created)->next(resources);
  ASSERT_TRUE(step.has_value()) << step.error().to_string();
  ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
  ASSERT_EQ(step->chunk()->chunk().selected_row_count(), 3U);
  EXPECT_EQ(int64_cell(step->chunk()->chunk(), 0U), 6);
  EXPECT_EQ(int64_cell(step->chunk()->chunk(), 1U), 7);
  EXPECT_EQ(int64_cell(step->chunk()->chunk(), 2U), 8);
}

TEST(DatabaseCsegPartScanTest, RejectsARequiredHelperBeyondEitherProjectionLimit) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  common::Result<SnapshotCsegPartScanPlan> planned =
      plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, point_predicate(7));
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  const auto loaded = load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot,
                                                          *planned, fixture->schemas)
                          .value();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();

  CsegScanLimits reader_limited;
  reader_limited.reader.max_projected_columns = 1U;
  EXPECT_EQ(create_snapshot_cseg_part_scan(resources, *planned, loaded, fixture->schemas,
                                           cseg::test::identifier<schema::SchemaId>(6U), {1U},
                                           reader_limited)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  CsegScanLimits chunk_limited;
  chunk_limited.chunk.maximum_columns = 1U;
  EXPECT_EQ(create_snapshot_cseg_part_scan(resources, *planned, loaded, fixture->schemas,
                                           cseg::test::identifier<schema::SchemaId>(6U), {1U},
                                           chunk_limited)
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(DatabaseCsegPartScanPropertyTest, ExactPointResultsMatchTheIndependentRowModel) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const schema::TabletId tablet = cseg::test::identifier<schema::TabletId>(3U);
  for (std::int64_t point = -120; point <= 120; point += 3) {
    SCOPED_TRACE(point);
    common::Result<SnapshotCsegPartScanPlan> planned =
        plan_snapshot_cseg_part_scan(*fixture->snapshot, tablet, point_predicate(point));
    ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
    auto loaded = load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot,
                                                      *planned, fixture->schemas)
                      .value();
    QueryResourceContext resources =
        QueryResourceContext::create(std::size_t{32U} * 1024U * 1024U).value();
    auto created =
        create_snapshot_cseg_part_scan(resources, *planned, std::move(loaded), fixture->schemas,
                                       cseg::test::identifier<schema::SchemaId>(4U), {0U});
    ASSERT_TRUE(created.has_value()) << created.error().to_string();

    std::size_t matches = 0U;
    while (true) {
      common::Result<PhysicalOperatorStep> step = (*created)->next(resources);
      ASSERT_TRUE(step.has_value()) << step.error().to_string();
      if (step->kind() == PhysicalOperatorStepKind::kEnd)
        break;
      for (std::size_t row = 0U; row < step->chunk()->chunk().selected_row_count(); ++row) {
        EXPECT_EQ(int64_cell(step->chunk()->chunk(), row), point);
        ++matches;
      }
    }
    const bool present = (-100 <= point && point <= -91) || (0 <= point && point <= 9) ||
                         (100 <= point && point <= 109);
    EXPECT_EQ(matches, present ? 1U : 0U);
    EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  }
}

TEST(DatabaseCsegPartScanTest, EmitsEverySelectedPartInCanonicalPhysicalOrder) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  common::Result<SnapshotCsegPartScanPlan> planned = plan_snapshot_cseg_part_scan(
      *fixture->snapshot, cseg::test::identifier<schema::TabletId>(3U));
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  common::Result<std::vector<std::shared_ptr<const manifest::SnapshotPartImage>>> loaded =
      load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot, *planned,
                                          fixture->schemas);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{64U} * 1024U * 1024U).value();
  common::Result<std::unique_ptr<PhysicalOperator>> created =
      create_snapshot_cseg_part_scan(resources, *planned, *loaded, fixture->schemas,
                                     cseg::test::identifier<schema::SchemaId>(4U), {0U});
  ASSERT_TRUE(created.has_value()) << created.error().to_string();

  const std::array first_values{-100, -95, 0, 5, 100, 105};
  for (const int first : first_values) {
    common::Result<PhysicalOperatorStep> step = (*created)->next(resources);
    ASSERT_TRUE(step.has_value()) << step.error().to_string();
    ASSERT_EQ(step->kind(), PhysicalOperatorStepKind::kChunk);
    ASSERT_EQ(step->chunk()->chunk().selected_row_count(), 5U);
    for (std::size_t row = 0U; row < 5U; ++row)
      EXPECT_EQ(int64_cell(step->chunk()->chunk(), row),
                static_cast<std::int64_t>(first) + static_cast<std::int64_t>(row));
  }
  EXPECT_EQ((*created)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(DatabaseCsegPartScanTest, EmptySelectionAvoidsIoButStillValidatesItsRequest) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  const cseg::EventTimePredicate empty{.lower = cseg::EventTimeBound{.value = 1, .inclusive = true},
                                       .upper =
                                           cseg::EventTimeBound{.value = 0, .inclusive = true}};
  common::Result<SnapshotCsegPartScanPlan> planned = plan_snapshot_cseg_part_scan(
      *fixture->snapshot, cseg::test::identifier<schema::TabletId>(3U), empty);
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  for (std::size_t index = 0U; index < fixture->descriptors.size(); ++index)
    corrupt_part_magic(*fixture, index);
  common::Result<std::vector<std::shared_ptr<const manifest::SnapshotPartImage>>> loaded =
      load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot, *planned,
                                          fixture->schemas);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_TRUE(loaded->empty());

  QueryResourceContext resources = QueryResourceContext::create(std::size_t{1024U} * 1024U).value();
  auto invalid_projection =
      create_snapshot_cseg_part_scan(resources, *planned, {}, fixture->schemas,
                                     cseg::test::identifier<schema::SchemaId>(4U), {1U});
  ASSERT_FALSE(invalid_projection.has_value());
  EXPECT_EQ(invalid_projection.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
  auto created = create_snapshot_cseg_part_scan(resources, *planned, {}, fixture->schemas,
                                                cseg::test::identifier<schema::SchemaId>(4U), {0U});
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_GT(resources.reserved_memory_bytes(), 0U);
  EXPECT_EQ((*created)->next(resources)->kind(), PhysicalOperatorStepKind::kEnd);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(DatabaseCsegPartScanTest, RejectsIncompleteReorderedAndForeignImagesBeforeReservation) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  common::Result<SnapshotCsegPartScanPlan> planned = plan_snapshot_cseg_part_scan(
      *fixture->snapshot, cseg::test::identifier<schema::TabletId>(3U));
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  auto loaded = load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot, *planned,
                                                    fixture->schemas)
                    .value();
  QueryResourceContext resources =
      QueryResourceContext::create(std::size_t{64U} * 1024U * 1024U).value();
  auto incomplete = loaded;
  incomplete.pop_back();
  EXPECT_EQ(create_snapshot_cseg_part_scan(resources, *planned, std::move(incomplete),
                                           fixture->schemas,
                                           cseg::test::identifier<schema::SchemaId>(4U), {0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto reordered = loaded;
  std::reverse(reordered.begin(), reordered.end());
  EXPECT_EQ(create_snapshot_cseg_part_scan(resources, *planned, std::move(reordered),
                                           fixture->schemas,
                                           cseg::test::identifier<schema::SchemaId>(4U), {0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  auto null_image = loaded;
  null_image[1].reset();
  EXPECT_EQ(create_snapshot_cseg_part_scan(resources, *planned, std::move(null_image),
                                           fixture->schemas,
                                           cseg::test::identifier<schema::SchemaId>(4U), {0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);

  const std::unique_ptr<MultiPartSnapshot> foreign = load_multi_part_snapshot(0x44U);
  auto foreign_plan = plan_snapshot_cseg_part_scan(
      *foreign->snapshot, cseg::test::identifier<schema::TabletId>(3U), point_predicate(7));
  ASSERT_TRUE(foreign_plan.has_value()) << foreign_plan.error().to_string();
  auto foreign_images = load_snapshot_cseg_part_scan_images(*foreign->storage, *foreign->snapshot,
                                                            *foreign_plan, foreign->schemas)
                            .value();
  foreign_images.front() = loaded[1];
  EXPECT_EQ(create_snapshot_cseg_part_scan(resources, *foreign_plan, std::move(foreign_images),
                                           foreign->schemas,
                                           cseg::test::identifier<schema::SchemaId>(4U), {0U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(load_snapshot_cseg_part_scan_images(*foreign->storage, *foreign->snapshot, *planned,
                                                foreign->schemas)
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(resources.reserved_memory_bytes(), 0U);
}

TEST(DatabaseCsegPartScanTest, ResourceFailureUnwindsParentAndEveryChildReservation) {
  const std::unique_ptr<MultiPartSnapshot> fixture = load_multi_part_snapshot();
  common::Result<SnapshotCsegPartScanPlan> planned = plan_snapshot_cseg_part_scan(
      *fixture->snapshot, cseg::test::identifier<schema::TabletId>(3U));
  ASSERT_TRUE(planned.has_value()) << planned.error().to_string();
  const auto loaded = load_snapshot_cseg_part_scan_images(*fixture->storage, *fixture->snapshot,
                                                          *planned, fixture->schemas)
                          .value();
  QueryResourceContext ample =
      QueryResourceContext::create(std::size_t{64U} * 1024U * 1024U).value();
  auto measured =
      create_snapshot_cseg_part_scan(ample, *planned, loaded, fixture->schemas,
                                     cseg::test::identifier<schema::SchemaId>(4U), {0U});
  ASSERT_TRUE(measured.has_value()) << measured.error().to_string();
  const std::size_t complete_charge = ample.reserved_memory_bytes();
  ASSERT_GT(complete_charge, 1U);
  measured->reset();
  EXPECT_EQ(ample.reserved_memory_bytes(), 0U);

  QueryResourceContext constrained = QueryResourceContext::create(complete_charge - 1U).value();
  auto failed = create_snapshot_cseg_part_scan(constrained, *planned, loaded, fixture->schemas,
                                               cseg::test::identifier<schema::SchemaId>(4U), {0U});
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(constrained.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::query
