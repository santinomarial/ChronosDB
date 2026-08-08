#include "chronos/manifest/compaction_coordinator.hpp"
#include "chronos/manifest/naming.hpp"
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
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

using cseg::test::identifier;

[[nodiscard]] common::Uuid nonce(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-compaction-coordinator-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
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

struct CoordinatorFixture {
  CoordinatorFixture()
      : encoded(cseg::test::make_valid_part(cseg::PageCompression::kNone)), schema(make_schema()),
        lineage(schema::SchemaLineage::create(schema).value()),
        descriptor{.part_id = part_id,
                   .table_id = table_id,
                   .tablet_id = tablet_id,
                   .schema_id = schema_id,
                   .schema_version = schema::SchemaVersion::initial(),
                   .file_length = encoded.size(),
                   .row_count = 2U,
                   .minimum_record_sequence = 7U,
                   .maximum_record_sequence = 7U,
                   .minimum_event_time = -5,
                   .maximum_event_time = 10} {
    wal_id.bytes = identifier<schema::SchemaId>(0x70U).bytes();
    EXPECT_TRUE(std::filesystem::create_directory(directory.path() / kPartsDirectoryName));
    EXPECT_TRUE(std::filesystem::create_directory(directory.path() / kManifestDirectoryName));
    write_bytes(directory.path() / kManifestDirectoryName / std::string{kManifestLockFileName}, {});
    const std::array tablets{
        TabletDescriptor{.table_id = table_id,
                         .tablet_id = tablet_id,
                         .recovery_schema_id = schema_id,
                         .recovery_schema_version = schema::SchemaVersion::initial(),
                         .durable_record_sequence = 7U,
                         .first_part_index = 0U,
                         .part_count = 1U,
                         .durable_row_count = 2U}};
    const std::array parts{descriptor};
    generation_one = std::make_unique<EncodedManifest>(
        encode_manifest_v1({.generation = 1U,
                            .database_id = database_id,
                            .wal_id = wal_id,
                            .reclaim_checkpoint = {.record_sequence = 0U,
                                                   .segment_number = 1U,
                                                   .byte_offset = 64U},
                            .tablets = tablets,
                            .parts = parts,
                            .retries = {}})
            .value());
    write_bytes(directory.path() / kPartsDirectoryName / part_file_name(part_id), encoded.bytes());
    write_bytes(directory.path() / kManifestDirectoryName / *manifest_file_name(1U),
                generation_one->bytes());
    storage = std::make_unique<ManifestStorage>(
        ManifestStorage::open_existing({.database_root = directory.path().string()}).value());
    const auto schema_bindings = bindings();
    selected_one = std::make_shared<const LoadedManifestGeneration>(
        storage
            ->load_selected_manifest({.expected_database_id = database_id,
                                      .expected_wal_id = wal_id,
                                      .schema_bindings = schema_bindings,
                                      .decode_limits = {},
                                      .part_validation_limits = {}})
            .value());
    publisher = std::make_unique<DatabaseStoragePublisher>(
        DatabaseStoragePublisher::create(selected_one, {}).value());
  }

  [[nodiscard]] schema::TableSchema make_schema() const {
    const schema::ColumnId event_id = identifier<schema::ColumnId>(5U);
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
                                        .deduplication_key = {}})
        .value();
  }

  [[nodiscard]] std::array<TabletSchemaBinding, 1> bindings() const {
    return {TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  }

  schema::TableId table_id{identifier<schema::TableId>(2U)};
  schema::TabletId tablet_id{identifier<schema::TabletId>(3U)};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(4U)};
  cseg::PartId part_id{identifier<cseg::PartId>(1U)};
  DatabaseId database_id{identifier<DatabaseId>(6U)};
  wal::WalId wal_id{};
  TemporaryDirectory directory;
  cseg::EncodedCsegPart encoded;
  schema::TableSchema schema;
  schema::SchemaLineage lineage;
  PartDescriptor descriptor;
  std::unique_ptr<EncodedManifest> generation_one;
  std::unique_ptr<ManifestStorage> storage;
  std::shared_ptr<const LoadedManifestGeneration> selected_one;
  std::unique_ptr<DatabaseStoragePublisher> publisher;
};

[[nodiscard]] AppendOnlyCompactionOperation
operation(const CoordinatorFixture& fixture, const std::span<const cseg::PartId> inputs,
          const std::span<const TabletSchemaBinding> bindings, const cseg::PartId& output) {
  return {.tablet_id = fixture.tablet_id,
          .input_part_ids = inputs,
          .output_part_id = output,
          .part_nonce = nonce(0xa0U),
          .manifest_nonce = nonce(0xb0U),
          .compression = cseg::PageCompression::kZstd,
          .schema_bindings = bindings,
          .manifest_decode_limits = {},
          .part_validation_limits = {},
          .compaction_limits = {}};
}

TEST(AppendOnlyCompactionCoordinatorTest, InstallsAndPublishesWhileRetainingInputFinal) {
  CoordinatorFixture fixture;
  AppendOnlyCompactionCoordinator coordinator =
      AppendOnlyCompactionCoordinator::create(*fixture.storage, *fixture.publisher).value();
  const DatabaseStorageSnapshot old = fixture.publisher->snapshot().value();
  const std::array inputs{fixture.part_id};
  const auto bindings = fixture.bindings();
  const cseg::PartId output = identifier<cseg::PartId>(9U);
  const common::Result<AppendOnlyCompactionCompletion> completed =
      coordinator.compact(operation(fixture, inputs, bindings, output));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  EXPECT_EQ(*completed, (AppendOnlyCompactionCompletion{.output_part_id = output,
                                                        .manifest_generation = 2U,
                                                        .row_count = 2U,
                                                        .resumed_durable_manifest = false}));
  const DatabaseStorageSnapshot next = fixture.publisher->snapshot().value();
  EXPECT_EQ(old.generation(), 1U);
  EXPECT_EQ(old.parts().front(), fixture.descriptor);
  EXPECT_EQ(next.generation(), 2U);
  EXPECT_EQ(next.parts().front().part_id, output);
  EXPECT_TRUE(std::filesystem::exists(fixture.directory.path() / kPartsDirectoryName /
                                      part_file_name(fixture.part_id)));
  EXPECT_TRUE(std::filesystem::exists(fixture.directory.path() / kPartsDirectoryName /
                                      part_file_name(output)));
  EXPECT_EQ(coordinator.metrics().completed, 1U);
  EXPECT_EQ(coordinator.metrics().input_parts, 1U);
  EXPECT_EQ(coordinator.metrics().compacted_rows, 2U);
  EXPECT_GT(coordinator.metrics().output_bytes, 0U);
}

TEST(AppendOnlyCompactionCoordinatorTest, ResumesExactDurableButUnpublishedSuccessor) {
  CoordinatorFixture fixture;
  const std::array inputs{fixture.part_id};
  const auto bindings = fixture.bindings();
  const cseg::PartId output = identifier<cseg::PartId>(9U);
  const std::array input_images{
      CompactionPartImage{.part_id = fixture.part_id, .bytes = fixture.encoded.bytes()}};
  common::Result<EncodedCompactionPart> merged =
      merge_append_only_cseg_v1({.inputs = input_images,
                                 .schema = std::cref(fixture.schema),
                                 .tablet_id = fixture.tablet_id,
                                 .wal_id = fixture.wal_id,
                                 .output_part_id = output,
                                 .limits = {}});
  ASSERT_TRUE(merged.has_value());
  const DecodedManifestView predecessor =
      decode_manifest_v1_exact(fixture.generation_one->bytes()).value();
  common::Result<EncodedManifest> candidate =
      build_manifest_v1_for_append_only_compaction({.predecessor = predecessor,
                                                    .inputs = input_images,
                                                    .output = std::cref(*merged),
                                                    .schema = std::cref(fixture.schema),
                                                    .schema_bindings = bindings,
                                                    .equivalence_limits = {},
                                                    .part_validation_limits = {}});
  ASSERT_TRUE(candidate.has_value());
  ASSERT_TRUE(fixture.storage
                  ->install_part({.encoded_part = std::cref(merged->encoded_part),
                                  .descriptor = merged->descriptor,
                                  .wal_id = merged->wal_id,
                                  .schema = std::cref(fixture.schema),
                                  .nonce = nonce(0xa0U),
                                  .validation_limits = {}})
                  .has_value());
  const std::array outputs{output};
  const ManifestCompactionReplacement replacement{
      .tablet_id = fixture.tablet_id, .input_part_ids = inputs, .output_part_ids = outputs};
  ASSERT_TRUE(fixture.storage
                  ->install_manifest({.encoded_manifest = std::cref(*candidate),
                                      .schema_bindings = bindings,
                                      .nonce = nonce(0xb0U),
                                      .decode_limits = {},
                                      .part_validation_limits = {},
                                      .compaction_replacement = &replacement,
                                      .compaction_equivalence_limits = {}})
                  .has_value());

  AppendOnlyCompactionCoordinator coordinator =
      AppendOnlyCompactionCoordinator::create(*fixture.storage, *fixture.publisher).value();
  const common::Result<AppendOnlyCompactionCompletion> completed =
      coordinator.compact(operation(fixture, inputs, bindings, output));
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  EXPECT_TRUE(completed->resumed_durable_manifest);
  EXPECT_EQ(fixture.publisher->snapshot()->generation(), 2U);
  EXPECT_EQ(coordinator.metrics().resumed_durable_manifests, 1U);
  EXPECT_TRUE(coordinator.is_usable());
}

TEST(AppendOnlyCompactionCoordinatorTest, PremanifestFailureLeavesCoordinatorRetryable) {
  CoordinatorFixture fixture;
  AppendOnlyCompactionCoordinator coordinator =
      AppendOnlyCompactionCoordinator::create(*fixture.storage, *fixture.publisher).value();
  const std::array inputs{fixture.part_id};
  const auto bindings = fixture.bindings();
  const common::Result<AppendOnlyCompactionCompletion> failed =
      coordinator.compact(operation(fixture, inputs, bindings, fixture.part_id));
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(coordinator.is_usable());
  EXPECT_EQ(fixture.publisher->snapshot()->generation(), 1U);
  EXPECT_EQ(coordinator.metrics().failures, 1U);
}

} // namespace
} // namespace chronos::manifest
