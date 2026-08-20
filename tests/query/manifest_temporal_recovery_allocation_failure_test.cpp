#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/query/temporal_recovery.hpp"
#include "chronos/wal/application.hpp"
#include "columnar/columnar_test_support.hpp"
#include "cseg/cseg_test_fixture.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-manifest-temporal-allocation-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return !path_.empty();
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

template <typename Identifier> [[nodiscard]] Identifier first_byte_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

void establish_manifest_layout(const std::filesystem::path& root) {
  ASSERT_TRUE(std::filesystem::create_directories(root / manifest::kPartsDirectoryName));
  ASSERT_TRUE(std::filesystem::create_directories(root / manifest::kManifestDirectoryName));
  std::ofstream lock{root / manifest::kManifestDirectoryName /
                     std::string{manifest::kManifestLockFileName}};
  ASSERT_TRUE(lock.good());
}

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  ASSERT_TRUE(output.good());
  for (const std::byte value : bytes) {
    output.put(static_cast<char>(std::to_integer<unsigned char>(value)));
  }
  ASSERT_TRUE(output.good());
}

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] EncodedTemporalCommand command(const columnar::OwnedColumnarBatch& batch) {
  const std::vector<TemporalMutationDescriptor> descriptors{
      {{std::byte{1U}}, 100, 110, TemporalMutationKind::kOriginal},
      {{std::byte{2U}}, 200, 220, TemporalMutationKind::kOriginal}};
  return encode_temporal_command_v1(batch, descriptors, 1000).value();
}

template <typename Integer>
void append_little_endian(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> temporal_storage_schema() {
  const schema::ColumnId event_id = first_byte_id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_id, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(
          first_byte_id<schema::TableId>(2U), first_byte_id<schema::SchemaId>(4U),
          schema::SchemaVersion::initial(), std::nullopt, std::move(columns),
          {.event_time_column = event_id,
           .physical_ordering_key = {event_id},
           .partition_columns = {event_id},
           .shard_key = {event_id},
           .deduplication_key = {event_id}})
          .value());
}

[[nodiscard]] EncodedTemporalCommand
correction_command(const std::shared_ptr<const schema::TableSchema>& retained) {
  std::vector<std::byte> values;
  append_little_endian(values, std::int64_t{30});
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::OwnedColumnVector::create(
                        {.column_id = retained->event_time_column(),
                         .type = retained->columns().front().type(),
                         .nullable = false,
                         .row_count = 1U,
                         .null_count = 0U},
                        {.validity = {}, .offsets = {}, .values = std::move(values)})
                        .value());
  const auto batch = columnar::OwnedColumnarBatch::create(retained, std::move(columns));
  return encode_temporal_command_v1(
             *batch,
             {TemporalMutationDescriptor{.logical_identity = {std::byte{'a'}},
                                         .event_time_ns = 30,
                                         .receive_time_ns = 300,
                                         .kind = TemporalMutationKind::kCorrection}},
             300)
      .value();
}

TEST(ManifestTemporalRecoveryAllocationFailureTest,
     UnwindsEveryCanonicalStartupAllocationAndReleasesBothLocks) {
  TemporaryDirectory directory;
  ASSERT_TRUE(directory.valid());
  const std::filesystem::path database_root = directory.path() / "database";
  const std::filesystem::path wal_root = directory.path() / "wal";
  establish_manifest_layout(database_root);
  ASSERT_TRUE(std::filesystem::create_directory(wal_root));

  const std::shared_ptr<const schema::TableSchema> retained = columnar::test::batch_schema();
  const columnar::OwnedColumnarBatch batch =
      columnar::OwnedColumnarBatch::create(retained, columnar::test::batch_columns()).value();
  const EncodedTemporalCommand suffix = command(batch);
  const auto covered_payload = wal::encode_application_payload({.application_format = 99U,
                                                                .application_kind = 99U,
                                                                .application_flags = 0U,
                                                                .application_body = {}});
  ASSERT_TRUE(covered_payload.has_value()) << covered_payload.error().to_string();

  const wal::WalWriterConfig writer_config{.directory_path = wal_root.string()};
  auto created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::WalId wal_id = writer.wal_id();
  const auto checkpoint_append = writer.append_application_entry(covered_payload->bytes());
  ASSERT_TRUE(checkpoint_append.has_value()) << checkpoint_append.error().to_string();
  ASSERT_EQ(checkpoint_append->record_sequence, 1U);
  const auto suffix_append = writer.append_application_entry(suffix.bytes());
  ASSERT_TRUE(suffix_append.has_value()) << suffix_append.error().to_string();
  ASSERT_EQ(suffix_append->record_sequence, 2U);
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const schema::TabletId tablet_id = first_byte_id<schema::TabletId>(3U);
  const manifest::DatabaseId database_id = first_byte_id<manifest::DatabaseId>(6U);
  const common::Uuid source_id{wal_id.bytes};
  const std::array tablets{manifest::TemporalTabletDescriptor{
      .table_id = retained->table_id(),
      .tablet_id = tablet_id,
      .recovery_schema_id = retained->schema_id(),
      .recovery_schema_version = retained->version(),
      .source_id = source_id,
      .durable_position = 1U,
      .reclaim_position = 0U,
      .first_part_index = 0U,
      .part_count = 0U,
      .durable_version_count = 0U,
      .commit_source = cseg::temporal_format::CommitSource::kWal}};
  const auto encoded_manifest = manifest::encode_manifest_v2_temporal(
      {.generation = 1U,
       .database_id = database_id,
       .wal_reclaim_checkpoint =
           manifest::TemporalWalReclaimCheckpoint{
               .wal_id = wal_id,
               .coordinate = {.record_sequence = checkpoint_append->record_sequence,
                              .segment_number = checkpoint_append->record_end.segment_number,
                              .byte_offset = checkpoint_append->record_end.byte_offset}},
       .tablets = tablets,
       .parts = {},
       .retries = {}});
  ASSERT_TRUE(encoded_manifest.has_value()) << encoded_manifest.error().to_string();
  write_bytes(database_root / manifest::kManifestDirectoryName / *manifest::manifest_file_name(1U),
              encoded_manifest->bytes());

  const schema::SchemaLineage lineage = schema::SchemaLineage::create(*retained).value();
  const std::array schema_bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  const std::array source_bindings{manifest::TemporalTabletSourceBinding{
      .tablet_id = tablet_id,
      .commit_source = cseg::temporal_format::CommitSource::kWal,
      .source_id = source_id}};
  const auto startup_config = [&] {
    return TemporalManifestWalStartupConfig{
        .manifest_storage = {.database_root = database_root.string()},
        .manifest_load = {.expected_database_id = database_id,
                          .schema_bindings = schema_bindings,
                          .source_bindings = source_bindings,
                          .decode_limits = {},
                          .part_validation_limits = {}},
        .wal_writer = writer_config,
        .wal_recovery = {},
        .retained_system_time_ns = std::nullopt,
        .store_limits = {},
        .cseg_limits = {},
        .command_limits = {},
        .reclaim_checkpointed_wal_segments = false};
  };

  bool succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 2048U; ++fail_after) {
    TemporalManifestWalStartupConfig config = startup_config();
    auto recovered =
        run_failure(fail_after, [&] { return recover_manifest_temporal_wal(std::move(config)); });
    if (!recovered.has_value()) {
      ASSERT_EQ(recovered.error().code(), common::StatusCode::kResourceExhausted)
          << "allocation " << fail_after << ": " << recovered.error().to_string();

      // A normal composed recovery immediately after every injected failure proves that neither
      // the Manifest writer lock nor the WAL writer lock escaped the failed unpublished owner.
      auto lock_probe = recover_manifest_temporal_wal(startup_config());
      ASSERT_TRUE(lock_probe.has_value())
          << "allocation " << fail_after << ": " << lock_probe.error().to_string();
      auto probe_writer = lock_probe->release_writer();
      ASSERT_TRUE(probe_writer.has_value()) << probe_writer.error().to_string();
      ASSERT_TRUE(probe_writer->close().is_ok());
      continue;
    }

    EXPECT_EQ(recovered->report().selected_generation, 1U);
    EXPECT_EQ(recovered->report().checkpoint.record_sequence, 1U);
    EXPECT_EQ(recovered->report().verified_covered_command_count, 0U);
    EXPECT_EQ(recovered->report().applied_suffix_command_count, 1U);
    EXPECT_EQ(recovered->report().part_count, 0U);
    EXPECT_EQ(recovered->report().durable_version_count, 0U);
    EXPECT_EQ(recovered->table_count(), 1U);
    const TemporalSnapshotProvider* const provider = recovered->provider(retained->table_id());
    ASSERT_NE(provider, nullptr);
    EXPECT_EQ(provider->latest_commit_position(), 2U);
    EXPECT_EQ(provider->logical_row_count(), 2U);
    EXPECT_EQ(provider->version_count(), 2U);
    auto reopened = recovered->release_writer();
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_TRUE(reopened->close().is_ok());
    succeeded = true;
    break;
  }
  EXPECT_TRUE(succeeded);
}

TEST(ManifestTemporalRecoveryAllocationFailureTest,
     UnwindsEveryNonemptyCsegRestoreAllocationAndReleasesBothLocks) {
  TemporaryDirectory directory;
  ASSERT_TRUE(directory.valid());
  const std::filesystem::path database_root = directory.path() / "database";
  const std::filesystem::path wal_root = directory.path() / "wal";
  establish_manifest_layout(database_root);
  ASSERT_TRUE(std::filesystem::create_directory(wal_root));

  const std::shared_ptr<const schema::TableSchema> retained = temporal_storage_schema();
  const EncodedTemporalCommand suffix = correction_command(retained);
  const auto covered_payload = wal::encode_application_payload({.application_format = 99U,
                                                                .application_kind = 99U,
                                                                .application_flags = 0U,
                                                                .application_body = {}});
  ASSERT_TRUE(covered_payload.has_value()) << covered_payload.error().to_string();
  const wal::WalWriterConfig writer_config{.directory_path = wal_root.string()};
  auto created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::WalId wal_id = writer.wal_id();
  wal::WalAppendResult checkpoint_append{};
  for (std::uint64_t sequence = 1U; sequence <= 9U; ++sequence) {
    const auto appended = writer.append_application_entry(covered_payload->bytes());
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    ASSERT_EQ(appended->record_sequence, sequence);
    checkpoint_append = *appended;
  }
  const auto suffix_append = writer.append_application_entry(suffix.bytes());
  ASSERT_TRUE(suffix_append.has_value()) << suffix_append.error().to_string();
  ASSERT_EQ(suffix_append->record_sequence, 10U);
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const common::Uuid source_id{wal_id.bytes};
  const cseg::EncodedCsegPart encoded =
      cseg::test::make_valid_temporal_part(cseg::PageCompression::kNone, {.source_id = source_id});
  const schema::TabletId tablet_id = first_byte_id<schema::TabletId>(3U);
  const auto part = manifest::describe_manifest_v2_temporal_part_image(
      encoded.bytes(), *retained, tablet_id, cseg::temporal_format::CommitSource::kWal, source_id);
  ASSERT_TRUE(part.has_value()) << part.error().to_string();
  const std::array tablets{manifest::TemporalTabletDescriptor{
      .table_id = retained->table_id(),
      .tablet_id = tablet_id,
      .recovery_schema_id = retained->schema_id(),
      .recovery_schema_version = retained->version(),
      .source_id = source_id,
      .durable_position = 9U,
      .reclaim_position = 0U,
      .first_part_index = 0U,
      .part_count = 1U,
      .durable_version_count = part->row_count,
      .commit_source = cseg::temporal_format::CommitSource::kWal}};
  const std::array parts{*part};
  const manifest::DatabaseId database_id = first_byte_id<manifest::DatabaseId>(6U);
  const auto encoded_manifest = manifest::encode_manifest_v2_temporal(
      {.generation = 1U,
       .database_id = database_id,
       .wal_reclaim_checkpoint =
           manifest::TemporalWalReclaimCheckpoint{
               .wal_id = wal_id,
               .coordinate = {.record_sequence = checkpoint_append.record_sequence,
                              .segment_number = checkpoint_append.record_end.segment_number,
                              .byte_offset = checkpoint_append.record_end.byte_offset}},
       .tablets = tablets,
       .parts = parts,
       .retries = {}});
  ASSERT_TRUE(encoded_manifest.has_value()) << encoded_manifest.error().to_string();
  write_bytes(database_root / manifest::kPartsDirectoryName /
                  manifest::part_file_name(part->part_id),
              encoded.bytes());
  write_bytes(database_root / manifest::kManifestDirectoryName / *manifest::manifest_file_name(1U),
              encoded_manifest->bytes());

  const schema::SchemaLineage lineage = schema::SchemaLineage::create(*retained).value();
  const std::array schema_bindings{
      manifest::TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  const std::array source_bindings{manifest::TemporalTabletSourceBinding{
      .tablet_id = tablet_id,
      .commit_source = cseg::temporal_format::CommitSource::kWal,
      .source_id = source_id}};
  const auto startup_config = [&] {
    return TemporalManifestWalStartupConfig{
        .manifest_storage = {.database_root = database_root.string()},
        .manifest_load = {.expected_database_id = database_id,
                          .schema_bindings = schema_bindings,
                          .source_bindings = source_bindings,
                          .decode_limits = {},
                          .part_validation_limits = {}},
        .wal_writer = writer_config,
        .wal_recovery = {},
        .retained_system_time_ns = part->minimum_system_time,
        .store_limits = {},
        .cseg_limits = {},
        .command_limits = {},
        .reclaim_checkpointed_wal_segments = false};
  };

  bool succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 2048U; ++fail_after) {
    TemporalManifestWalStartupConfig config = startup_config();
    auto recovered =
        run_failure(fail_after, [&] { return recover_manifest_temporal_wal(std::move(config)); });
    if (!recovered.has_value()) {
      ASSERT_EQ(recovered.error().code(), common::StatusCode::kResourceExhausted)
          << "allocation " << fail_after << ": " << recovered.error().to_string();
      auto lock_probe = recover_manifest_temporal_wal(startup_config());
      ASSERT_TRUE(lock_probe.has_value())
          << "allocation " << fail_after << ": " << lock_probe.error().to_string();
      auto probe_writer = lock_probe->release_writer();
      ASSERT_TRUE(probe_writer.has_value()) << probe_writer.error().to_string();
      ASSERT_TRUE(probe_writer->close().is_ok());
      continue;
    }

    EXPECT_EQ(recovered->report().selected_generation, 1U);
    EXPECT_EQ(recovered->report().checkpoint.record_sequence, 9U);
    EXPECT_EQ(recovered->report().applied_suffix_command_count, 1U);
    EXPECT_EQ(recovered->report().part_count, 1U);
    EXPECT_EQ(recovered->report().durable_version_count, 2U);
    const TemporalSnapshotProvider* const provider = recovered->provider(retained->table_id());
    ASSERT_NE(provider, nullptr);
    EXPECT_EQ(provider->latest_commit_position(), 10U);
    EXPECT_EQ(provider->logical_row_count(), 2U);
    EXPECT_EQ(provider->version_count(), 3U);
    auto reopened = recovered->release_writer();
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_TRUE(reopened->close().is_ok());
    succeeded = true;
    break;
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::query
