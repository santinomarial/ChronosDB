#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/manifest/checkpoint_builder.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/generation_builder.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/sealed_head_flush.hpp"
#include "chronos/manifest/startup_recovery.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/codec.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] common::Uuid nonce(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-startup-recovery-XXXXXX").string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    const char* const created = ::mkdtemp(writable.data());
    if (created != nullptr) {
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

[[nodiscard]] std::vector<std::byte> read_bytes(const std::filesystem::path& path) {
  std::ifstream input{path, std::ios::binary | std::ios::ate};
  const std::streamsize size = input.tellg();
  if (!input.good() || size < 0) {
    return {};
  }
  std::vector<std::byte> bytes(static_cast<std::size_t>(size));
  input.seekg(0);
  // std::ifstream has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  input.read(reinterpret_cast<char*>(bytes.data()), size);
  return input.good() ? bytes : std::vector<std::byte>{};
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

[[nodiscard]] std::shared_ptr<const schema::TableSchema> make_schema() {
  const schema::ColumnId event_time = id<schema::ColumnId>(3U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event_time, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(1U), id<schema::SchemaId>(2U),
                                  schema::SchemaVersion::initial(), std::nullopt,
                                  std::move(columns),
                                  {.event_time_column = event_time,
                                   .physical_ordering_key = {event_time},
                                   .partition_columns = {event_time},
                                   .shard_key = {event_time},
                                   .deduplication_key = {}})
          .value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
make_batch(const std::shared_ptr<const schema::TableSchema>& schema_value,
           const std::span<const std::int64_t> values) {
  std::vector<std::byte> encoded;
  encoded.reserve(values.size() * sizeof(std::int64_t));
  for (const std::int64_t value : values) {
    append_le(encoded, value);
  }
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::OwnedColumnVector::create(
                        {.column_id = schema_value->event_time_column(),
                         .type = schema_value->columns().front().type(),
                         .nullable = false,
                         .row_count = static_cast<std::uint32_t>(values.size()),
                         .null_count = 0U},
                        {.validity = {}, .offsets = {}, .values = std::move(encoded)})
                        .value());
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(schema_value, std::move(columns)).value());
}

[[nodiscard]] wal::EncodedApplicationPayload
make_command(const std::shared_ptr<const columnar::OwnedColumnarBatch>& batch_value,
             const schema::TabletId& tablet_id, const std::uint8_t identity_seed) {
  const columnar::EncodedColumnarBatch encoded =
      columnar::encode_columnar_batch_v1(*batch_value).value();
  return ingest::encode_columnar_append_v1({.client_id = id<ingest::ClientId>(identity_seed),
                                            .client_batch_id = id<ingest::ClientBatchId>(
                                                static_cast<std::uint8_t>(identity_seed + 1U)),
                                            .tablet_id = tablet_id},
                                           encoded)
      .value();
}

TEST(ManifestColumnarStartupRecoveryTest, PublicBoundaryIsMoveOnly) {
  static_assert(!std::is_copy_constructible_v<RecoveredManifestColumnarState>);
  static_assert(std::is_nothrow_move_constructible_v<RecoveredManifestColumnarState>);
}

TEST(ManifestColumnarStartupRecoveryTest,
     OpensEmptyManifestReplaysWalCleansTemporariesAndPublishesOneDatabaseEpoch) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::filesystem::path parts = directory.path() / kPartsDirectoryName;
  const std::filesystem::path manifests = directory.path() / kManifestDirectoryName;
  const std::filesystem::path wal_path = directory.path() / "wal";
  ASSERT_TRUE(std::filesystem::create_directory(parts));
  ASSERT_TRUE(std::filesystem::create_directory(manifests));
  ASSERT_TRUE(std::filesystem::create_directory(wal_path));

  const std::shared_ptr<const schema::TableSchema> schema_value = make_schema();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(*schema_value).value();
  const schema::TabletId tablet_id = id<schema::TabletId>(6U);
  const wal::WalWriterConfig writer_config{.directory_path = wal_path.string()};
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::WalId wal_id = writer.wal_id();
  const std::array values{std::int64_t{10}, std::int64_t{20}};
  const wal::EncodedApplicationPayload command =
      make_command(make_batch(schema_value, values), tablet_id, 4U);
  ASSERT_TRUE(writer.append_application_entry(command.bytes()).has_value());
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  const DatabaseId database_id = id<DatabaseId>(7U);
  const EncodedManifest generation_one =
      encode_manifest_v1(
          {.generation = 1U,
           .database_id = database_id,
           .wal_id = wal_id,
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = {},
           .parts = {},
           .retries = {}})
          .value();
  write_bytes(manifests / std::string{kManifestLockFileName}, {});
  write_bytes(manifests / *manifest_file_name(1U), generation_one.bytes());
  const cseg::PartId temporary_part_id = id<cseg::PartId>(8U);
  write_bytes(parts / temporary_part_file_name(temporary_part_id, nonce(9U)), {});
  write_bytes(manifests / *temporary_manifest_file_name(2U, nonce(10U)), {});

  const auto make_config = [&]() {
    return ManifestColumnarStartupConfig{
        .manifest_storage = {.database_root = directory.path().string()},
        .manifest_load = {.expected_database_id = database_id,
                          .expected_wal_id = wal_id,
                          .schema_bindings = {},
                          .decode_limits = {},
                          .part_validation_limits = {}},
        .wal_writer = writer_config,
        .wal_recovery = {},
        .columnar_recovery = {
            .retry_directory = {.maximum_entries = 8U},
            .tablets = {ingest::ColumnarRecoveryTabletConfig{
                .schema = schema_value,
                .tablet_id = tablet_id,
                .state = {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U}},
                          .maximum_schema_versions = 1U,
                          .maximum_sealed_generations = 2U,
                          .maximum_retry_entries = 8U,
                          .flush_queue = nullptr},
                .successors = {},
                .durable_seed = std::nullopt}},
            .decode_limits = {},
            .checkpoint = std::nullopt}};
  };

  {
    common::Result<RecoveredManifestColumnarState> recovered =
        recover_manifest_columnar_database(make_config());
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    EXPECT_EQ(recovered->report().selected_generation, 1U);
    EXPECT_EQ(recovered->report().tablet_count, 0U);
    EXPECT_EQ(recovered->report().temporary_cleanup.removed_parts, 1U);
    EXPECT_EQ(recovered->report().temporary_cleanup.removed_manifests, 1U);
    EXPECT_FALSE(recovered->report().wal_reclamation.has_value());
    EXPECT_FALSE(
        std::filesystem::exists(parts / temporary_part_file_name(temporary_part_id, nonce(9U))));
    EXPECT_FALSE(
        std::filesystem::exists(manifests / *temporary_manifest_file_name(2U, nonce(10U))));

    const common::Result<DatabaseStorageSnapshot> database = recovered->snapshot();
    ASSERT_TRUE(database.has_value()) << database.error().to_string();
    EXPECT_EQ(database->generation(), 1U);
    EXPECT_EQ(database->durable_tablets().size(), 0U);
    EXPECT_EQ(database->tablets().size(), 1U);
    EXPECT_EQ(database->visible_head_row_count(), 2U);
    ASSERT_NE(recovered->tablet(tablet_id), nullptr);
    EXPECT_EQ(recovered->tablet(tablet_id)->snapshot()->visible_row_count(), 2U);
    EXPECT_EQ(recovered->retry_directory().metrics().committed_entries, 1U);

    const common::Result<RecoveredManifestColumnarState> concurrent =
        recover_manifest_columnar_database(make_config());
    ASSERT_FALSE(concurrent.has_value());
    EXPECT_EQ(concurrent.error().code(), common::StatusCode::kUnavailable);

    common::Result<wal::WalWriter> reopened = recovered->release_writer();
    ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
    EXPECT_EQ(reopened->next_record_sequence().value(), 2U);
    EXPECT_TRUE(reopened->close().is_ok());
  }

  common::Result<RecoveredManifestColumnarState> repeated =
      recover_manifest_columnar_database(make_config());
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_EQ(repeated->report().temporary_cleanup.removed_parts, 0U);
  EXPECT_EQ(repeated->report().temporary_cleanup.removed_manifests, 0U);
  EXPECT_FALSE(repeated->report().wal_reclamation.has_value());
  EXPECT_EQ(repeated->snapshot()->visible_head_row_count(), 2U);
  EXPECT_TRUE(repeated->release_writer()->close().is_ok());
}

TEST(ManifestColumnarStartupRecoveryTest,
     RestoresSelectedPartAndRetryThenAppliesOnlyTheUncoveredWalRows) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::filesystem::path parts = directory.path() / kPartsDirectoryName;
  const std::filesystem::path manifests = directory.path() / kManifestDirectoryName;
  const std::filesystem::path wal_path = directory.path() / "wal";
  ASSERT_TRUE(std::filesystem::create_directory(parts));
  ASSERT_TRUE(std::filesystem::create_directory(manifests));
  ASSERT_TRUE(std::filesystem::create_directory(wal_path));

  const std::shared_ptr<const schema::TableSchema> schema_value = make_schema();
  const schema::SchemaLineage lineage = schema::SchemaLineage::create(*schema_value).value();
  const schema::TabletId tablet_id = id<schema::TabletId>(6U);
  const std::array durable_values{std::int64_t{10}, std::int64_t{20}};
  const std::array uncovered_values{std::int64_t{30}};
  const std::shared_ptr<const columnar::OwnedColumnarBatch> durable_batch =
      make_batch(schema_value, durable_values);
  const std::shared_ptr<const columnar::OwnedColumnarBatch> uncovered_batch =
      make_batch(schema_value, uncovered_values);
  const wal::EncodedApplicationPayload durable_command =
      make_command(durable_batch, tablet_id, 20U);
  const wal::EncodedApplicationPayload uncovered_command =
      make_command(uncovered_batch, tablet_id, 30U);
  const ingest::ColumnarAppendDecodeResult decoded_durable =
      ingest::decode_columnar_append_v1_exact(durable_command.bytes());
  const ingest::ColumnarAppendDecodeResult decoded_uncovered =
      ingest::decode_columnar_append_v1_exact(uncovered_command.bytes());
  ASSERT_TRUE(decoded_durable.has_value());
  ASSERT_TRUE(decoded_uncovered.has_value());

  const common::Result<wal::RecordLayout> durable_record_layout =
      wal::calculate_record_layout(durable_command.bytes().size());
  ASSERT_TRUE(durable_record_layout.has_value());
  ASSERT_LE(uncovered_command.bytes().size(), durable_command.bytes().size());
  const wal::WalWriterConfig writer_config{
      .directory_path = wal_path.string(),
      .target_segment_size = wal::kSegmentHeaderSize + durable_record_layout->total_length,
      .maximum_application_payload = durable_command.bytes().size()};
  common::Result<wal::WalWriter> created = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const wal::WalId wal_id = writer.wal_id();
  const common::Result<wal::WalAppendResult> durable_append =
      writer.append_application_entry(durable_command.bytes());
  ASSERT_TRUE(durable_append.has_value());
  const common::Result<wal::WalAppendResult> uncovered_append =
      writer.append_application_entry(uncovered_command.bytes());
  ASSERT_TRUE(uncovered_append.has_value());
  EXPECT_EQ(durable_append->record_end.segment_number, 1U);
  EXPECT_EQ(uncovered_append->record_end.segment_number, 2U);
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  ingest::TabletState live =
      ingest::TabletState::create(
          schema_value, tablet_id,
          {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U}},
           .maximum_schema_versions = 1U,
           .maximum_sealed_generations = 2U,
           .maximum_retry_entries = 8U,
           .flush_queue = nullptr})
          .value();
  const ingest::RetryIdentity durable_identity{.client_id = decoded_durable->client_id(),
                                               .client_batch_id =
                                                   decoded_durable->client_batch_id()};
  const ingest::ColumnarAppendMutationIdentity durable_mutation{
      .table_id = decoded_durable->table_id(),
      .tablet_id = decoded_durable->tablet_id(),
      .request_digest = decoded_durable->request_digest()};
  ingest::PreparedTabletAppend first =
      live.prepare_append(durable_identity, durable_mutation, durable_batch).value();
  ASSERT_TRUE(first.mark_wal_started().is_ok());
  ASSERT_TRUE(first.publish({.wal_id = wal_id, .record_sequence = 1U}).has_value());
  const ingest::RetryIdentity uncovered_identity{.client_id = decoded_uncovered->client_id(),
                                                 .client_batch_id =
                                                     decoded_uncovered->client_batch_id()};
  const ingest::ColumnarAppendMutationIdentity uncovered_mutation{
      .table_id = decoded_uncovered->table_id(),
      .tablet_id = decoded_uncovered->tablet_id(),
      .request_digest = decoded_uncovered->request_digest()};
  ingest::PreparedTabletAppend second =
      live.prepare_append(uncovered_identity, uncovered_mutation, uncovered_batch).value();
  ASSERT_TRUE(second.mark_wal_started().is_ok());
  const ingest::TabletSnapshot live_snapshot =
      second.publish({.wal_id = wal_id, .record_sequence = 2U}).value().snapshot;
  ASSERT_EQ(live_snapshot.sealed_generations().size(), 1U);

  const DatabaseId database_id = id<DatabaseId>(7U);
  const EncodedManifest generation_one =
      encode_manifest_v1(
          {.generation = 1U,
           .database_id = database_id,
           .wal_id = wal_id,
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = {},
           .parts = {},
           .retries = {}})
          .value();
  write_bytes(manifests / std::string{kManifestLockFileName}, {});
  write_bytes(manifests / *manifest_file_name(1U), generation_one.bytes());
  const std::array bindings{
      TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  const RetryDescriptor durable_retry{.client_id = durable_identity.client_id,
                                      .client_batch_id = durable_identity.client_batch_id,
                                      .table_id = durable_mutation.table_id,
                                      .tablet_id = durable_mutation.tablet_id,
                                      .request_digest = durable_mutation.request_digest,
                                      .wal_id = wal_id,
                                      .record_sequence = 1U,
                                      .applied_row_count = durable_batch->row_count()};
  const std::array retries{durable_retry};
  const cseg::PartId part_id = id<cseg::PartId>(40U);
  const EncodedSealedHeadPart encoded_part =
      encode_sealed_head_v1({.snapshot = live_snapshot.sealed_generations().front(),
                             .part_id = part_id,
                             .compression = cseg::PageCompression::kNone})
          .value();
  const DecodedManifestView predecessor = decode_manifest_v1_exact(generation_one.bytes()).value();
  const EncodedManifest generation_two =
      build_manifest_v1_for_sealed_head({.predecessor = predecessor,
                                         .sealed_part = encoded_part,
                                         .new_retries = retries,
                                         .schema_bindings = bindings,
                                         .part_validation_limits = {}})
          .value();
  const DecodedManifestView generation_two_view =
      decode_manifest_v1_exact(generation_two.bytes()).value();
  const EncodedManifest generation_three_candidate =
      encode_manifest_v1({.generation = 3U,
                          .database_id = generation_two_view.database_id(),
                          .wal_id = generation_two_view.wal_id(),
                          .reclaim_checkpoint = generation_two_view.reclaim_checkpoint(),
                          .tablets = generation_two_view.tablets(),
                          .parts = generation_two_view.parts(),
                          .retries = generation_two_view.retries()})
          .value();
  const DecodedManifestView generation_three_view =
      decode_manifest_v1_exact(generation_three_candidate.bytes()).value();
  const std::string installed_part_name = part_file_name(part_id);
  const std::array referenced_parts{ReferencedPartImage{
      .file_name = installed_part_name, .bytes = encoded_part.encoded_part.bytes()}};
  const common::Result<CheckpointedManifestGeneration> checkpointed =
      build_manifest_v1_checkpointed_generation({.wal_directory = wal_path.string(),
                                                 .predecessor = std::cref(generation_two_view),
                                                 .candidate = std::cref(generation_three_view),
                                                 .schema_bindings = bindings,
                                                 .referenced_parts = referenced_parts,
                                                 .command_decode_limits = {},
                                                 .part_validation_limits = {}});
  ASSERT_TRUE(checkpointed.has_value()) << checkpointed.error().to_string();
  EXPECT_EQ(checkpointed->reclaim_checkpoint.record_sequence, 1U);
  EXPECT_EQ(checkpointed->reclaim_checkpoint.segment_number, 1U);
  EXPECT_EQ(checkpointed->reclaim_checkpoint.byte_offset, durable_append->record_end.byte_offset);
  {
    ManifestStorage storage =
        ManifestStorage::open_existing({.database_root = directory.path().string()}).value();
    ASSERT_TRUE(storage
                    .install_part({.encoded_part = std::cref(encoded_part.encoded_part),
                                   .descriptor = encoded_part.descriptor,
                                   .wal_id = wal_id,
                                   .schema = std::cref(*schema_value),
                                   .nonce = nonce(41U),
                                   .validation_limits = {}})
                    .has_value());
    ASSERT_TRUE(storage
                    .install_manifest({.encoded_manifest = std::cref(generation_two),
                                       .schema_bindings = bindings,
                                       .nonce = nonce(42U),
                                       .decode_limits = {},
                                       .part_validation_limits = {},
                                       .compaction_equivalence_limits = {}})
                    .has_value());
  }

  const auto make_config = [&](const bool reclaim_wal = false) {
    return ManifestColumnarStartupConfig{
        .manifest_storage = {.database_root = directory.path().string()},
        .manifest_load = {.expected_database_id = database_id,
                          .expected_wal_id = wal_id,
                          .schema_bindings = bindings,
                          .decode_limits = {},
                          .part_validation_limits = {}},
        .wal_writer = writer_config,
        .wal_recovery = {},
        .reclaim_checkpointed_wal_segments = reclaim_wal,
        .columnar_recovery = {
            .retry_directory = {.maximum_entries = 8U},
            .tablets = {ingest::ColumnarRecoveryTabletConfig{
                .schema = schema_value,
                .tablet_id = tablet_id,
                .state = {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U}},
                          .maximum_schema_versions = 1U,
                          .maximum_sealed_generations = 2U,
                          .maximum_retry_entries = 8U,
                          .flush_queue = nullptr},
                .successors = {},
                .durable_seed = std::nullopt}},
            .decode_limits = {},
            .checkpoint = std::nullopt}};
  };

  ManifestColumnarStartupConfig caller_checkpoint = make_config();
  caller_checkpoint.columnar_recovery.checkpoint = wal::WalReplayCheckpoint{
      .wal_id = wal_id, .record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U};
  common::Result<RecoveredManifestColumnarState> rejected =
      recover_manifest_columnar_database(std::move(caller_checkpoint));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);

  ManifestColumnarStartupConfig missing_tablet = make_config();
  missing_tablet.columnar_recovery.tablets.clear();
  rejected = recover_manifest_columnar_database(std::move(missing_tablet));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kNotFound);

  const std::filesystem::path first_segment = wal_path / "wal-00000000000000000001.cwal";
  ASSERT_TRUE(std::filesystem::is_regular_file(first_segment));
  {
    common::Result<RecoveredManifestColumnarState> retained =
        recover_manifest_columnar_database(make_config());
    ASSERT_TRUE(retained.has_value()) << retained.error().to_string();
    EXPECT_EQ(retained->report().selected_generation, 2U);
    EXPECT_FALSE(retained->report().wal_reclamation.has_value());
    EXPECT_EQ(retained->snapshot()->visible_head_row_count(), 1U);
    EXPECT_EQ(retained->retry_directory().metrics().committed_entries, 2U);
    EXPECT_TRUE(std::filesystem::exists(first_segment));
    EXPECT_TRUE(retained->release_writer()->close().is_ok());
  }

  {
    ManifestStorage storage =
        ManifestStorage::open_existing({.database_root = directory.path().string()}).value();
    ASSERT_TRUE(
        storage
            .install_manifest({.encoded_manifest = std::cref(checkpointed->encoded_manifest),
                               .schema_bindings = bindings,
                               .nonce = nonce(43U),
                               .decode_limits = {},
                               .part_validation_limits = {},
                               .compaction_equivalence_limits = {}})
            .has_value());
  }

  const std::vector<std::byte> original_segment = read_bytes(first_segment);
  ASSERT_FALSE(original_segment.empty());
  std::vector<std::byte> corrupt_segment = original_segment;
  corrupt_segment.back() ^= std::byte{0x01U};
  write_bytes(first_segment, corrupt_segment);
  rejected = recover_manifest_columnar_database(make_config(true));
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(std::filesystem::exists(first_segment));
  write_bytes(first_segment, original_segment);

  for (std::size_t attempt = 0U; attempt < 2U; ++attempt) {
    common::Result<RecoveredManifestColumnarState> recovered =
        recover_manifest_columnar_database(make_config(true));
    ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
    EXPECT_EQ(recovered->report().selected_generation, 3U);
    EXPECT_EQ(recovered->report().tablet_count, 1U);
    EXPECT_EQ(recovered->report().part_count, 1U);
    EXPECT_EQ(recovered->report().retry_count, 1U);
    ASSERT_TRUE(recovered->report().wal_reclamation.has_value());
    const wal::WalSegmentReclamationReport reclamation =
        recovered->report().wal_reclamation.value_or(wal::WalSegmentReclamationReport{});
    EXPECT_EQ(reclamation.checkpoint.record_sequence, 1U);
    EXPECT_EQ(reclamation.removed_segment_count, attempt == 0U ? 1U : 0U);
    EXPECT_EQ(reclamation.removed_physical_bytes, attempt == 0U ? original_segment.size() : 0U);
    EXPECT_EQ(reclamation.directory_sync_count, attempt == 0U ? 1U : 0U);
    EXPECT_FALSE(std::filesystem::exists(first_segment));
    const common::Result<DatabaseStorageSnapshot> database = recovered->snapshot();
    ASSERT_TRUE(database.has_value());
    EXPECT_EQ(database->generation(), 3U);
    ASSERT_EQ(database->durable_tablets().size(), 1U);
    EXPECT_EQ(database->durable_tablets().front().durable_row_count, 2U);
    EXPECT_EQ(database->parts().front().part_id, part_id);
    EXPECT_EQ(database->visible_head_row_count(), 1U);
    ASSERT_NE(recovered->tablet(tablet_id), nullptr);
    const ingest::TabletSnapshot recovered_tablet =
        recovered->tablet(tablet_id)->snapshot().value();
    EXPECT_EQ(recovered_tablet.visible_row_count(), 1U);
    EXPECT_EQ(recovered_tablet.retry_entry_count(), 2U);
    EXPECT_EQ(recovered_tablet.retry_outcome(durable_identity)->record_sequence, 1U);
    EXPECT_EQ(recovered_tablet.retry_outcome(uncovered_identity)->record_sequence, 2U);
    EXPECT_EQ(recovered->retry_directory().metrics().committed_entries, 2U);
    common::Result<wal::WalWriter> reopened = recovered->release_writer();
    ASSERT_TRUE(reopened.has_value());
    EXPECT_EQ(reopened->next_record_sequence().value(), 3U);
    EXPECT_TRUE(reopened->close().is_ok());
  }
}

} // namespace
} // namespace chronos::manifest
