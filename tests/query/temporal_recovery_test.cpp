#include "chronos/io/posix_io.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/query/temporal_recovery.hpp"
#include "chronos/wal/application.hpp"
#include "columnar/columnar_test_support.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <variant>
#include <vector>

namespace chronos::query {
namespace {

class TemporaryDirectory {
public:
  explicit TemporaryDirectory(const char* label) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / (std::string{label} + "-XXXXXX")).string();
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

class FixedWalIdGenerator final : public wal::WalLogIdGenerator {
public:
  explicit FixedWalIdGenerator(const wal::WalId id) : id_(id) {}
  [[nodiscard]] common::Result<wal::WalId> generate() override {
    return id_;
  }

private:
  wal::WalId id_;
};

template <typename Identifier> [[nodiscard]] Identifier first_byte_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

template <typename Integer>
void append_little_endian(std::vector<std::byte>& bytes, Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
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
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(output.good());
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
temporal_command(const std::shared_ptr<const schema::TableSchema>& schema_value,
                 const std::int64_t event_time, const char logical_identity,
                 const std::int64_t receive_time, const TemporalMutationKind kind,
                 const std::int64_t system_time) {
  std::vector<std::byte> values;
  append_little_endian(values, event_time);
  std::vector<columnar::OwnedColumnVector> columns;
  columns.push_back(columnar::OwnedColumnVector::create(
                        {.column_id = schema_value->event_time_column(),
                         .type = schema_value->columns().front().type(),
                         .nullable = false,
                         .row_count = 1U,
                         .null_count = 0U},
                        {.validity = {}, .offsets = {}, .values = std::move(values)})
                        .value());
  auto batch = columnar::OwnedColumnarBatch::create(schema_value, std::move(columns));
  return encode_temporal_command_v1(
             *batch,
             {TemporalMutationDescriptor{
                 .logical_identity = {std::byte{static_cast<std::uint8_t>(logical_identity)}},
                 .event_time_ns = event_time,
                 .receive_time_ns = receive_time,
                 .kind = kind}},
             system_time)
      .value();
}

[[nodiscard]] std::vector<TemporalMutationDescriptor> descriptors(const TemporalMutationKind kind) {
  return {{{std::byte{1U}}, 100, 110, kind}, {{std::byte{2U}}, 200, 220, kind}};
}

[[nodiscard]] EncodedTemporalCommand command(const columnar::OwnedColumnarBatch& batch,
                                             const TemporalMutationKind kind,
                                             const std::int64_t system_time) {
  return encode_temporal_command_v1(batch, descriptors(kind), system_time).value();
}

[[nodiscard]] wal::WalId write_history(const wal::WalWriterConfig& config,
                                       const columnar::OwnedColumnarBatch& batch,
                                       const bool begin_with_correction = false) {
  auto created = wal::WalWriter::create_new(config);
  EXPECT_TRUE(created.has_value());
  if (!created.has_value()) {
    return {};
  }
  wal::WalWriter writer = std::move(*created);
  const wal::WalId identity = writer.wal_id();
  auto first = command(batch,
                       begin_with_correction ? TemporalMutationKind::kCorrection
                                             : TemporalMutationKind::kOriginal,
                       1000);
  EXPECT_TRUE(writer.append_application_entry(first.bytes()).has_value());
  if (!begin_with_correction) {
    auto second = command(batch, TemporalMutationKind::kCorrection, 2000);
    EXPECT_TRUE(writer.append_application_entry(second.bytes()).has_value());
  }
  EXPECT_TRUE(writer.synchronize().has_value());
  EXPECT_TRUE(writer.close().is_ok());
  return identity;
}

[[nodiscard]] TemporalRecoveryConfig
recovery_config(const std::shared_ptr<const schema::TableSchema>& retained) {
  std::vector<TemporalRecoveryTableConfig> tables;
  tables.push_back({.schema = retained, .store_limits = {}});
  return TemporalRecoveryConfig{.tables = std::move(tables), .decode_limits = {}};
}

TEST(TemporalRecoveryTest, ReplaysVerifiedHistoryAndContinuesTheWalSequence) {
  TemporaryDirectory directory{"chronos-temporal-recovery"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const std::shared_ptr<const schema::TableSchema> retained = columnar::test::batch_schema();
  auto batch = columnar::OwnedColumnarBatch::create(retained, columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  const wal::WalId expected_wal_id = write_history(writer_config, *batch);
  ASSERT_TRUE(expected_wal_id.is_valid());

  auto recovered = recover_temporal_wal(writer_config, {}, recovery_config(retained));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  ASSERT_EQ(recovered->table_count(), 1U);
  TemporalSnapshotProvider* const provider = recovered->provider(retained->table_id());
  ASSERT_NE(provider, nullptr);
  EXPECT_EQ(provider->latest_commit_position(), 2U);
  EXPECT_EQ(provider->logical_row_count(), 2U);
  EXPECT_EQ(provider->version_count(), 4U);
  auto historical = provider->resolve(retained, 1500);
  ASSERT_TRUE(historical.has_value()) << historical.error().to_string();
  EXPECT_EQ((*historical)->committed_position(), 1U);
  ASSERT_EQ((*historical)->rows().size(), 2U);
  EXPECT_EQ((*historical)->rows()[0].wal_id, common::Uuid{expected_wal_id.bytes});

  auto writer = recovered->release_writer();
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  EXPECT_FALSE(recovered->release_writer().has_value());
  auto next = writer->next_record_sequence();
  ASSERT_TRUE(next.has_value());
  EXPECT_EQ(*next, 3U);
  EXPECT_TRUE(writer->close().is_ok());
}

TEST(TemporalRecoveryTest, RejectsCorrectionWithoutAnOriginalAsCommittedCorruption) {
  TemporaryDirectory directory{"chronos-temporal-invalid-history"};
  ASSERT_TRUE(directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = directory.path().string()};
  const std::shared_ptr<const schema::TableSchema> retained = columnar::test::batch_schema();
  auto batch = columnar::OwnedColumnarBatch::create(retained, columnar::test::batch_columns());
  ASSERT_TRUE(batch.has_value()) << batch.error().to_string();
  ASSERT_TRUE(write_history(writer_config, *batch, true).is_valid());

  auto recovered = recover_temporal_wal(writer_config, {}, recovery_config(retained));
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption);
}

TEST(TemporalManifestWalRecoveryTest, VerifiesCheckpointOverlapAndReplaysOnlyTheSuffix) {
  TemporaryDirectory directory{"chronos-manifest-temporal-recovery"};
  ASSERT_TRUE(directory.valid());
  const std::filesystem::path database_root = directory.path() / "database";
  const std::filesystem::path wal_root = directory.path() / "wal";
  const std::filesystem::path disagreeing_wal_root = directory.path() / "disagreeing-wal";
  ASSERT_TRUE(std::filesystem::create_directories(database_root));
  ASSERT_TRUE(std::filesystem::create_directories(wal_root));
  ASSERT_TRUE(std::filesystem::create_directories(disagreeing_wal_root));
  establish_manifest_layout(database_root);

  wal::WalId wal_id{};
  wal_id.bytes.front() = std::byte{8U};
  FixedWalIdGenerator generator{wal_id};
  const wal::WalWriterConfig writer_config{.directory_path = wal_root.string()};
  auto created = wal::WalWriter::create_new(writer_config, generator);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  wal::WalWriter writer = std::move(*created);
  const auto covered_payload = wal::encode_application_payload({.application_format = 99U,
                                                                .application_kind = 99U,
                                                                .application_flags = 0U,
                                                                .application_body = {}});
  ASSERT_TRUE(covered_payload.has_value());
  wal::WalAppendResult checkpoint_append{};
  for (std::uint64_t sequence = 1U; sequence <= 7U; ++sequence) {
    auto appended = writer.append_application_entry(covered_payload->bytes());
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->record_sequence, sequence);
    checkpoint_append = *appended;
  }

  const std::shared_ptr<const schema::TableSchema> retained = temporal_storage_schema();
  const EncodedTemporalCommand reclaimed =
      temporal_command(retained, 15, 'x', 150, TemporalMutationKind::kOriginal, 199);
  auto reclaimed_append = writer.append_application_entry(reclaimed.bytes());
  ASSERT_TRUE(reclaimed_append.has_value()) << reclaimed_append.error().to_string();
  EXPECT_EQ(reclaimed_append->record_sequence, 8U);
  const EncodedTemporalCommand covered =
      temporal_command(retained, 20, 'b', 101, TemporalMutationKind::kOriginal, 201);
  auto covered_append = writer.append_application_entry(covered.bytes());
  ASSERT_TRUE(covered_append.has_value()) << covered_append.error().to_string();
  EXPECT_EQ(covered_append->record_sequence, 9U);
  const EncodedTemporalCommand correction =
      temporal_command(retained, 30, 'a', 300, TemporalMutationKind::kCorrection, 300);
  auto suffix = writer.append_application_entry(correction.bytes());
  ASSERT_TRUE(suffix.has_value()) << suffix.error().to_string();
  EXPECT_EQ(suffix->record_sequence, 10U);
  ASSERT_TRUE(writer.synchronize().has_value());
  ASSERT_TRUE(writer.close().is_ok());

  FixedWalIdGenerator disagreeing_generator{wal_id};
  const wal::WalWriterConfig disagreeing_writer_config{.directory_path =
                                                           disagreeing_wal_root.string()};
  auto disagreeing_created =
      wal::WalWriter::create_new(disagreeing_writer_config, disagreeing_generator);
  ASSERT_TRUE(disagreeing_created.has_value()) << disagreeing_created.error().to_string();
  wal::WalWriter disagreeing_writer = std::move(*disagreeing_created);
  wal::WalAppendResult disagreeing_checkpoint{};
  for (std::uint64_t sequence = 1U; sequence <= 7U; ++sequence) {
    auto appended = disagreeing_writer.append_application_entry(covered_payload->bytes());
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    disagreeing_checkpoint = *appended;
  }
  ASSERT_EQ(disagreeing_checkpoint.record_end, checkpoint_append.record_end);
  ASSERT_TRUE(disagreeing_writer.append_application_entry(reclaimed.bytes()).has_value());
  const EncodedTemporalCommand disagreeing =
      temporal_command(retained, 21, 'b', 101, TemporalMutationKind::kOriginal, 201);
  ASSERT_TRUE(disagreeing_writer.append_application_entry(disagreeing.bytes()).has_value());
  ASSERT_TRUE(disagreeing_writer.append_application_entry(correction.bytes()).has_value());
  ASSERT_TRUE(disagreeing_writer.synchronize().has_value());
  ASSERT_TRUE(disagreeing_writer.close().is_ok());

  const cseg::EncodedCsegPart encoded = cseg::test::make_valid_temporal_part();
  const schema::TabletId tablet_id = first_byte_id<schema::TabletId>(3U);
  const common::Uuid source_id{wal_id.bytes};
  const auto part = manifest::describe_manifest_v2_temporal_part_image(
      encoded.bytes(), *retained, tablet_id, cseg::temporal_format::CommitSource::kWal, source_id);
  ASSERT_TRUE(part.has_value()) << part.error().to_string();
  const manifest::TemporalTabletDescriptor tablet{.table_id = retained->table_id(),
                                                  .tablet_id = tablet_id,
                                                  .recovery_schema_id = retained->schema_id(),
                                                  .recovery_schema_version = retained->version(),
                                                  .source_id = source_id,
                                                  .durable_position = 9U,
                                                  .reclaim_position = 0U,
                                                  .first_part_index = 0U,
                                                  .part_count = 1U,
                                                  .durable_version_count = part->row_count,
                                                  .commit_source =
                                                      cseg::temporal_format::CommitSource::kWal};
  const manifest::DatabaseId database_id = first_byte_id<manifest::DatabaseId>(6U);
  const std::array tablets{tablet};
  const std::array parts{*part};
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
  const auto startup_config = [&](const wal::WalWriterConfig& selected_writer,
                                  const std::optional<std::int64_t> retention) {
    return TemporalManifestWalStartupConfig{
        .manifest_storage = {.database_root = database_root.string()},
        .manifest_load = {.expected_database_id = database_id,
                          .schema_bindings = schema_bindings,
                          .source_bindings = source_bindings,
                          .decode_limits = {},
                          .part_validation_limits = {}},
        .wal_writer = selected_writer,
        .wal_recovery = {},
        .retained_system_time_ns = retention,
        .store_limits = {},
        .cseg_limits = {},
        .command_limits = {},
        .reclaim_checkpointed_wal_segments = false};
  };

  const auto missing_boundary =
      recover_manifest_temporal_wal(startup_config(writer_config, std::nullopt));
  ASSERT_FALSE(missing_boundary.has_value());
  EXPECT_EQ(missing_boundary.error().code(), common::StatusCode::kInvalidArgument);

  const auto disagreement = recover_manifest_temporal_wal(
      startup_config(disagreeing_writer_config, part->minimum_system_time));
  ASSERT_FALSE(disagreement.has_value());
  EXPECT_EQ(disagreement.error().code(), common::StatusCode::kCorruption);

  auto recovered =
      recover_manifest_temporal_wal(startup_config(writer_config, part->minimum_system_time));
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(recovered->report().selected_generation, 1U);
  EXPECT_EQ(recovered->report().checkpoint.record_sequence, 7U);
  EXPECT_EQ(recovered->report().tablet_id, tablet_id);
  EXPECT_EQ(recovered->report().tablet_durable_position, 9U);
  EXPECT_EQ(recovered->report().verified_covered_command_count, 2U);
  EXPECT_EQ(recovered->report().applied_suffix_command_count, 1U);
  EXPECT_EQ(recovered->report().part_count, 1U);
  EXPECT_EQ(recovered->report().durable_version_count, 2U);
  EXPECT_EQ(recovered->selected_manifest().generation(), 1U);

  TemporalSnapshotProvider* const provider = recovered->provider();
  ASSERT_NE(provider, nullptr);
  EXPECT_EQ(provider->latest_commit_position(), 10U);
  EXPECT_EQ(provider->version_count(), 3U);
  auto historical = provider->resolve(retained, 250);
  ASSERT_TRUE(historical.has_value()) << historical.error().to_string();
  EXPECT_EQ((*historical)->committed_position(), 9U);
  EXPECT_EQ((*historical)->rows().size(), 2U);
  auto current = provider->resolve(retained, std::nullopt);
  ASSERT_TRUE(current.has_value()) << current.error().to_string();
  EXPECT_EQ((*current)->committed_position(), 10U);
  ASSERT_EQ((*current)->rows().size(), 2U);
  EXPECT_TRUE(std::ranges::any_of((*current)->rows(), [](const ScalarInputRow& row) {
    return std::get<std::int64_t>(row.columns.front().storage()) == 30;
  }));

  auto reopened_writer = recovered->release_writer();
  ASSERT_TRUE(reopened_writer.has_value()) << reopened_writer.error().to_string();
  EXPECT_EQ(reopened_writer->next_record_sequence().value(), 11U);
  EXPECT_TRUE(reopened_writer->close().is_ok());
}

} // namespace
} // namespace chronos::query
