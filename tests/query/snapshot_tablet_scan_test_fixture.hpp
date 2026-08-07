#ifndef CHRONOS_TESTS_QUERY_SNAPSHOT_TABLET_SCAN_TEST_FIXTURE_HPP_
#define CHRONOS_TESTS_QUERY_SNAPSHOT_TABLET_SCAN_TEST_FIXTURE_HPP_

#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/ingest/retry_directory.hpp"
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
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::query::test {

class SnapshotTabletScanFixture {
public:
  explicit SnapshotTabletScanFixture(const std::uint32_t rows)
      : schema_(make_schema()), lineage_(schema::SchemaLineage::create(*schema_).value()),
        state_(ingest::TabletState::create(schema_, tablet_id(),
                                           {.head_capacity = {.row_capacity = std::max(rows, 1U),
                                                              .variable_value_bytes = {0U}},
                                            .maximum_schema_versions = 1U,
                                            .maximum_sealed_generations = 1U,
                                            .maximum_retry_entries = 4U,
                                            .flush_queue = {}})
                   .value()) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-tablet-scan-fixture-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      directory_ = created;
    std::filesystem::create_directory(directory_ / manifest::kPartsDirectoryName);
    std::filesystem::create_directory(directory_ / manifest::kManifestDirectoryName);
    write_bytes(directory_ / manifest::kManifestDirectoryName /
                    std::string{manifest::kManifestLockFileName},
                {});

    wal::WalId wal{};
    wal.bytes = cseg::test::identifier<schema::SchemaId>(0x70U).bytes();
    const std::array tablets{
        manifest::TabletDescriptor{.table_id = schema_->table_id(),
                                   .tablet_id = tablet_id(),
                                   .recovery_schema_id = schema_->schema_id(),
                                   .recovery_schema_version = schema_->version(),
                                   .durable_record_sequence = 0U,
                                   .first_part_index = 0U,
                                   .part_count = 0U,
                                   .durable_row_count = 0U}};
    manifest::EncodedManifest encoded =
        manifest::encode_manifest_v1(
            {.generation = 1U,
             .database_id = cseg::test::identifier<manifest::DatabaseId>(6U),
             .wal_id = wal,
             .reclaim_checkpoint = {.record_sequence = 0U,
                                    .segment_number = 1U,
                                    .byte_offset = 64U},
             .tablets = tablets,
             .parts = {},
             .retries = {}})
            .value();
    write_bytes(directory_ / manifest::kManifestDirectoryName / *manifest::manifest_file_name(1U),
                encoded.bytes());
    storage_ = std::make_unique<manifest::ManifestStorage>(
        manifest::ManifestStorage::open_existing({.database_root = directory_.string()}).value());
    const std::array bindings{
        manifest::TabletSchemaBinding{.tablet_id = tablet_id(), .lineage = std::cref(lineage_)}};
    auto selected = std::make_shared<const manifest::LoadedManifestGeneration>(
        storage_
            ->load_selected_manifest(
                {.expected_database_id = cseg::test::identifier<manifest::DatabaseId>(6U),
                 .expected_wal_id = wal,
                 .schema_bindings = bindings,
                 .decode_limits = {},
                 .part_validation_limits = {}})
            .value());

    ingest::TabletSnapshot tablet_snapshot = state_.snapshot().value();
    if (rows != 0U) {
      std::vector<std::int64_t> values(rows);
      for (std::uint32_t row = 0U; row < rows; ++row)
        values[row] = -100 + static_cast<std::int64_t>(row);
      ingest::PreparedTabletAppend prepared =
          state_
              .prepare_append(
                  {.client_id = cseg::test::identifier<ingest::ClientId>(0x11U),
                   .client_batch_id = cseg::test::identifier<ingest::ClientBatchId>(0x12U)},
                  {.table_id = schema_->table_id(),
                   .tablet_id = tablet_id(),
                   .request_digest = digest(0x13U)},
                  batch(values))
              .value();
      if (!prepared.mark_wal_started().is_ok())
        throw std::runtime_error{"snapshot tablet scan fixture WAL transition failed"};
      tablet_snapshot = prepared.publish({.wal_id = wal, .record_sequence = 1U}).value().snapshot;
    }
    const std::array input{manifest::DatabaseStorageTabletInput{.snapshot = tablet_snapshot}};
    publisher_ = std::make_unique<manifest::DatabaseStoragePublisher>(
        manifest::DatabaseStoragePublisher::create(std::move(selected), input).value());
    snapshot_ = std::make_unique<manifest::DatabaseStorageSnapshot>(publisher_->snapshot().value());
  }

  ~SnapshotTabletScanFixture() {
    snapshot_.reset();
    publisher_.reset();
    storage_.reset();
    std::error_code ignored;
    std::filesystem::remove_all(directory_, ignored);
  }

  SnapshotTabletScanFixture(const SnapshotTabletScanFixture&) = delete;
  SnapshotTabletScanFixture& operator=(const SnapshotTabletScanFixture&) = delete;

  [[nodiscard]] common::Result<std::unique_ptr<PhysicalOperator>>
  source(const QueryResourceContext& resources,
         const std::optional<cseg::EventTimePredicate>& predicate = std::nullopt,
         SnapshotTabletScanLimits limits = {}) const {
    common::Result<SnapshotCsegPartScanPlan> plan =
        plan_snapshot_cseg_part_scan(*snapshot_, tablet_id(), predicate);
    if (!plan.has_value())
      return common::make_unexpected(plan.error());
    return create_snapshot_tablet_scan(resources, *snapshot_, *plan, {}, lineage_,
                                       schema_->schema_id(), {0U}, limits);
  }

private:
  template <typename Integer>
  static void append_le(std::vector<std::byte>& bytes, const Integer value) {
    using Unsigned = std::make_unsigned_t<Integer>;
    const Unsigned encoded = std::bit_cast<Unsigned>(value);
    for (std::size_t index = 0U; index < sizeof(Integer); ++index)
      bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }

  [[nodiscard]] static schema::TabletId tablet_id() {
    return cseg::test::identifier<schema::TabletId>(3U);
  }

  [[nodiscard]] static ingest::Sha256Digest digest(const std::uint8_t seed) {
    ingest::Sha256Digest::Bytes bytes{};
    bytes.front() = std::byte{seed};
    return ingest::Sha256Digest{bytes};
  }

  [[nodiscard]] static std::shared_ptr<const schema::TableSchema> make_schema() {
    const schema::ColumnId event = cseg::test::identifier<schema::ColumnId>(5U);
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event, "event_time", cseg::test::type(schema::LogicalTypeKind::kTimestampNs), false)
            .value());
    return std::make_shared<const schema::TableSchema>(
        schema::TableSchema::create(cseg::test::identifier<schema::TableId>(2U),
                                    cseg::test::identifier<schema::SchemaId>(4U),
                                    schema::SchemaVersion::initial(), std::nullopt,
                                    std::move(columns),
                                    {.event_time_column = event,
                                     .physical_ordering_key = {event},
                                     .partition_columns = {event},
                                     .shard_key = {event},
                                     .deduplication_key = {}})
            .value());
  }

  [[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
  batch(const std::span<const std::int64_t> values) const {
    std::vector<std::byte> encoded;
    encoded.reserve(values.size() * sizeof(std::int64_t));
    for (const std::int64_t value : values)
      append_le(encoded, value);
    std::vector<columnar::OwnedColumnVector> columns;
    columns.push_back(columnar::OwnedColumnVector::create(
                          {.column_id = schema_->event_time_column(),
                           .type = schema_->columns().front().type(),
                           .nullable = false,
                           .row_count = static_cast<std::uint32_t>(values.size()),
                           .null_count = 0U},
                          {.validity = {}, .offsets = {}, .values = std::move(encoded)})
                          .value());
    return std::make_shared<const columnar::OwnedColumnarBatch>(
        columnar::OwnedColumnarBatch::create(schema_, std::move(columns)).value());
  }

  static void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
    std::ofstream output{path, std::ios::binary};
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
  }

  std::filesystem::path directory_;
  std::shared_ptr<const schema::TableSchema> schema_;
  schema::SchemaLineage lineage_;
  ingest::TabletState state_;
  std::unique_ptr<manifest::ManifestStorage> storage_;
  std::unique_ptr<manifest::DatabaseStoragePublisher> publisher_;
  std::unique_ptr<manifest::DatabaseStorageSnapshot> snapshot_;
};

} // namespace chronos::query::test

#endif // CHRONOS_TESTS_QUERY_SNAPSHOT_TABLET_SCAN_TEST_FIXTURE_HPP_
