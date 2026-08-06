#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/compaction_coordinator.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"
#include "manifest/manifest_flush_crash_fixture.hpp"

#include <array>
#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
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

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId result{};
  result.bytes.front() = std::byte{0x70U};
  return result;
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

class BenchmarkDirectory {
public:
  BenchmarkDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-publication-bench-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
    }
  }
  ~BenchmarkDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] std::shared_ptr<const schema::TableSchema> make_schema() {
  const schema::ColumnId event = id<schema::ColumnId>(5U);
  std::vector<schema::ColumnDefinition> columns;
  columns.push_back(schema::ColumnDefinition::create(
                        event, "event_time",
                        schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(),
                        false)
                        .value());
  return std::make_shared<const schema::TableSchema>(
      schema::TableSchema::create(id<schema::TableId>(2U), id<schema::SchemaId>(4U),
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
batch(const std::shared_ptr<const schema::TableSchema>& schema_value,
      const std::span<const std::int64_t> values) {
  std::vector<std::byte> encoded;
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

[[nodiscard]] ingest::RetryIdentity retry(const std::uint8_t seed) {
  return {.client_id = id<ingest::ClientId>(seed),
          .client_batch_id = id<ingest::ClientBatchId>(static_cast<std::uint8_t>(seed + 1U))};
}

[[nodiscard]] ingest::ColumnarAppendMutationIdentity
mutation(const std::shared_ptr<const schema::TableSchema>& schema_value, const std::uint8_t seed) {
  ingest::Sha256Digest::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return {.table_id = schema_value->table_id(),
          .tablet_id = id<schema::TabletId>(3U),
          .request_digest = ingest::Sha256Digest{bytes}};
}

class PublicationFixture {
public:
  PublicationFixture() : schema_value_(make_schema()) {
    std::filesystem::create_directory(directory_.path() / kPartsDirectoryName);
    std::filesystem::create_directory(directory_.path() / kManifestDirectoryName);
    std::ofstream lock{directory_.path() / kManifestDirectoryName /
                       std::string{kManifestLockFileName}};
    lock.close();
    const EncodedManifest encoded =
        encode_manifest_v1({.generation = 1U,
                            .database_id = id<DatabaseId>(6U),
                            .wal_id = wal_id(),
                            .reclaim_checkpoint = {.record_sequence = 0U,
                                                   .segment_number = 1U,
                                                   .byte_offset = 64U},
                            .tablets = {},
                            .parts = {},
                            .retries = {}})
            .value();
    std::ofstream output{directory_.path() / kManifestDirectoryName / *manifest_file_name(1U),
                         std::ios::binary};
    // std::ofstream has no std::byte overload.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output.write(reinterpret_cast<const char*>(encoded.bytes().data()),
                 static_cast<std::streamsize>(encoded.size()));
    output.close();
    ManifestStorage storage =
        ManifestStorage::open_existing({.database_root = directory_.path().string()}).value();
    manifest_ = std::make_shared<const LoadedManifestGeneration>(
        storage
            .load_selected_manifest({.expected_database_id = id<DatabaseId>(6U),
                                     .expected_wal_id = wal_id(),
                                     .schema_bindings = {},
                                     .decode_limits = {},
                                     .part_validation_limits = {}})
            .value());

    state_ = std::make_unique<ingest::TabletState>(
        ingest::TabletState::create(
            schema_value_, id<schema::TabletId>(3U),
            {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U}},
             .maximum_schema_versions = 1U,
             .maximum_sealed_generations = 4U,
             .maximum_retry_entries = 8U})
            .value());
    const std::array first_values{std::int64_t{-5}, std::int64_t{10}};
    ingest::PreparedTabletAppend first =
        state_
            ->prepare_append(retry(0x10U), mutation(schema_value_, 0x11U),
                             batch(schema_value_, first_values))
            .value();
    static_cast<void>(first.mark_wal_started());
    first_ = std::make_unique<ingest::TabletSnapshot>(
        first.publish({.wal_id = wal_id(), .record_sequence = 7U}).value().snapshot);
    const std::array second_values{std::int64_t{20}};
    ingest::PreparedTabletAppend second =
        state_
            ->prepare_append(retry(0x20U), mutation(schema_value_, 0x21U),
                             batch(schema_value_, second_values))
            .value();
    static_cast<void>(second.mark_wal_started());
    latest_ = std::make_unique<ingest::TabletSnapshot>(
        second.publish({.wal_id = wal_id(), .record_sequence = 8U}).value().snapshot);
  }

  [[nodiscard]] DatabaseStoragePublisher publisher(const ingest::TabletSnapshot& tablet) const {
    const std::array input{DatabaseStorageTabletInput{.snapshot = std::cref(tablet)}};
    return DatabaseStoragePublisher::create(manifest_, input).value();
  }
  [[nodiscard]] const ingest::TabletSnapshot& first() const noexcept {
    return *first_;
  }
  [[nodiscard]] const ingest::TabletSnapshot& latest() const noexcept {
    return *latest_;
  }

private:
  BenchmarkDirectory directory_;
  std::shared_ptr<const schema::TableSchema> schema_value_;
  std::shared_ptr<const LoadedManifestGeneration> manifest_;
  std::unique_ptr<ingest::TabletState> state_;
  std::unique_ptr<ingest::TabletSnapshot> first_;
  std::unique_ptr<ingest::TabletSnapshot> latest_;
};

void write_file(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  // std::ofstream has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
}

class ReclamationBenchmarkFixture {
public:
  ReclamationBenchmarkFixture() {
    const std::filesystem::path& root = directory_.path();
    std::filesystem::create_directory(root / kPartsDirectoryName);
    std::filesystem::create_directory(root / kManifestDirectoryName);
    write_file(root / kManifestDirectoryName / std::string{kManifestLockFileName}, {});
    write_file(root / kPartsDirectoryName / part_file_name(fixture_.part_id),
               fixture_.encoded.bytes());
    write_file(root / kManifestDirectoryName / *manifest_file_name(1U),
               fixture_.manifest(1U).bytes());
    write_file(root / kManifestDirectoryName / *manifest_file_name(2U),
               fixture_.manifest(2U).bytes());
    storage_ = std::make_unique<ManifestStorage>(
        ManifestStorage::open_existing({.database_root = directory_.path().string()}).value());
    const auto bindings = fixture_.bindings();
    predecessor_ = std::make_shared<const LoadedManifestGeneration>(
        storage_
            ->load_selected_manifest({.expected_database_id = fixture_.database_id,
                                      .expected_wal_id = fixture_.wal_id,
                                      .schema_bindings = bindings,
                                      .decode_limits = {},
                                      .part_validation_limits = {}})
            .value());
    publisher_ = std::make_unique<DatabaseStoragePublisher>(
        DatabaseStoragePublisher::create(predecessor_, {}).value());
    predecessor_snapshot_.emplace(publisher_->snapshot().value());
    AppendOnlyCompactionCoordinator coordinator =
        AppendOnlyCompactionCoordinator::create(*storage_, *publisher_).value();
    const std::array inputs{fixture_.part_id};
    const common::Result<AppendOnlyCompactionCompletion> completed =
        coordinator.compact({.tablet_id = fixture_.tablet_id,
                             .input_part_ids = inputs,
                             .output_part_id = test::crash_id<cseg::PartId>(9U),
                             .part_nonce = test::crash_nonce(0xc0U),
                             .manifest_nonce = test::crash_nonce(0xd0U),
                             .compression = cseg::PageCompression::kZstd,
                             .schema_bindings = bindings,
                             .manifest_decode_limits = {},
                             .part_validation_limits = {},
                             .compaction_limits = {}});
    if (!completed.has_value()) {
      throw std::runtime_error{completed.error().to_string()};
    }
    retirements_ = publisher_->drain_retired_part_sets().value();
    current_ = std::make_shared<const LoadedManifestGeneration>(
        storage_
            ->load_selected_manifest({.expected_database_id = fixture_.database_id,
                                      .expected_wal_id = fixture_.wal_id,
                                      .schema_bindings = bindings,
                                      .decode_limits = {},
                                      .part_validation_limits = {}})
            .value());
  }

  [[nodiscard]] PartReclamationReport reclaim() {
    return storage_
        ->reclaim_retired_parts({.selected_manifest = std::cref(*current_),
                                 .retirement = std::cref(retirements_.front()),
                                 .decode_limits = {}})
        .value();
  }

  void release_predecessor() {
    predecessor_snapshot_.reset();
  }

private:
  BenchmarkDirectory directory_;
  test::ManifestFlushCrashFixture fixture_;
  std::unique_ptr<ManifestStorage> storage_;
  std::shared_ptr<const LoadedManifestGeneration> predecessor_;
  std::unique_ptr<DatabaseStoragePublisher> publisher_;
  std::optional<DatabaseStorageSnapshot> predecessor_snapshot_;
  std::vector<RetiredPartSet> retirements_;
  std::shared_ptr<const LoadedManifestGeneration> current_;
};

void snapshot_acquire(benchmark::State& state) {
  const PublicationFixture fixture;
  DatabaseStoragePublisher publisher = fixture.publisher(fixture.latest());
  for (auto _ : state) {
    static_cast<void>(_);
    const auto snapshot = publisher.snapshot();
    benchmark::DoNotOptimize(snapshot->generation());
  }
  state.SetItemsProcessed(state.iterations());
}

void tablet_refresh(benchmark::State& state) {
  const PublicationFixture fixture;
  for (auto _ : state) {
    static_cast<void>(_);
    DatabaseStoragePublisher publisher = fixture.publisher(fixture.first());
    const auto snapshot = publisher.publish_tablet_snapshot(fixture.latest());
    benchmark::DoNotOptimize(snapshot->visible_head_row_count());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * 3);
}

void pinned_retirement_check(benchmark::State& state) {
  ReclamationBenchmarkFixture fixture;
  for (auto _ : state) {
    static_cast<void>(_);
    PartReclamationReport report = fixture.reclaim();
    benchmark::DoNotOptimize(report.outcome);
  }
  state.counters["candidates"] = 1.0;
  state.SetItemsProcessed(state.iterations());
}

void idempotent_reclamation_verification(benchmark::State& state) {
  ReclamationBenchmarkFixture fixture;
  fixture.release_predecessor();
  benchmark::DoNotOptimize(fixture.reclaim());
  for (auto _ : state) {
    static_cast<void>(_);
    PartReclamationReport report = fixture.reclaim();
    benchmark::DoNotOptimize(report.already_absent_parts);
  }
  state.counters["candidates"] = 1.0;
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(snapshot_acquire);
BENCHMARK(tablet_refresh);
BENCHMARK(pinned_retirement_check);
BENCHMARK(idempotent_reclamation_verification);

} // namespace
} // namespace chronos::manifest
