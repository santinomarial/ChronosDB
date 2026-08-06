#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/table_schema.hpp"

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
  [[nodiscard]] const std::string& path() const noexcept {
    return path_;
  }

private:
  std::string path_;
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
    std::filesystem::create_directory(std::filesystem::path{directory_.path()} /
                                      kPartsDirectoryName);
    std::filesystem::create_directory(std::filesystem::path{directory_.path()} /
                                      kManifestDirectoryName);
    std::ofstream lock{std::filesystem::path{directory_.path()} / kManifestDirectoryName /
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
    std::ofstream output{std::filesystem::path{directory_.path()} / kManifestDirectoryName /
                             *manifest_file_name(1U),
                         std::ios::binary};
    // std::ofstream has no std::byte overload.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output.write(reinterpret_cast<const char*>(encoded.bytes().data()),
                 static_cast<std::streamsize>(encoded.size()));
    output.close();
    ManifestStorage storage =
        ManifestStorage::open_existing({.database_root = directory_.path()}).value();
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

BENCHMARK(snapshot_acquire);
BENCHMARK(tablet_refresh);

} // namespace
} // namespace chronos::manifest
