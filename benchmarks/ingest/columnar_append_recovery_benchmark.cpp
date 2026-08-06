#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append_recovery.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint64_t value) {
  chronos::common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[bytes.size() - sizeof(value) + index] =
        static_cast<std::byte>((value >> ((sizeof(value) - index - 1U) * 8U)) & 0xffU);
  }
  return Identifier::from_bytes(bytes).value();
}

class TemporaryWalDirectory {
public:
  [[nodiscard]] static std::optional<TemporaryWalDirectory> create() {
    std::error_code error;
    const std::filesystem::path root = std::filesystem::temp_directory_path(error);
    if (error) {
      return std::nullopt;
    }
    std::string pattern = (root / "chronos-columnar-recovery-benchmark-XXXXXX").string();
    if (::mkdtemp(pattern.data()) == nullptr) {
      return std::nullopt;
    }
    return TemporaryWalDirectory{std::filesystem::path{pattern}};
  }

  TemporaryWalDirectory(const TemporaryWalDirectory&) = delete;
  TemporaryWalDirectory& operator=(const TemporaryWalDirectory&) = delete;
  TemporaryWalDirectory(TemporaryWalDirectory&& other) noexcept
      : path_(std::exchange(other.path_, {})) {}
  TemporaryWalDirectory& operator=(TemporaryWalDirectory&& other) noexcept {
    if (this != &other) {
      remove();
      path_ = std::exchange(other.path_, {});
    }
    return *this;
  }
  ~TemporaryWalDirectory() {
    remove();
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  explicit TemporaryWalDirectory(std::filesystem::path path) : path_(std::move(path)) {}

  void remove() noexcept {
    if (!path_.empty()) {
      std::error_code ignored;
      static_cast<void>(std::filesystem::remove_all(path_, ignored));
      path_.clear();
    }
  }

  std::filesystem::path path_;
};

struct RecoveryFixture {
  std::shared_ptr<const chronos::schema::TableSchema> schema;
  std::shared_ptr<const chronos::columnar::OwnedColumnarBatch> batch;
  chronos::schema::TabletId tablet_id;
  std::size_t encoded_batch_bytes{};
};

[[nodiscard]] RecoveryFixture make_fixture(const std::uint32_t rows) {
  const auto timestamp =
      chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kTimestampNs).value();
  const chronos::schema::ColumnId timestamp_id = id<chronos::schema::ColumnId>(1U);
  std::vector<chronos::schema::ColumnDefinition> definitions;
  definitions.push_back(
      chronos::schema::ColumnDefinition::create(timestamp_id, "ts", timestamp, false).value());
  const chronos::schema::TableSchemaRoles roles{.event_time_column = timestamp_id,
                                                .physical_ordering_key = {timestamp_id},
                                                .partition_columns = {timestamp_id},
                                                .shard_key = {timestamp_id},
                                                .deduplication_key = {}};
  auto schema = std::make_shared<const chronos::schema::TableSchema>(
      chronos::schema::TableSchema::create(
          id<chronos::schema::TableId>(90U), id<chronos::schema::SchemaId>(91U),
          chronos::schema::SchemaVersion::initial(), std::nullopt, std::move(definitions), roles)
          .value());
  std::vector<chronos::columnar::OwnedColumnVector> columns;
  columns.push_back(chronos::columnar::OwnedColumnVector::create(
                        chronos::columnar::ColumnVectorMetadata{.column_id = timestamp_id,
                                                                .type = timestamp,
                                                                .nullable = false,
                                                                .row_count = rows,
                                                                .null_count = 0U},
                        chronos::columnar::ColumnVectorBuffers{
                            .validity = {},
                            .offsets = {},
                            .values = std::vector<std::byte>(static_cast<std::size_t>(rows) * 8U)})
                        .value());
  auto batch = std::make_shared<const chronos::columnar::OwnedColumnarBatch>(
      chronos::columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
  const std::size_t encoded_batch_bytes =
      chronos::columnar::encode_columnar_batch_v1(*batch).value().size();
  return RecoveryFixture{.schema = std::move(schema),
                         .batch = std::move(batch),
                         .tablet_id = id<chronos::schema::TabletId>(92U),
                         .encoded_batch_bytes = encoded_batch_bytes};
}

[[nodiscard]] bool write_history(const chronos::wal::WalWriterConfig& writer_config,
                                 const RecoveryFixture& fixture, const std::size_t records,
                                 const bool duplicate_pairs, benchmark::State& state) {
  auto created = chronos::wal::WalWriter::create_new(writer_config);
  if (!created.has_value()) {
    const std::string message = created.error().to_string();
    state.SkipWithError(message);
    return false;
  }
  chronos::wal::WalWriter writer = std::move(*created);
  std::optional<chronos::wal::EncodedApplicationPayload> previous;
  for (std::size_t index = 0U; index < records; ++index) {
    const bool is_duplicate = duplicate_pairs && index % 2U == 1U;
    if (!is_duplicate) {
      auto encoded_batch = chronos::columnar::encode_columnar_batch_v1(*fixture.batch);
      if (!encoded_batch.has_value()) {
        const std::string message = encoded_batch.error().to_string();
        state.SkipWithError(message);
        return false;
      }
      auto command = chronos::ingest::encode_columnar_append_v1(
          {.client_id = id<chronos::ingest::ClientId>(index + 100U),
           .client_batch_id = id<chronos::ingest::ClientBatchId>(index + 10'000U),
           .tablet_id = fixture.tablet_id},
          *encoded_batch);
      if (!command.has_value()) {
        const std::string message = command.error().to_string();
        state.SkipWithError(message);
        return false;
      }
      previous.emplace(std::move(*command));
    }
    if (!previous.has_value()) {
      state.SkipWithError("recovery benchmark did not construct a command");
      return false;
    }
    auto appended = writer.append_application_entry(previous->bytes());
    if (!appended.has_value()) {
      const std::string message = appended.error().to_string();
      state.SkipWithError(message);
      return false;
    }
  }
  auto synchronized = writer.synchronize();
  if (!synchronized.has_value()) {
    const std::string message = synchronized.error().to_string();
    state.SkipWithError(message);
    return false;
  }
  const chronos::common::Status closed = writer.close();
  if (!closed.is_ok()) {
    const std::string message = closed.to_string();
    state.SkipWithError(message);
    return false;
  }
  return true;
}

[[nodiscard]] chronos::ingest::ColumnarAppendRecoveryConfig
recovery_config(const RecoveryFixture& fixture, const std::size_t records,
                const bool duplicate_pairs) {
  const std::size_t unique_records = duplicate_pairs ? records / 2U : records;
  const std::uint64_t rows =
      static_cast<std::uint64_t>(fixture.batch->row_count()) * unique_records;
  return chronos::ingest::ColumnarAppendRecoveryConfig{
      .retry_directory = {.maximum_entries = unique_records},
      .tablets = {{.schema = fixture.schema,
                   .tablet_id = fixture.tablet_id,
                   .state = {.head_capacity = {.row_capacity = static_cast<std::uint32_t>(rows),
                                               .variable_value_bytes = {0U}},
                             .maximum_sealed_generations = 1U,
                             .maximum_retry_entries = unique_records}}},
      .decode_limits = {}};
}

void benchmark_recovery(benchmark::State& state, const bool duplicate_pairs) {
  const auto records = static_cast<std::size_t>(state.range(0));
  const auto rows_per_batch = static_cast<std::uint32_t>(state.range(1));
  const std::uint64_t unique_records = duplicate_pairs ? records / 2U : records;
  if (records == 0U || unique_records == 0U ||
      unique_records * rows_per_batch > std::numeric_limits<std::uint32_t>::max()) {
    state.SkipWithError("recovery benchmark arguments exceed tablet row bounds");
    return;
  }
  const RecoveryFixture fixture = make_fixture(rows_per_batch);
  auto directory = TemporaryWalDirectory::create();
  if (!directory.has_value()) {
    state.SkipWithError("could not create a temporary WAL directory");
    return;
  }
  const chronos::wal::WalWriterConfig writer_config{.directory_path = directory->path().string()};
  if (!write_history(writer_config, fixture, records, duplicate_pairs, state)) {
    return;
  }

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    {
      state.ResumeTiming();
      auto recovered = chronos::ingest::recover_columnar_append_wal(
          writer_config, {}, recovery_config(fixture, records, duplicate_pairs));
      benchmark::DoNotOptimize(recovered.has_value() ? recovered->tablet_count() : 0U);
      benchmark::ClobberMemory();
      state.PauseTiming();
      if (!recovered.has_value()) {
        const std::string message = recovered.error().to_string();
        state.SkipWithError(message);
        return;
      }
      auto writer = recovered->release_writer();
      if (!writer.has_value()) {
        const std::string message = writer.error().to_string();
        state.SkipWithError(message);
        return;
      }
      const chronos::common::Status closed = writer->close();
      if (!closed.is_ok()) {
        const std::string message = closed.to_string();
        state.SkipWithError(message);
        return;
      }
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(records));
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(records) *
                          static_cast<std::int64_t>(fixture.encoded_batch_bytes));
}

void benchmark_unique_recovery(benchmark::State& state) {
  benchmark_recovery(state, false);
  state.SetLabel(
      "verify + preflight + own batches + fresh tablet/retry replay; warm cache allowed");
}

void benchmark_matching_retry_recovery(benchmark::State& state) {
  benchmark_recovery(state, true);
  state.SetLabel(
      "verify + preflight + alternating apply/matching-retry position replay; warm cache allowed");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_unique_recovery)
    ->Args({16, 64})
    ->Args({256, 64})
    ->Args({16, 1024})
    ->UseRealTime();
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_matching_retry_recovery)
    ->Args({16, 64})
    ->Args({256, 64})
    ->Args({16, 1024})
    ->UseRealTime();

} // namespace
