#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

template <typename Identifier> [[nodiscard]] Identifier id(const std::uint16_t value) {
  chronos::common::Uuid::Bytes bytes{};
  bytes[14] = static_cast<std::byte>((value >> 8U) & 0xffU);
  bytes[15] = static_cast<std::byte>(value & 0xffU);
  return Identifier::from_bytes(bytes).value();
}

template <typename Identifier> [[nodiscard]] Identifier request_id(const std::uint8_t seed) {
  chronos::common::Uuid::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(seed + index);
  }
  return Identifier::from_bytes(bytes).value();
}

class TemporaryWalDirectory {
public:
  [[nodiscard]] static std::optional<TemporaryWalDirectory> create() {
    std::error_code error;
    const std::filesystem::path temporary_root = std::filesystem::temp_directory_path(error);
    if (error) {
      return std::nullopt;
    }
    std::string pattern =
        (temporary_root / "chronos-columnar-append-executor-benchmark-XXXXXX").string();
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
      std::error_code error;
      static_cast<void>(std::filesystem::remove_all(path_, error));
      path_.clear();
    }
  }

  std::filesystem::path path_;
};

struct ExecutorFixture {
  std::shared_ptr<const chronos::columnar::OwnedColumnarBatch> batch;
  chronos::schema::TabletId tablet_id;
  std::size_t encoded_batch_bytes{};
};

[[nodiscard]] ExecutorFixture make_fixture(const std::uint32_t rows) {
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
      chronos::columnar::OwnedColumnarBatch::create(std::move(schema), std::move(columns)).value());
  const std::size_t encoded_bytes =
      chronos::columnar::encode_columnar_batch_v1(*batch).value().size();
  return ExecutorFixture{.batch = std::move(batch),
                         .tablet_id = id<chronos::schema::TabletId>(92U),
                         .encoded_batch_bytes = encoded_bytes};
}

void benchmark_execute(benchmark::State& state, const chronos::wal::WalDurabilityMode durability) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const ExecutorFixture fixture = make_fixture(rows);
  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    {
      auto directory = TemporaryWalDirectory::create();
      if (!directory.has_value()) {
        state.SkipWithError("could not create a temporary WAL directory");
        return;
      }
      auto writer =
          chronos::wal::WalWriter::create_new({.directory_path = directory->path().string()});
      if (!writer.has_value()) {
        const std::string message = writer.error().to_string();
        state.SkipWithError(message);
        return;
      }
      auto started = chronos::wal::WalCommitCoordinator::start(
          std::move(*writer), {.maximum_sync_batch_delay = std::chrono::microseconds{0}});
      if (!started.has_value()) {
        const std::string message = started.error().to_string();
        state.SkipWithError(message);
        return;
      }
      chronos::wal::WalCommitCoordinator coordinator = std::move(*started);
      auto retry_directory = chronos::ingest::RetryDirectory::create({.maximum_entries = 1U});
      auto tablet = chronos::ingest::TabletState::create(
          fixture.batch->schema_ptr(), fixture.tablet_id,
          {.head_capacity = {.row_capacity = rows, .variable_value_bytes = {0U}},
           .maximum_sealed_generations = 1U,
           .maximum_retry_entries = 1U});
      if (!retry_directory.has_value() || !tablet.has_value()) {
        static_cast<void>(coordinator.shutdown());
        state.SkipWithError("could not create bounded executor state");
        return;
      }
      const chronos::ingest::ColumnarAppendExecutionInput input{
          .client_id = request_id<chronos::ingest::ClientId>(16U),
          .client_batch_id = request_id<chronos::ingest::ClientBatchId>(32U),
          .batch = fixture.batch,
          .durability = durability};
      state.ResumeTiming();

      auto result =
          chronos::ingest::execute_columnar_append(input, *retry_directory, *tablet, coordinator);
      benchmark::DoNotOptimize(result.has_value() ? result->outcome.get() : nullptr);
      benchmark::ClobberMemory();

      state.PauseTiming();
      const chronos::common::Status shutdown = coordinator.shutdown();
      if (!result.has_value()) {
        const std::string message = result.error().to_string();
        state.SkipWithError(message);
        return;
      }
      if (!shutdown.is_ok()) {
        const std::string message = shutdown.to_string();
        state.SkipWithError(message);
        return;
      }
    }
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<std::int64_t>(rows));
  state.SetBytesProcessed(state.iterations() *
                          static_cast<std::int64_t>(fixture.encoded_batch_bytes));
}

void benchmark_execute_async(benchmark::State& state) {
  benchmark_execute(state, chronos::wal::WalDurabilityMode::kAsync);
  state.SetLabel(
      "canonical encode + retry + real WAL write + tablet publication; lifecycle excluded");
}

void benchmark_execute_local_sync(benchmark::State& state) {
  benchmark_execute(state, chronos::wal::WalDurabilityMode::kLocalSync);
  state.SetLabel("canonical encode + retry + real WAL write/fdatasync + tablet publication; "
                 "lifecycle excluded");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_execute_async)->Arg(64)->Arg(1024)->UseRealTime();
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_execute_local_sync)->Arg(64)->Arg(1024)->UseRealTime();

} // namespace
