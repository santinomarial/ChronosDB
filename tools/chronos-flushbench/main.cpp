#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/common/version.hpp"
#include "chronos/cseg/compression.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/sealed_head_flush_queue.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/sealed_head_flush_coordinator.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_writer.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#if defined(__APPLE__)
#include <sys/sysctl.h>
#endif

namespace {

using chronos::common::Result;
using chronos::common::Status;
using chronos::common::StatusCode;

constexpr std::string_view kScenarioVersion = "manifest-sealed-head-flush/1";
constexpr std::uint64_t kDefaultMaximumArtifactBytes = std::uint64_t{2U} << 30U;

struct Options {
  std::filesystem::path output_directory;
  std::uint64_t flushes{16U};
  std::uint64_t warmup_flushes{2U};
  std::uint64_t repetitions{3U};
  std::uint64_t seed{0x464c55534842454eULL};
  std::uint32_t rows_per_head{1'024U};
  std::size_t snapshot_readers{1U};
  std::uint64_t baseline_snapshots{10'000U};
  std::uint64_t maximum_foreground_samples{1'000'000U};
  std::uint64_t maximum_artifact_bytes{kDefaultMaximumArtifactBytes};
  chronos::cseg::PageCompression compression{chronos::cseg::PageCompression::kZstd};
  bool allow_dirty{false};
  bool allow_non_release{false};
  std::vector<std::string> invocation;
};

struct FlushSample {
  std::uint64_t ordinal{};
  std::uint64_t latency_ns{};
  std::uint64_t rows{};
  std::uint64_t encoded_part_bytes{};
  std::uint64_t selected_manifest_bytes{};
  std::uint64_t durable_final_bytes{};
  std::uint64_t peak_candidate_bytes{};
  std::uint64_t file_syncs{};
  std::uint64_t directory_syncs{};
};

enum class ForegroundPhase : std::uint8_t {
  kBaseline,
  kFlush,
};

struct ForegroundSample {
  ForegroundPhase phase{};
  std::size_t reader{};
  std::uint64_t ordinal{};
  std::uint64_t latency_ns{};
};

struct ResourceSnapshot {
  std::uint64_t user_cpu_us{};
  std::uint64_t system_cpu_us{};
  std::uint64_t maximum_rss_bytes{};
  std::optional<std::uint64_t> disk_read_bytes;
  std::optional<std::uint64_t> disk_written_bytes;
};

struct RepetitionResult {
  std::uint64_t index{};
  std::uint64_t measured_elapsed_ns{};
  std::uint64_t baseline_elapsed_ns{};
  std::uint64_t manifest_startup_ns{};
  std::uint64_t wal_replay_ns{};
  std::uint64_t repeated_manifest_startup_ns{};
  std::uint64_t repeated_wal_replay_ns{};
  std::uint64_t manifest_generation{};
  std::uint64_t part_count{};
  std::uint64_t retry_count{};
  std::uint64_t selected_manifest_bytes{};
  std::uint64_t manifest_directory_bytes{};
  std::uint64_t part_directory_bytes{};
  std::uint64_t logical_user_bytes{};
  std::uint64_t encoded_part_bytes{};
  std::uint64_t peak_candidate_bytes{};
  std::uint64_t file_syncs{};
  std::uint64_t directory_syncs{};
  std::uint64_t replayed_records{};
  std::uint64_t replayed_rows{};
  std::uint64_t baseline_snapshot_count{};
  std::uint64_t flush_snapshot_count{};
  std::uint64_t foreground_samples_dropped{};
  ResourceSnapshot resources_before;
  ResourceSnapshot resources_after;
  std::vector<FlushSample> flush_samples;
  std::vector<ForegroundSample> foreground_samples;
};

struct AppendInfo {
  chronos::manifest::RetryDescriptor retry;
};

[[nodiscard]] Status invalid(std::string message) {
  return Status{StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] Status io_error(std::string message) {
  return Status{StatusCode::kIoError, std::move(message)};
}

[[nodiscard]] Status corruption(std::string message) {
  return Status{StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] std::string json_escape(const std::string_view input) {
  std::string output;
  output.reserve(input.size() + 8U);
  for (const char character : input) {
    const auto byte = static_cast<unsigned char>(character);
    switch (byte) {
    case '"':
      output.append("\\\"");
      break;
    case '\\':
      output.append("\\\\");
      break;
    case '\b':
      output.append("\\b");
      break;
    case '\f':
      output.append("\\f");
      break;
    case '\n':
      output.append("\\n");
      break;
    case '\r':
      output.append("\\r");
      break;
    case '\t':
      output.append("\\t");
      break;
    default:
      if (byte < 0x20U || byte >= 0x80U) {
        constexpr std::string_view kHex = "0123456789abcdef";
        output.append("\\u00");
        output.push_back(kHex[byte >> 4U]);
        output.push_back(kHex[byte & 0x0fU]);
      } else {
        output.push_back(static_cast<char>(byte));
      }
    }
  }
  return output;
}

[[nodiscard]] std::string quoted(const std::string_view value) {
  return "\"" + json_escape(value) + "\"";
}

template <typename Integer>
[[nodiscard]] bool parse_integer(const std::string_view text, Integer& value) {
  if (text.empty()) {
    return false;
  }
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end == text.data() + text.size();
}

[[nodiscard]] std::string compression_name(const chronos::cseg::PageCompression compression) {
  return compression == chronos::cseg::PageCompression::kZstd ? "ZSTD" : "RAW";
}

void print_usage(const std::string_view program) {
  std::cerr << "Usage: " << program << " --output-dir PATH [options]\n"
            << "  --flushes N --warmup-flushes N --repetitions N --rows-per-head N\n"
            << "  --snapshot-readers N --baseline-snapshots N --compression RAW|ZSTD\n"
            << "  --seed N --maximum-foreground-samples N --maximum-artifact-bytes N\n"
            << "  --allow-dirty --allow-non-release\n";
}

[[nodiscard]] Result<Options> parse_options(const int argc, char** const argv) {
  Options options;
  options.invocation.reserve(static_cast<std::size_t>(argc));
  for (int index = 0; index < argc; ++index) {
    options.invocation.emplace_back(argv[index]);
  }
  for (int index = 1; index < argc; ++index) {
    const std::string_view key{argv[index]};
    if (key == "--allow-dirty") {
      options.allow_dirty = true;
      continue;
    }
    if (key == "--allow-non-release") {
      options.allow_non_release = true;
      continue;
    }
    if (key == "--help") {
      return chronos::common::make_unexpected(Status{StatusCode::kCancelled, "help requested"});
    }
    if (index + 1 >= argc) {
      return chronos::common::make_unexpected(invalid("missing value for " + std::string{key}));
    }
    const std::string_view value{argv[++index]};
    if (key == "--output-dir") {
      options.output_directory = value;
    } else if (key == "--flushes") {
      if (!parse_integer(value, options.flushes)) {
        return chronos::common::make_unexpected(invalid("invalid --flushes value"));
      }
    } else if (key == "--warmup-flushes") {
      if (!parse_integer(value, options.warmup_flushes)) {
        return chronos::common::make_unexpected(invalid("invalid --warmup-flushes value"));
      }
    } else if (key == "--repetitions") {
      if (!parse_integer(value, options.repetitions)) {
        return chronos::common::make_unexpected(invalid("invalid --repetitions value"));
      }
    } else if (key == "--rows-per-head") {
      if (!parse_integer(value, options.rows_per_head)) {
        return chronos::common::make_unexpected(invalid("invalid --rows-per-head value"));
      }
    } else if (key == "--snapshot-readers") {
      if (!parse_integer(value, options.snapshot_readers)) {
        return chronos::common::make_unexpected(invalid("invalid --snapshot-readers value"));
      }
    } else if (key == "--baseline-snapshots") {
      if (!parse_integer(value, options.baseline_snapshots)) {
        return chronos::common::make_unexpected(invalid("invalid --baseline-snapshots value"));
      }
    } else if (key == "--maximum-foreground-samples") {
      if (!parse_integer(value, options.maximum_foreground_samples)) {
        return chronos::common::make_unexpected(
            invalid("invalid --maximum-foreground-samples value"));
      }
    } else if (key == "--maximum-artifact-bytes") {
      if (!parse_integer(value, options.maximum_artifact_bytes)) {
        return chronos::common::make_unexpected(invalid("invalid --maximum-artifact-bytes value"));
      }
    } else if (key == "--seed") {
      if (!parse_integer(value, options.seed)) {
        return chronos::common::make_unexpected(invalid("invalid --seed value"));
      }
    } else if (key == "--compression") {
      const bool raw = value == "RAW";
      const bool zstd = value == "ZSTD";
      if (!raw && !zstd) {
        return chronos::common::make_unexpected(invalid("--compression must be RAW or ZSTD"));
      }
      options.compression =
          raw ? chronos::cseg::PageCompression::kNone : chronos::cseg::PageCompression::kZstd;
    } else {
      return chronos::common::make_unexpected(invalid("unknown option " + std::string{key}));
    }
  }
  return options;
}

[[nodiscard]] Status validate_options(const Options& options) {
  if (options.output_directory.empty()) {
    return invalid("--output-dir is required");
  }
  if (options.flushes == 0U || options.repetitions == 0U || options.rows_per_head == 0U ||
      options.snapshot_readers == 0U || options.baseline_snapshots == 0U) {
    return invalid("flushes, repetitions, rows, readers, and baseline snapshots must be nonzero");
  }
  if (options.flushes > std::numeric_limits<std::uint64_t>::max() - options.warmup_flushes - 1U) {
    return invalid("configured flush count overflows the record sequence space");
  }
  const std::uint64_t total_flushes = options.flushes + options.warmup_flushes;
  if (total_flushes > 100'000U || options.repetitions > 1'000U || options.snapshot_readers > 256U) {
    return invalid("configured run exceeds the harness safety bounds");
  }
  const std::uint64_t reader_count = static_cast<std::uint64_t>(options.snapshot_readers);
  if (options.maximum_foreground_samples < reader_count ||
      options.baseline_snapshots >
          (options.maximum_foreground_samples - reader_count) / reader_count) {
    return invalid("baseline samples leave no bounded during-flush sample per reader");
  }
  constexpr std::uint64_t kFixedArtifactAllowance = 1U << 20U;
  if (options.maximum_artifact_bytes <= kFixedArtifactAllowance) {
    return invalid("--maximum-artifact-bytes is too small for fixed metadata");
  }
  const std::uint64_t logical_bytes_per_part =
      static_cast<std::uint64_t>(options.rows_per_head) * sizeof(std::int64_t);
  const std::uint64_t generous_bytes_per_flush = logical_bytes_per_part * 8U + (1U << 20U);
  constexpr std::uint64_t kMaximumForegroundSampleTextBytes = 128U;
  const std::uint64_t variable_budget = options.maximum_artifact_bytes - kFixedArtifactAllowance;
  if (generous_bytes_per_flush > variable_budget / total_flushes / options.repetitions ||
      options.maximum_foreground_samples >
          variable_budget / kMaximumForegroundSampleTextBytes / options.repetitions) {
    return invalid("configured run may exceed --maximum-artifact-bytes");
  }
  const std::uint64_t image_budget = generous_bytes_per_flush * total_flushes * options.repetitions;
  const std::uint64_t sample_budget =
      options.maximum_foreground_samples * kMaximumForegroundSampleTextBytes * options.repetitions;
  if (image_budget > variable_budget || sample_budget > variable_budget - image_budget) {
    return invalid("configured run may exceed --maximum-artifact-bytes");
  }
  const chronos::common::VersionInfo version = chronos::common::version_info();
  if (version.git_dirty && !options.allow_dirty) {
    return invalid("working tree was dirty at build time; use --allow-dirty and retain a diff");
  }
  if (version.build_type != "Release" && !options.allow_non_release) {
    return invalid("benchmark build is not Release; use --allow-non-release only for smoke tests");
  }
  return Status::ok();
}

template <typename Identifier>
// Seed, ordinal, and domain are intentionally adjacent parts of one deterministic test identity.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] Identifier make_id(const std::uint64_t seed, const std::uint64_t ordinal,
                                 const std::uint8_t domain) {
  chronos::common::Uuid::Bytes bytes{};
  bytes[0] = std::byte{domain};
  for (std::size_t index = 0U; index < sizeof(seed); ++index) {
    bytes[index + 1U] = std::byte{static_cast<std::uint8_t>((seed >> (index * 8U)) & 0xffU)};
  }
  for (std::size_t index = 0U; index < 7U; ++index) {
    bytes[index + 9U] = std::byte{static_cast<std::uint8_t>((ordinal >> (index * 8U)) & 0xffU)};
  }
  return Identifier::from_bytes(bytes).value();
}

// Seed, ordinal, and domain are intentionally adjacent parts of one deterministic nonce.
[[nodiscard]] chronos::common::Uuid
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
make_nonce(const std::uint64_t seed, const std::uint64_t ordinal, const std::uint8_t domain) {
  chronos::common::Uuid::Bytes bytes{};
  bytes[0] = std::byte{domain};
  for (std::size_t index = 0U; index < sizeof(seed); ++index) {
    bytes[index + 1U] = std::byte{static_cast<std::uint8_t>((seed >> (index * 8U)) & 0xffU)};
  }
  for (std::size_t index = 0U; index < 7U; ++index) {
    bytes[index + 9U] = std::byte{static_cast<std::uint8_t>((ordinal >> (index * 8U)) & 0xffU)};
  }
  return chronos::common::Uuid{bytes};
}

template <typename Integer> void append_le(std::vector<std::byte>& bytes, const Integer value) {
  using Unsigned = std::make_unsigned_t<Integer>;
  const Unsigned encoded = std::bit_cast<Unsigned>(value);
  for (std::size_t index = 0U; index < sizeof(Integer); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(encoded >> (index * 8U))});
  }
}

[[nodiscard]] std::shared_ptr<const chronos::schema::TableSchema>
make_schema(const std::uint64_t seed) {
  const auto table_id = make_id<chronos::schema::TableId>(seed, 1U, 1U);
  const auto schema_id = make_id<chronos::schema::SchemaId>(seed, 1U, 2U);
  const auto event_id = make_id<chronos::schema::ColumnId>(seed, 1U, 3U);
  std::vector<chronos::schema::ColumnDefinition> columns;
  columns.push_back(
      chronos::schema::ColumnDefinition::create(
          event_id, "event_time",
          chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kTimestampNs)
              .value(),
          false)
          .value());
  return std::make_shared<const chronos::schema::TableSchema>(
      chronos::schema::TableSchema::create(table_id, schema_id,
                                           chronos::schema::SchemaVersion::initial(), std::nullopt,
                                           std::move(columns),
                                           {.event_time_column = event_id,
                                            .physical_ordering_key = {event_id},
                                            .partition_columns = {event_id},
                                            .shard_key = {event_id},
                                            .deduplication_key = {}})
          .value());
}

[[nodiscard]] std::shared_ptr<const chronos::columnar::OwnedColumnarBatch>
make_batch(const std::shared_ptr<const chronos::schema::TableSchema>& schema,
           const std::uint32_t rows, const std::uint64_t batch_ordinal) {
  std::vector<std::byte> values;
  values.reserve(static_cast<std::size_t>(rows) * sizeof(std::int64_t));
  const std::uint64_t base = batch_ordinal * static_cast<std::uint64_t>(rows);
  for (std::uint32_t row = rows; row > 0U; --row) {
    append_le(values, static_cast<std::int64_t>(base + row));
  }
  std::vector<chronos::columnar::OwnedColumnVector> columns;
  columns.push_back(chronos::columnar::OwnedColumnVector::create(
                        {.column_id = schema->event_time_column(),
                         .type = schema->columns().front().type(),
                         .nullable = false,
                         .row_count = rows,
                         .null_count = 0U},
                        {.validity = {}, .offsets = {}, .values = std::move(values)})
                        .value());
  return std::make_shared<const chronos::columnar::OwnedColumnarBatch>(
      chronos::columnar::OwnedColumnarBatch::create(schema, std::move(columns)).value());
}

class FixedWalIdGenerator final : public chronos::wal::WalLogIdGenerator {
public:
  explicit FixedWalIdGenerator(const chronos::wal::WalId id) noexcept : id_(id) {}
  [[nodiscard]] Result<chronos::wal::WalId> generate() override {
    return id_;
  }

private:
  chronos::wal::WalId id_;
};

[[nodiscard]] Status write_bytes(const std::filesystem::path& path,
                                 const chronos::common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  // std::ofstream has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  output.flush();
  return output.good() ? Status::ok() : io_error("failed to write " + path.string());
}

[[nodiscard]] Status write_text(const std::filesystem::path& path, const std::string_view text) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  output.write(text.data(), static_cast<std::streamsize>(text.size()));
  output.flush();
  return output.good() ? Status::ok()
                       : io_error("failed to write benchmark artifact " + path.string());
}

[[nodiscard]] Result<std::uint64_t> directory_file_bytes(const std::filesystem::path& path) {
  std::error_code error;
  std::uint64_t total = 0U;
  for (std::filesystem::directory_iterator iterator{path, error}, end; iterator != end && !error;
       iterator.increment(error)) {
    if (!iterator->is_regular_file(error) || error) {
      if (error) {
        break;
      }
      continue;
    }
    const std::uint64_t size = iterator->file_size(error);
    if (error || total > std::numeric_limits<std::uint64_t>::max() - size) {
      return chronos::common::make_unexpected(io_error("cannot measure benchmark directory bytes"));
    }
    total += size;
  }
  if (error) {
    return chronos::common::make_unexpected(io_error("cannot enumerate benchmark directory bytes"));
  }
  return total;
}

[[nodiscard]] std::uint64_t timeval_microseconds(const timeval& value) {
  return static_cast<std::uint64_t>(value.tv_sec) * 1'000'000U +
         static_cast<std::uint64_t>(value.tv_usec);
}

[[nodiscard]] std::optional<std::uint64_t> read_proc_io_value(const std::string_view key) {
#if defined(__linux__)
  std::ifstream input{"/proc/self/io"};
  std::string name;
  std::uint64_t value = 0U;
  while (input >> name >> value) {
    if (name == key) {
      return value;
    }
  }
#else
  static_cast<void>(key);
#endif
  return std::nullopt;
}

[[nodiscard]] ResourceSnapshot resource_snapshot() {
  rusage usage{};
  static_cast<void>(::getrusage(RUSAGE_SELF, &usage));
#if defined(__APPLE__)
  const std::uint64_t maximum_rss = static_cast<std::uint64_t>(usage.ru_maxrss);
#else
  const std::uint64_t maximum_rss = static_cast<std::uint64_t>(usage.ru_maxrss) * 1024U;
#endif
  return {.user_cpu_us = timeval_microseconds(usage.ru_utime),
          .system_cpu_us = timeval_microseconds(usage.ru_stime),
          .maximum_rss_bytes = maximum_rss,
          .disk_read_bytes = read_proc_io_value("read_bytes:"),
          .disk_written_bytes = read_proc_io_value("write_bytes:")};
}

// The two ordinals and three distinct owners are explicit fixture inputs at every call site.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] Result<AppendInfo>
append_batch(const Options& options, const std::uint64_t repetition, const std::uint64_t ordinal,
             const std::shared_ptr<const chronos::schema::TableSchema>& schema,
             const chronos::schema::TabletId& tablet_id, chronos::wal::WalWriter& writer,
             chronos::ingest::TabletState& tablet) {
  const auto batch = make_batch(schema, options.rows_per_head, ordinal);
  const Result<chronos::columnar::EncodedColumnarBatch> encoded_batch =
      chronos::columnar::encode_columnar_batch_v1(*batch);
  if (!encoded_batch.has_value()) {
    return chronos::common::make_unexpected(encoded_batch.error());
  }
  const chronos::ingest::RetryIdentity identity{
      .client_id = make_id<chronos::ingest::ClientId>(options.seed ^ repetition, ordinal + 1U, 4U),
      .client_batch_id =
          make_id<chronos::ingest::ClientBatchId>(options.seed ^ repetition, ordinal + 1U, 5U)};
  const Result<chronos::wal::EncodedApplicationPayload> application =
      chronos::ingest::encode_columnar_append_v1({.client_id = identity.client_id,
                                                  .client_batch_id = identity.client_batch_id,
                                                  .tablet_id = tablet_id},
                                                 *encoded_batch);
  if (!application.has_value()) {
    return chronos::common::make_unexpected(application.error());
  }
  const auto decoded = chronos::ingest::decode_columnar_append_v1_exact(application->bytes());
  if (!decoded.has_value()) {
    return chronos::common::make_unexpected(decoded.error().status());
  }
  const chronos::ingest::ColumnarAppendMutationIdentity mutation{.table_id = decoded->table_id(),
                                                                 .tablet_id = decoded->tablet_id(),
                                                                 .request_digest =
                                                                     decoded->request_digest()};
  Result<chronos::ingest::PreparedTabletAppend> prepared =
      tablet.prepare_append(identity, mutation, batch);
  if (!prepared.has_value()) {
    return chronos::common::make_unexpected(prepared.error());
  }
  const Result<chronos::wal::WalAppendResult> appended =
      writer.append_application_entry(application->bytes());
  if (!appended.has_value()) {
    return chronos::common::make_unexpected(appended.error());
  }
  Status status = prepared->mark_wal_started();
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }
  Result<chronos::ingest::TabletAppendResult> published = prepared->publish(
      {.wal_id = appended->record_start.wal_id, .record_sequence = appended->record_sequence});
  if (!published.has_value()) {
    return chronos::common::make_unexpected(published.error());
  }
  return AppendInfo{.retry = {.client_id = identity.client_id,
                              .client_batch_id = identity.client_batch_id,
                              .table_id = mutation.table_id,
                              .tablet_id = mutation.tablet_id,
                              .request_digest = mutation.request_digest,
                              .wal_id = appended->record_start.wal_id,
                              .record_sequence = appended->record_sequence,
                              .applied_row_count = options.rows_per_head}};
}

class VerifyingReplaySink final : public chronos::wal::WalReplaySink {
public:
  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
  VerifyingReplaySink(const std::uint64_t expected_records, const std::uint64_t expected_rows)
      : expected_records_(expected_records), expected_rows_(expected_rows) {}

  [[nodiscard]] Status preflight(const chronos::wal::WalReplayRecord& record) override {
    return validate(record, false);
  }

  [[nodiscard]] Status replay(const chronos::wal::WalReplayRecord& record) override {
    return validate(record, true);
  }

  [[nodiscard]] Status finish() const {
    if (preflighted_ != expected_records_ || replayed_ != expected_records_ ||
        replayed_rows_ != expected_rows_) {
      return corruption("WAL suffix recovery did not reproduce the expected commands and rows");
    }
    return Status::ok();
  }

  [[nodiscard]] std::uint64_t replayed() const noexcept {
    return replayed_;
  }
  [[nodiscard]] std::uint64_t replayed_rows() const noexcept {
    return replayed_rows_;
  }

private:
  [[nodiscard]] Status validate(const chronos::wal::WalReplayRecord& record, const bool replay) {
    const std::uint64_t expected_sequence = replay ? replayed_ + 1U : preflighted_ + 1U;
    if (record.header.record_sequence != expected_sequence) {
      return corruption("WAL suffix recovery observed a sequence gap");
    }
    const auto command = chronos::ingest::decode_columnar_append_v1_record(
        chronos::wal::DecodedRecord{.header = record.header, .payload = record.payload});
    if (!command.has_value()) {
      return command.error().status();
    }
    if (replay) {
      ++replayed_;
      replayed_rows_ += command->row_count();
    } else {
      ++preflighted_;
    }
    return Status::ok();
  }

  std::uint64_t expected_records_{};
  std::uint64_t expected_rows_{};
  std::uint64_t preflighted_{};
  std::uint64_t replayed_{};
  std::uint64_t replayed_rows_{};
};

struct ForegroundRun {
  std::uint64_t elapsed_ns{};
  std::uint64_t completed{};
  std::uint64_t dropped{};
  std::vector<ForegroundSample> samples;
};

template <typename Work>
[[nodiscard]] Result<ForegroundRun>
run_foreground(const Options& options, chronos::manifest::DatabaseStoragePublisher& publisher,
               const ForegroundPhase phase, const std::uint64_t sample_budget, Work&& work) {
  std::atomic<bool> begin{false};
  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};
  std::atomic<std::size_t> ready{0U};
  std::atomic<std::uint64_t> completed{0U};
  std::atomic<std::uint64_t> dropped{0U};
  std::mutex sample_mutex;
  std::vector<ForegroundSample> samples;
  samples.reserve(static_cast<std::size_t>(std::min<std::uint64_t>(
      sample_budget,
      options.baseline_snapshots * static_cast<std::uint64_t>(options.snapshot_readers))));
  const std::uint64_t sample_limit_per_reader =
      sample_budget / static_cast<std::uint64_t>(options.snapshot_readers);
  std::vector<std::thread> readers;
  readers.reserve(options.snapshot_readers);
  for (std::size_t reader = 0U; reader < options.snapshot_readers; ++reader) {
    readers.emplace_back([&, reader] {
      std::vector<ForegroundSample> local;
      local.reserve(static_cast<std::size_t>(
          std::min<std::uint64_t>(sample_limit_per_reader, options.baseline_snapshots)));
      std::uint64_t sample_state =
          options.seed ^ (static_cast<std::uint64_t>(reader) * 0x9e3779b97f4a7c15ULL) ^
          (phase == ForegroundPhase::kBaseline ? 0x424153454c494e45ULL : 0x464c55534853414dULL);
      if (sample_state == 0U) {
        sample_state = 0x4348524f4e4f5355ULL;
      }
      ready.fetch_add(1U, std::memory_order_release);
      while (!begin.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      std::uint64_t ordinal = 0U;
      do {
        const auto started = std::chrono::steady_clock::now();
        const Result<chronos::manifest::DatabaseStorageSnapshot> snapshot = publisher.snapshot();
        const auto finished = std::chrono::steady_clock::now();
        if (!snapshot.has_value() || (snapshot->visible_head_row_count() != options.rows_per_head &&
                                      snapshot->visible_head_row_count() !=
                                          static_cast<std::size_t>(options.rows_per_head) * 2U)) {
          failed.store(true, std::memory_order_release);
          break;
        }
        const std::uint64_t latency = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());
        const ForegroundSample observation{
            .phase = phase, .reader = reader, .ordinal = ordinal, .latency_ns = latency};
        if (local.size() < sample_limit_per_reader) {
          local.push_back(observation);
        } else {
          sample_state ^= sample_state >> 12U;
          sample_state ^= sample_state << 25U;
          sample_state ^= sample_state >> 27U;
          const std::uint64_t candidate = (sample_state * 0x2545f4914f6cdd1dULL) % (ordinal + 1U);
          if (candidate < sample_limit_per_reader) {
            local[static_cast<std::size_t>(candidate)] = observation;
          }
          dropped.fetch_add(1U, std::memory_order_relaxed);
        }
        ++ordinal;
        completed.fetch_add(1U, std::memory_order_relaxed);
      } while (phase == ForegroundPhase::kBaseline ? ordinal < options.baseline_snapshots
                                                   : !stop.load(std::memory_order_acquire));
      const std::lock_guard lock{sample_mutex};
      samples.insert(samples.end(), local.begin(), local.end());
    });
  }
  while (ready.load(std::memory_order_acquire) != options.snapshot_readers) {
    std::this_thread::yield();
  }
  const auto started = std::chrono::steady_clock::now();
  begin.store(true, std::memory_order_release);
  const Status work_status = work();
  stop.store(true, std::memory_order_release);
  for (std::thread& reader : readers) {
    reader.join();
  }
  const auto finished = std::chrono::steady_clock::now();
  if (!work_status.is_ok()) {
    return chronos::common::make_unexpected(work_status);
  }
  if (failed.load(std::memory_order_acquire)) {
    return chronos::common::make_unexpected(
        corruption("foreground snapshot reader observed an invalid publication"));
  }
  return ForegroundRun{
      .elapsed_ns = static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count()),
      .completed = completed.load(std::memory_order_relaxed),
      .dropped = dropped.load(std::memory_order_relaxed),
      .samples = std::move(samples)};
}

// The ordinals and state owners are deliberately explicit at the storage-owner call boundary.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] Status
flush_one(const Options& options, const std::uint64_t repetition, const std::uint64_t flush_ordinal,
          const AppendInfo& append, chronos::ingest::TabletState& tablet,
          chronos::manifest::ManifestStorage& storage,
          chronos::manifest::SealedHeadFlushCoordinator& coordinator,
          const std::span<const chronos::manifest::TabletSchemaBinding> bindings,
          FlushSample* const sample) {
  const std::array retries{append.retry};
  const auto part_id =
      make_id<chronos::cseg::PartId>(options.seed ^ repetition, flush_ordinal + 1U, 6U);
  const chronos::manifest::SealedHeadFlushCoordinatorMetrics before = coordinator.metrics();
  const chronos::manifest::PartInstallationMetrics part_before = storage.metrics();
  const chronos::manifest::ManifestInstallationMetrics manifest_before = storage.manifest_metrics();
  const auto started = std::chrono::steady_clock::now();
  const auto completed = coordinator.try_flush_one(
      tablet, {.part_id = part_id,
               .part_nonce = make_nonce(options.seed ^ repetition, flush_ordinal + 1U, 7U),
               .manifest_nonce = make_nonce(options.seed ^ repetition, flush_ordinal + 1U, 8U),
               .compression = options.compression,
               .new_retries = retries,
               .schema_bindings = bindings,
               .manifest_decode_limits = {},
               .part_validation_limits = {}});
  const auto finished = std::chrono::steady_clock::now();
  if (!completed.has_value()) {
    return completed.error();
  }
  if (!completed->has_value()) {
    return corruption("flush coordinator reported empty work for a queued sealed generation");
  }
  const auto after = coordinator.metrics();
  const auto part_after = storage.metrics();
  const auto manifest_after = storage.manifest_metrics();
  const Result<chronos::manifest::ManifestNamespaceSnapshot> names = storage.scan_namespace();
  if (!names.has_value()) {
    return names.error();
  }
  if (!names->temporary_parts.empty() || !names->temporary_manifests.empty() ||
      names->final_parts.size() != flush_ordinal + 1U ||
      names->generations.back() != flush_ordinal + 2U) {
    return corruption("successful flush left an unexpected durable namespace");
  }
  const Result<std::uint64_t> part_bytes =
      directory_file_bytes(std::filesystem::path{options.output_directory} /
                           ("repetition-" + std::to_string(repetition)) / "database" /
                           chronos::manifest::kPartsDirectoryName);
  const Result<std::uint64_t> manifest_bytes =
      directory_file_bytes(std::filesystem::path{options.output_directory} /
                           ("repetition-" + std::to_string(repetition)) / "database" /
                           chronos::manifest::kManifestDirectoryName);
  if (!part_bytes.has_value() || !manifest_bytes.has_value()) {
    return !part_bytes.has_value() ? part_bytes.error() : manifest_bytes.error();
  }
  const std::uint64_t encoded_delta = after.encoded_bytes - before.encoded_bytes;
  const std::uint64_t manifest_delta =
      manifest_after.installed_bytes - manifest_before.installed_bytes;
  if (sample != nullptr) {
    *sample = {
        .ordinal = flush_ordinal,
        .latency_ns = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count()),
        .rows = options.rows_per_head,
        .encoded_part_bytes = encoded_delta,
        .selected_manifest_bytes = manifest_delta,
        .durable_final_bytes = *part_bytes + *manifest_bytes,
        .peak_candidate_bytes = std::max(encoded_delta, manifest_delta),
        .file_syncs = (part_after.file_syncs - part_before.file_syncs) +
                      (manifest_after.file_syncs - manifest_before.file_syncs),
        .directory_syncs = (part_after.directory_syncs - part_before.directory_syncs) +
                           (manifest_after.directory_syncs - manifest_before.directory_syncs)};
  }
  return Status::ok();
}

[[nodiscard]] Result<RepetitionResult> run_repetition(const Options& options,
                                                      const std::uint64_t repetition) {
  RepetitionResult result;
  result.index = repetition;
  const std::filesystem::path repetition_root =
      options.output_directory / ("repetition-" + std::to_string(repetition));
  const std::filesystem::path database_root = repetition_root / "database";
  const std::filesystem::path parts_path = database_root / chronos::manifest::kPartsDirectoryName;
  const std::filesystem::path manifest_path =
      database_root / chronos::manifest::kManifestDirectoryName;
  const std::filesystem::path wal_path = database_root / "wal";
  std::error_code error;
  if (!std::filesystem::create_directories(parts_path, error) || error ||
      !std::filesystem::create_directory(manifest_path, error) || error ||
      !std::filesystem::create_directory(wal_path, error) || error) {
    return chronos::common::make_unexpected(
        io_error("cannot create flush benchmark repetition directories"));
  }
  Status status =
      write_bytes(manifest_path / std::string{chronos::manifest::kManifestLockFileName}, {});
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }

  const std::uint64_t fixture_seed = options.seed ^ (repetition * 0x9e3779b97f4a7c15ULL);
  const auto schema = make_schema(fixture_seed);
  const auto tablet_id = make_id<chronos::schema::TabletId>(fixture_seed, 1U, 9U);
  const auto database_id = make_id<chronos::manifest::DatabaseId>(fixture_seed, 1U, 10U);
  chronos::wal::WalId wal_id{};
  const chronos::common::Uuid wal_uuid = make_nonce(fixture_seed, 1U, 11U);
  wal_id.bytes = wal_uuid.bytes();
  FixedWalIdGenerator wal_generator{wal_id};
  Result<chronos::wal::WalWriter> writer_result =
      chronos::wal::WalWriter::create_new({.directory_path = wal_path.string()}, wal_generator);
  if (!writer_result.has_value()) {
    return chronos::common::make_unexpected(writer_result.error());
  }
  chronos::wal::WalWriter writer = std::move(*writer_result);
  const auto lineage = chronos::schema::SchemaLineage::create(*schema);
  if (!lineage.has_value()) {
    return chronos::common::make_unexpected(lineage.error());
  }
  const std::array bindings{chronos::manifest::TabletSchemaBinding{.tablet_id = tablet_id,
                                                                   .lineage = std::cref(*lineage)}};
  auto queue_result = chronos::ingest::SealedHeadFlushQueue::create({.capacity = 2U});
  if (!queue_result.has_value()) {
    return chronos::common::make_unexpected(queue_result.error());
  }
  std::shared_ptr<chronos::ingest::SealedHeadFlushQueue> queue = std::move(*queue_result);
  const std::uint64_t total_flushes = options.warmup_flushes + options.flushes;
  Result<chronos::ingest::TabletState> tablet_result = chronos::ingest::TabletState::create(
      schema, tablet_id,
      {.head_capacity = {.row_capacity = options.rows_per_head, .variable_value_bytes = {0U}},
       .maximum_schema_versions = 1U,
       .maximum_sealed_generations = 2U,
       .maximum_retry_entries = static_cast<std::size_t>(total_flushes + 1U),
       .flush_queue = queue});
  if (!tablet_result.has_value()) {
    return chronos::common::make_unexpected(tablet_result.error());
  }
  chronos::ingest::TabletState tablet = std::move(*tablet_result);
  std::vector<AppendInfo> appends;
  appends.reserve(static_cast<std::size_t>(total_flushes + 1U));
  for (std::uint64_t ordinal = 0U; ordinal < 2U; ++ordinal) {
    Result<AppendInfo> appended =
        append_batch(options, repetition, ordinal, schema, tablet_id, writer, tablet);
    if (!appended.has_value()) {
      return chronos::common::make_unexpected(appended.error());
    }
    appends.push_back(*appended);
  }
  Result<chronos::wal::PhysicalWalPosition> synchronized = writer.synchronize();
  if (!synchronized.has_value()) {
    return chronos::common::make_unexpected(synchronized.error());
  }
  const chronos::manifest::EncodedManifest initial_manifest =
      chronos::manifest::encode_manifest_v1(
          {.generation = 1U,
           .database_id = database_id,
           .wal_id = wal_id,
           .reclaim_checkpoint = {.record_sequence = 0U, .segment_number = 1U, .byte_offset = 64U},
           .tablets = {},
           .parts = {},
           .retries = {}})
          .value();
  status = write_bytes(manifest_path / *chronos::manifest::manifest_file_name(1U),
                       initial_manifest.bytes());
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }

  {
    Result<chronos::manifest::ManifestStorage> storage_result =
        chronos::manifest::ManifestStorage::open_existing(
            {.database_root = database_root.string()});
    if (!storage_result.has_value()) {
      return chronos::common::make_unexpected(storage_result.error());
    }
    chronos::manifest::ManifestStorage storage = std::move(*storage_result);
    Result<chronos::manifest::LoadedManifestGeneration> loaded =
        storage.load_selected_manifest({.expected_database_id = database_id,
                                        .expected_wal_id = wal_id,
                                        .schema_bindings = {},
                                        .decode_limits = {},
                                        .part_validation_limits = {}});
    if (!loaded.has_value()) {
      return chronos::common::make_unexpected(loaded.error());
    }
    auto selected =
        std::make_shared<const chronos::manifest::LoadedManifestGeneration>(std::move(*loaded));
    const Result<chronos::ingest::TabletSnapshot> tablet_snapshot = tablet.snapshot();
    if (!tablet_snapshot.has_value()) {
      return chronos::common::make_unexpected(tablet_snapshot.error());
    }
    const std::array tablet_inputs{
        chronos::manifest::DatabaseStorageTabletInput{.snapshot = std::cref(*tablet_snapshot)}};
    Result<chronos::manifest::DatabaseStoragePublisher> publisher_result =
        chronos::manifest::DatabaseStoragePublisher::create(selected, tablet_inputs);
    if (!publisher_result.has_value()) {
      return chronos::common::make_unexpected(publisher_result.error());
    }
    chronos::manifest::DatabaseStoragePublisher publisher = std::move(*publisher_result);
    Result<chronos::manifest::SealedHeadFlushCoordinator> coordinator_result =
        chronos::manifest::SealedHeadFlushCoordinator::create(queue, storage, publisher);
    if (!coordinator_result.has_value()) {
      return chronos::common::make_unexpected(coordinator_result.error());
    }
    chronos::manifest::SealedHeadFlushCoordinator coordinator = std::move(*coordinator_result);

    for (std::uint64_t ordinal = 0U; ordinal < options.warmup_flushes; ++ordinal) {
      status = flush_one(options, repetition, ordinal, appends[static_cast<std::size_t>(ordinal)],
                         tablet, storage, coordinator, bindings, nullptr);
      if (!status.is_ok()) {
        return chronos::common::make_unexpected(status);
      }
      Result<AppendInfo> appended =
          append_batch(options, repetition, appends.size(), schema, tablet_id, writer, tablet);
      if (!appended.has_value()) {
        return chronos::common::make_unexpected(appended.error());
      }
      appends.push_back(*appended);
      synchronized = writer.synchronize();
      if (!synchronized.has_value()) {
        return chronos::common::make_unexpected(synchronized.error());
      }
    }

    const std::uint64_t baseline_sample_budget =
        options.baseline_snapshots * static_cast<std::uint64_t>(options.snapshot_readers);
    Result<ForegroundRun> baseline =
        run_foreground(options, publisher, ForegroundPhase::kBaseline, baseline_sample_budget,
                       [] { return Status::ok(); });
    if (!baseline.has_value()) {
      return chronos::common::make_unexpected(baseline.error());
    }
    result.baseline_elapsed_ns = baseline->elapsed_ns;
    result.baseline_snapshot_count = baseline->completed;
    result.foreground_samples_dropped = baseline->dropped;
    result.foreground_samples = std::move(baseline->samples);

    result.resources_before = resource_snapshot();
    Result<ForegroundRun> during = run_foreground(
        options, publisher, ForegroundPhase::kFlush,
        options.maximum_foreground_samples - baseline_sample_budget, [&] {
          result.flush_samples.reserve(static_cast<std::size_t>(options.flushes));
          const auto measured_started = std::chrono::steady_clock::now();
          for (std::uint64_t measured = 0U; measured < options.flushes; ++measured) {
            const std::uint64_t ordinal = options.warmup_flushes + measured;
            FlushSample sample;
            Status flush_status =
                flush_one(options, repetition, ordinal, appends[static_cast<std::size_t>(ordinal)],
                          tablet, storage, coordinator, bindings, &sample);
            if (!flush_status.is_ok()) {
              return flush_status;
            }
            result.flush_samples.push_back(sample);
            if (measured + 1U != options.flushes) {
              Result<AppendInfo> appended = append_batch(options, repetition, appends.size(),
                                                         schema, tablet_id, writer, tablet);
              if (!appended.has_value()) {
                return appended.error();
              }
              appends.push_back(*appended);
              Result<chronos::wal::PhysicalWalPosition> sync = writer.synchronize();
              if (!sync.has_value()) {
                return sync.error();
              }
            }
          }
          const auto measured_finished = std::chrono::steady_clock::now();
          result.measured_elapsed_ns =
              static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                             measured_finished - measured_started)
                                             .count());
          return Status::ok();
        });
    result.resources_after = resource_snapshot();
    if (!during.has_value()) {
      return chronos::common::make_unexpected(during.error());
    }
    result.flush_snapshot_count = during->completed;
    result.foreground_samples_dropped += during->dropped;
    result.foreground_samples.insert(result.foreground_samples.end(), during->samples.begin(),
                                     during->samples.end());

    const auto coordinator_metrics = coordinator.metrics();
    const auto part_metrics = storage.metrics();
    const auto manifest_metrics = storage.manifest_metrics();
    result.encoded_part_bytes = 0U;
    result.peak_candidate_bytes = 0U;
    result.file_syncs = 0U;
    result.directory_syncs = 0U;
    for (const FlushSample& sample : result.flush_samples) {
      result.encoded_part_bytes += sample.encoded_part_bytes;
      result.peak_candidate_bytes =
          std::max(result.peak_candidate_bytes, sample.peak_candidate_bytes);
      result.file_syncs += sample.file_syncs;
      result.directory_syncs += sample.directory_syncs;
    }
    if (coordinator_metrics.completed != total_flushes ||
        part_metrics.installed_parts != total_flushes ||
        manifest_metrics.installed_generations != total_flushes ||
        queue->metrics().occupied != 0U || !tablet.snapshot()->sealed_generations().empty()) {
      return chronos::common::make_unexpected(
          corruption("successful benchmark flushes did not converge to exact component metrics"));
    }
    const Result<chronos::manifest::DatabaseStorageSnapshot> published = publisher.snapshot();
    if (!published.has_value() || published->generation() != total_flushes + 1U ||
        published->parts().size() != total_flushes ||
        published->retries().size() != total_flushes ||
        published->visible_head_row_count() != options.rows_per_head) {
      return chronos::common::make_unexpected(
          corruption("final published database storage epoch disagrees with flushed work"));
    }
  }

  status = writer.close();
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }
  const std::uint64_t expected_records = total_flushes + 1U;
  const std::uint64_t expected_rows =
      expected_records * static_cast<std::uint64_t>(options.rows_per_head);
  std::vector<std::byte> selected_bytes;
  chronos::manifest::WalCheckpoint checkpoint;
  auto load_once = [&](std::uint64_t& elapsed_ns) -> Status {
    const auto started = std::chrono::steady_clock::now();
    Result<chronos::manifest::ManifestStorage> opened =
        chronos::manifest::ManifestStorage::open_existing(
            {.database_root = database_root.string()});
    if (!opened.has_value()) {
      return opened.error();
    }
    Result<chronos::manifest::LoadedManifestGeneration> loaded =
        opened->load_selected_manifest({.expected_database_id = database_id,
                                        .expected_wal_id = wal_id,
                                        .schema_bindings = bindings,
                                        .decode_limits = {},
                                        .part_validation_limits = {}});
    const auto finished = std::chrono::steady_clock::now();
    if (!loaded.has_value()) {
      return loaded.error();
    }
    elapsed_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());
    if (selected_bytes.empty()) {
      selected_bytes.assign(loaded->encoded_bytes().begin(), loaded->encoded_bytes().end());
      checkpoint = loaded->reclaim_checkpoint();
      result.manifest_generation = loaded->generation();
      result.part_count = loaded->parts().size();
      result.retry_count = loaded->retries().size();
      result.selected_manifest_bytes = loaded->encoded_bytes().size();
    } else if (!std::ranges::equal(selected_bytes, loaded->encoded_bytes())) {
      return corruption("repeated Manifest recovery did not select byte-identical state");
    }
    return Status::ok();
  };
  status = load_once(result.manifest_startup_ns);
  if (status.is_ok()) {
    status = load_once(result.repeated_manifest_startup_ns);
  }
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }
  auto replay_once = [&](std::uint64_t& elapsed_ns, const bool retain_counts) -> Status {
    VerifyingReplaySink sink{expected_records, expected_rows};
    const auto started = std::chrono::steady_clock::now();
    const auto recovered =
        chronos::wal::recover_wal_from_checkpoint({.directory_path = wal_path.string()}, {},
                                                  {.wal_id = wal_id,
                                                   .record_sequence = checkpoint.record_sequence,
                                                   .segment_number = checkpoint.segment_number,
                                                   .byte_offset = checkpoint.byte_offset},
                                                  sink);
    const auto finished = std::chrono::steady_clock::now();
    if (!recovered.has_value()) {
      return recovered.error();
    }
    Status verified = sink.finish();
    if (!verified.is_ok()) {
      return verified;
    }
    elapsed_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());
    if (retain_counts) {
      result.replayed_records = sink.replayed();
      result.replayed_rows = sink.replayed_rows();
    }
    return Status::ok();
  };
  status = replay_once(result.wal_replay_ns, true);
  if (status.is_ok()) {
    status = replay_once(result.repeated_wal_replay_ns, false);
  }
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }
  const Result<std::uint64_t> manifest_bytes = directory_file_bytes(manifest_path);
  const Result<std::uint64_t> part_bytes = directory_file_bytes(parts_path);
  if (!manifest_bytes.has_value() || !part_bytes.has_value()) {
    return chronos::common::make_unexpected(!manifest_bytes.has_value() ? manifest_bytes.error()
                                                                        : part_bytes.error());
  }
  result.manifest_directory_bytes = *manifest_bytes;
  result.part_directory_bytes = *part_bytes;
  result.logical_user_bytes =
      total_flushes * static_cast<std::uint64_t>(options.rows_per_head) * sizeof(std::int64_t);
  if (result.manifest_generation != total_flushes + 1U || result.part_count != total_flushes ||
      result.retry_count != total_flushes || result.replayed_records != expected_records ||
      result.replayed_rows != expected_rows) {
    return chronos::common::make_unexpected(
        corruption("recovered benchmark state disagrees with the exact generated workload"));
  }
  return result;
}

[[nodiscard]] std::uint64_t percentile_nearest_rank(std::vector<std::uint64_t> values,
                                                    const long double percentile) {
  std::ranges::sort(values);
  const long double rank = std::ceil(percentile * static_cast<long double>(values.size()));
  const std::size_t index = static_cast<std::size_t>(std::max<long double>(1.0L, rank)) - 1U;
  return values[index];
}

[[nodiscard]] std::string phase_name(const ForegroundPhase phase) {
  return phase == ForegroundPhase::kBaseline ? "baseline" : "flush";
}

[[nodiscard]] Status write_raw_samples(const Options& options,
                                       const std::vector<RepetitionResult>& results) {
  std::ostringstream flushes;
  flushes << "repetition,ordinal,latency_ns,rows,encoded_part_bytes,selected_manifest_bytes,"
             "durable_final_bytes,peak_candidate_bytes,file_syncs,directory_syncs\n";
  std::ostringstream foreground;
  foreground << "repetition,phase,reader,ordinal,latency_ns\n";
  for (const RepetitionResult& result : results) {
    for (const FlushSample& sample : result.flush_samples) {
      flushes << result.index << ',' << sample.ordinal << ',' << sample.latency_ns << ','
              << sample.rows << ',' << sample.encoded_part_bytes << ','
              << sample.selected_manifest_bytes << ',' << sample.durable_final_bytes << ','
              << sample.peak_candidate_bytes << ',' << sample.file_syncs << ','
              << sample.directory_syncs << '\n';
    }
    for (const ForegroundSample& sample : result.foreground_samples) {
      foreground << result.index << ',' << phase_name(sample.phase) << ',' << sample.reader << ','
                 << sample.ordinal << ',' << sample.latency_ns << '\n';
    }
  }
  Status status = write_text(options.output_directory / "raw-flushes.csv", flushes.str());
  return status.is_ok() ? write_text(options.output_directory / "raw-foreground-snapshots.csv",
                                     foreground.str())
                        : status;
}

void write_latency_object(std::ostringstream& output, const std::vector<std::uint64_t>& values) {
  output << "{\"count\":" << values.size() << ",\"p50\":" << percentile_nearest_rank(values, 0.50L)
         << ",\"p95\":" << percentile_nearest_rank(values, 0.95L)
         << ",\"p99\":" << percentile_nearest_rank(values, 0.99L) << ",\"p99_9\":";
  if (values.size() >= 1'000U) {
    output << percentile_nearest_rank(values, 0.999L);
  } else {
    output << "null";
  }
  output << '}';
}

[[nodiscard]] Status write_summary(const Options& options,
                                   const std::vector<RepetitionResult>& results) {
  std::ostringstream output;
  output << "{\n  \"schema_version\":1,\n  \"percentile_method\":"
         << quoted("nearest-rank over closed-loop operation latency") << ",\n  \"repetitions\":[\n";
  for (std::size_t index = 0U; index < results.size(); ++index) {
    const RepetitionResult& result = results[index];
    std::vector<std::uint64_t> flush_latencies;
    std::vector<std::uint64_t> baseline_latencies;
    std::vector<std::uint64_t> foreground_latencies;
    flush_latencies.reserve(result.flush_samples.size());
    for (const FlushSample& sample : result.flush_samples) {
      flush_latencies.push_back(sample.latency_ns);
    }
    for (const ForegroundSample& sample : result.foreground_samples) {
      (sample.phase == ForegroundPhase::kBaseline ? baseline_latencies : foreground_latencies)
          .push_back(sample.latency_ns);
    }
    const long double measured_seconds =
        static_cast<long double>(result.measured_elapsed_ns) / 1'000'000'000.0L;
    const long double rows_per_second =
        static_cast<long double>(options.flushes) * options.rows_per_head / measured_seconds;
    const std::uint64_t durable_bytes =
        result.manifest_directory_bytes + result.part_directory_bytes;
    output << "    {\"index\":" << result.index
           << ",\"measured_elapsed_ns\":" << result.measured_elapsed_ns
           << ",\"flush_rows_per_second\":" << std::fixed << std::setprecision(6) << rows_per_second
           << ",\"flush_latency_ns\":";
    write_latency_object(output, flush_latencies);
    output << ",\"foreground_baseline_latency_ns\":";
    write_latency_object(output, baseline_latencies);
    output << ",\"foreground_during_flush_latency_ns\":";
    write_latency_object(output, foreground_latencies);
    output << ",\"baseline_snapshot_count\":" << result.baseline_snapshot_count
           << ",\"flush_snapshot_count\":" << result.flush_snapshot_count
           << ",\"foreground_samples_dropped\":" << result.foreground_samples_dropped
           << ",\"manifest_startup_ns\":" << result.manifest_startup_ns
           << ",\"wal_replay_ns\":" << result.wal_replay_ns
           << ",\"repeated_manifest_startup_ns\":" << result.repeated_manifest_startup_ns
           << ",\"repeated_wal_replay_ns\":" << result.repeated_wal_replay_ns
           << ",\"manifest_generation\":" << result.manifest_generation
           << ",\"part_count\":" << result.part_count << ",\"retry_count\":" << result.retry_count
           << ",\"selected_manifest_bytes\":" << result.selected_manifest_bytes
           << ",\"manifest_directory_bytes\":" << result.manifest_directory_bytes
           << ",\"part_directory_bytes\":" << result.part_directory_bytes
           << ",\"durable_final_bytes\":" << durable_bytes
           << ",\"logical_user_bytes\":" << result.logical_user_bytes
           << ",\"durable_space_amplification\":"
           << static_cast<long double>(durable_bytes) /
                  static_cast<long double>(result.logical_user_bytes)
           << ",\"encoded_part_bytes\":" << result.encoded_part_bytes
           << ",\"peak_candidate_bytes\":" << result.peak_candidate_bytes
           << ",\"temporary_space_amplification\":"
           << static_cast<long double>(result.peak_candidate_bytes) /
                  static_cast<long double>(static_cast<std::uint64_t>(options.rows_per_head) *
                                           sizeof(std::int64_t))
           << ",\"file_syncs\":" << result.file_syncs
           << ",\"directory_syncs\":" << result.directory_syncs << ",\"syncs_per_flush\":"
           << static_cast<long double>(result.file_syncs + result.directory_syncs) /
                  static_cast<long double>(options.flushes)
           << ",\"replayed_records\":" << result.replayed_records
           << ",\"replayed_rows\":" << result.replayed_rows << ",\"user_cpu_us\":"
           << result.resources_after.user_cpu_us - result.resources_before.user_cpu_us
           << ",\"system_cpu_us\":"
           << result.resources_after.system_cpu_us - result.resources_before.system_cpu_us
           << ",\"peak_rss_bytes\":" << result.resources_after.maximum_rss_bytes
           << ",\"process_disk_read_bytes\":";
    if (result.resources_before.disk_read_bytes && result.resources_after.disk_read_bytes) {
      output << *result.resources_after.disk_read_bytes - *result.resources_before.disk_read_bytes;
    } else {
      output << "null";
    }
    output << ",\"process_disk_written_bytes\":";
    if (result.resources_before.disk_written_bytes && result.resources_after.disk_written_bytes) {
      output << *result.resources_after.disk_written_bytes -
                    *result.resources_before.disk_written_bytes;
    } else {
      output << "null";
    }
    output << '}' << (index + 1U == results.size() ? "\n" : ",\n");
  }
  output << "  ]\n}\n";
  return write_text(options.output_directory / "summary.json", output.str());
}

[[nodiscard]] std::string utc_timestamp() {
  const std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::tm utc{};
  if (::gmtime_r(&now, &utc) == nullptr) {
    return "unknown";
  }
  std::ostringstream output;
  output << std::put_time(&utc, "%Y-%m-%dT%H:%M:%SZ");
  return output.str();
}

[[nodiscard]] std::string invocation_json(const std::vector<std::string>& invocation) {
  std::ostringstream output;
  output << '[';
  for (std::size_t index = 0U; index < invocation.size(); ++index) {
    if (index != 0U) {
      output << ',';
    }
    output << quoted(invocation[index]);
  }
  output << ']';
  return output.str();
}

[[nodiscard]] std::string standard_library() {
#if defined(_LIBCPP_VERSION)
  return "libc++ " + std::to_string(_LIBCPP_VERSION);
#elif defined(__GLIBCXX__)
  return "libstdc++ " + std::to_string(__GLIBCXX__);
#else
  return "unknown";
#endif
}

[[nodiscard]] std::string cpu_model() {
#if defined(__APPLE__)
  std::size_t length = 0U;
  if (::sysctlbyname("machdep.cpu.brand_string", nullptr, &length, nullptr, 0) == 0 &&
      length > 1U) {
    std::string value(length, '\0');
    if (::sysctlbyname("machdep.cpu.brand_string", value.data(), &length, nullptr, 0) == 0) {
      value.resize(length - 1U);
      return value;
    }
  }
#elif defined(__linux__)
  std::ifstream input{"/proc/cpuinfo"};
  std::string line;
  while (std::getline(input, line)) {
    if (const std::size_t separator = line.find(':');
        separator != std::string::npos &&
        line.substr(0U, separator).find("model name") != std::string::npos) {
      return line.substr(separator + 2U);
    }
  }
#endif
  return "unknown";
}

[[nodiscard]] Status write_manifest(const Options& options, const bool validated) {
  const chronos::common::VersionInfo version = chronos::common::version_info();
  utsname system{};
  const bool have_uname = ::uname(&system) == 0;
  const long logical_cpus = ::sysconf(_SC_NPROCESSORS_ONLN);
  const long pages = ::sysconf(_SC_PHYS_PAGES);
  const long page_size = ::sysconf(_SC_PAGESIZE);
  const std::uint64_t memory_bytes =
      pages > 0 && page_size > 0
          ? static_cast<std::uint64_t>(pages) * static_cast<std::uint64_t>(page_size)
          : 0U;
  const char* const lang = std::getenv("LANG");
  const char* const lc_all = std::getenv("LC_ALL");
  const char* const timezone = std::getenv("TZ");
  std::ostringstream output;
  output << "{\n  \"schema_version\":1,\n  \"scenario\":" << quoted(kScenarioVersion)
         << ",\n  \"created_utc\":" << quoted(utc_timestamp())
         << ",\n  \"invocation_argv\":" << invocation_json(options.invocation)
         << ",\n  \"source\":{\"git_commit\":" << quoted(version.git_commit)
         << ",\"git_metadata_available\":" << (version.git_metadata_available ? "true" : "false")
         << ",\"git_dirty\":" << (version.git_dirty ? "true" : "false")
         << "},\n  \"workload\":{\"generator\":"
         << quoted("chronos-flushbench deterministic descending timestamp batches v1")
         << ",\"seed\":" << options.seed << ",\"repetitions\":" << options.repetitions
         << ",\"warmup_flushes\":" << options.warmup_flushes
         << ",\"measured_flushes\":" << options.flushes
         << ",\"rows_per_head\":" << options.rows_per_head
         << ",\"logical_columns\":1,\"logical_type\":\"TIMESTAMP_NS\","
            "\"batch_size_distribution\":\"fixed rows per head; one WAL command per head\","
            "\"event_time_distribution\":\"unique contiguous values, descending within each "
            "batch\",\"out_of_order_distribution\":\"none across batches; each sealed part is "
            "sorted during conversion\",\"duplicates_corrections_tombstones\":\"zero\","
            "\"compression\":"
         << quoted(compression_name(options.compression))
         << ",\"snapshot_readers\":" << options.snapshot_readers
         << ",\"baseline_snapshots_per_reader\":" << options.baseline_snapshots << "},\n"
         << "  \"database\":{\"replication\":\"single-node\",\"durability\":\"part and "
            "Manifest file sync plus directory sync; WAL fixture synchronized before flush\","
            "\"query_workload\":\"closed-loop aggregate snapshot acquisition only\","
            "\"flush_workers\":1,\"compaction\":\"disabled and not implemented\","
            "\"checkpoint\":\"empty-prefix coordinate; recovery replays complete retained WAL\","
            "\"cache_state\":\"uncontrolled host cache; warmup count declared\"},\n"
         << R"(  "build":{"semantic_version":)" << quoted(version.semantic_version)
         << ",\"build_type\":" << quoted(version.build_type)
         << ",\"compiler\":" << quoted(version.compiler)
         << ",\"standard_library\":" << quoted(standard_library())
         << ",\"target_architecture\":" << quoted(version.target_architecture)
         << ",\"operating_system\":" << quoted(version.operating_system) << "},\n"
         << R"(  "host":{"uname":)"
         << quoted(have_uname
                       ? std::string{system.sysname} + " " + system.release + " " + system.machine
                       : "unknown (uname failed)")
         << ",\"cpu_model\":" << quoted(cpu_model())
         << ",\"logical_cpus\":" << (logical_cpus > 0 ? logical_cpus : 0)
         << ",\"memory_bytes\":" << memory_bytes
         << ",\"storage_and_power_loss_properties\":\"unknown unless wrapper inventory provides "
            "operator evidence\",\"numa_frequency_power_container\":\"unknown unless wrapper "
            "inventory provides evidence\",\"network\":\"not used\"},\n"
         << "  \"environment\":{\"capture_policy\":\"security allowlist plus wrapper system "
            "inventory\",\"LANG\":"
         << quoted(lang == nullptr ? "unset" : lang)
         << ",\"LC_ALL\":" << quoted(lc_all == nullptr ? "unset" : lc_all)
         << ",\"TZ\":" << quoted(timezone == nullptr ? "unset" : timezone)
         << ",\"privilege_and_tuning_steps\":\"none performed by chronos-flushbench\"},\n"
         << "  \"procedure\":{\"arrival_model\":\"single closed-loop storage owner with "
            "concurrent closed-loop snapshot readers\",\"run_order\":\"sequential fresh database "
            "per repetition\",\"cooldown\":\"none\",\"outlier_policy\":\"none; every retained "
            "sample is reported\",\"percentiles\":\"nearest-rank; p99.9 null below 1000 "
            "samples; bounded per-reader deterministic reservoir sampling preserves observations "
            "across the complete phase\",\"maximum_foreground_samples_per_repetition\":"
         << options.maximum_foreground_samples
         << ",\"maximum_artifact_bytes_total\":" << options.maximum_artifact_bytes << "},\n"
         << R"(  "correctness":{"validation_status":)" << quoted(validated ? "passed" : "pending")
         << ",\"exact_generation_part_retry_counts_required\":true,"
            "\"zero_recognized_temporaries_after_success_required\":true,"
            "\"byte_identical_repeated_manifest_recovery_required\":true,"
            "\"complete_repeated_wal_suffix_replay_required\":true,"
            "\"acknowledged_write_loss_count\":"
         << (validated ? "0" : "null") << "},\n"
         << "  \"limitations\":[\"Results are local measurements, not published performance "
            "claims.\",\"Foreground work is snapshot acquisition, not SQL or networking.\","
            "\"Temporary peak bytes are the exact largest successful single candidate image under "
            "the one-at-a-time protocol, not sampled device allocation.\",\"Manifest startup "
            "validates all referenced parts; WAL replay validates and decodes the retained suffix "
            "without reconstructing a server catalog.\",\"Process tests do not qualify power-loss "
            "behavior, firmware, controller caches, filesystems, hypervisors, or network "
            "filesystems.\"]\n}\n";
  return write_text(options.output_directory / "manifest.json", output.str());
}

[[nodiscard]] Status create_output_directory(Options& options) {
  std::error_code error;
  options.output_directory = std::filesystem::absolute(options.output_directory, error);
  if (error) {
    return io_error("cannot resolve benchmark output path");
  }
  if (std::filesystem::exists(options.output_directory, error) || error) {
    return invalid("benchmark output directory already exists or cannot be inspected");
  }
  const std::filesystem::path parent = options.output_directory.parent_path();
  if (!std::filesystem::is_directory(parent, error) || error) {
    return invalid("benchmark output parent must already exist and be a directory");
  }
  if (!std::filesystem::create_directory(options.output_directory, error) || error) {
    return io_error("cannot create benchmark output directory");
  }
  return Status::ok();
}

int run_main(const int argc, char** const argv) {
  Result<Options> parsed = parse_options(argc, argv);
  if (!parsed.has_value()) {
    if (parsed.error().code() == StatusCode::kCancelled) {
      print_usage(argc > 0 ? std::string_view{argv[0]} : "chronos-flushbench");
      return 0;
    }
    std::cerr << parsed.error().to_string() << '\n';
    print_usage(argc > 0 ? std::string_view{argv[0]} : "chronos-flushbench");
    return 2;
  }
  Options options = std::move(*parsed);
  Status status = validate_options(options);
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 2;
  }
  status = create_output_directory(options);
  if (status.is_ok()) {
    status = write_manifest(options, false);
  }
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 1;
  }
  std::vector<RepetitionResult> results;
  results.reserve(static_cast<std::size_t>(options.repetitions));
  for (std::uint64_t repetition = 1U; repetition <= options.repetitions; ++repetition) {
    Result<RepetitionResult> result = run_repetition(options, repetition);
    if (!result.has_value()) {
      static_cast<void>(
          write_text(options.output_directory / "failure.txt", result.error().to_string() + "\n"));
      std::cerr << result.error().to_string() << '\n';
      return 1;
    }
    results.push_back(std::move(*result));
  }
  status = write_raw_samples(options, results);
  if (status.is_ok()) {
    status = write_summary(options, results);
  }
  if (status.is_ok()) {
    status = write_manifest(options, true);
  }
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 1;
  }
  std::cout << "Flush benchmark completed with repeated Manifest/WAL recovery; artifacts: "
            << options.output_directory << '\n';
  return 0;
}

} // namespace

int main(const int argc, char** const argv) {
  try {
    return run_main(argc, argv);
  } catch (const std::bad_alloc&) {
    std::cerr << "RESOURCE_EXHAUSTED: chronos-flushbench could not allocate bounded run state\n";
    return 1;
  } catch (const std::exception& error) {
    std::cerr << "INTERNAL: chronos-flushbench encountered an unexpected exception: "
              << error.what() << '\n';
    return 1;
  } catch (...) {
    std::cerr << "INTERNAL: chronos-flushbench encountered an unknown exception\n";
    return 1;
  }
}
