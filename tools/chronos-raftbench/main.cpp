#include "chronos/common/version.hpp"
#include "chronos/raft/multiplexed_log.hpp"
#include "chronos/raft/persistent_log.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
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
#include <new>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/resource.h>
#include <sys/utsname.h>
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
using chronos::raft::GroupId;
using chronos::raft::GroupPersistentState;
using chronos::raft::RaftPersistentLog;
using chronos::raft::RaftPersistentLogConfig;

constexpr std::string_view kScenarioVersion = "raft-persistent-log-batches/1";
constexpr std::uint64_t kDefaultMaximumArtifactBytes = std::uint64_t{1U} << 30U;

enum class Mode : std::uint8_t { kAppendOnly = 1U, kLocalSync = 2U };

struct Options {
  std::filesystem::path output_directory;
  Mode mode{Mode::kLocalSync};
  std::uint64_t operations{10'000U};
  std::uint64_t warmup_operations{1'000U};
  std::uint64_t repetitions{3U};
  std::uint64_t seed{0x4348524f4e4f5352ULL};
  std::size_t batch_records{16U};
  std::size_t payload_bytes{256U};
  std::size_t logical_groups{8U};
  std::uint64_t target_segment_bytes{chronos::raft::kDefaultRaftSegmentTargetSize};
  std::uint64_t maximum_artifact_bytes{kDefaultMaximumArtifactBytes};
  bool allow_dirty{false};
  bool allow_non_release{false};
  std::vector<std::string> invocation;
};

struct Sample {
  std::uint64_t ordinal{};
  std::uint64_t first_physical_sequence{};
  std::uint64_t last_physical_sequence{};
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
  std::uint64_t elapsed_ns{};
  std::uint64_t recovery_ns{};
  std::uint64_t physical_log_bytes{};
  std::uint64_t segment_count{};
  std::uint64_t measured_explicit_synchronize_calls{};
  ResourceSnapshot resources_before;
  ResourceSnapshot resources_after;
  std::vector<Sample> samples;
};

[[nodiscard]] Status invalid(std::string message) {
  return Status{StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] std::string mode_name(const Mode mode) {
  return mode == Mode::kAppendOnly ? "APPEND_ONLY" : "LOCAL_SYNC";
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
  if (text.empty())
    return false;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end == text.data() + text.size();
}

void print_usage(const std::string_view program) {
  std::cerr << "Usage: " << program << " --output-dir PATH [options]\n"
            << "  --mode APPEND_ONLY|LOCAL_SYNC\n"
            << "  --operations N --warmup-operations N --repetitions N\n"
            << "  --batch-records N --payload-bytes N --logical-groups N --seed N\n"
            << "  --target-segment-bytes N --maximum-artifact-bytes N\n"
            << "  --allow-dirty --allow-non-release\n";
}

[[nodiscard]] Result<Options> parse_options(const int argc, char** const argv) {
  Options options;
  options.invocation.reserve(static_cast<std::size_t>(argc));
  for (int index = 0; index < argc; ++index)
    options.invocation.emplace_back(argv[index]);

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
    if (index + 1 >= argc)
      return chronos::common::make_unexpected(invalid("missing value for " + std::string{key}));
    const std::string_view value{argv[++index]};
    if (key == "--output-dir") {
      options.output_directory = value;
    } else if (key == "--mode") {
      if (value == "APPEND_ONLY")
        options.mode = Mode::kAppendOnly;
      else if (value == "LOCAL_SYNC")
        options.mode = Mode::kLocalSync;
      else
        return chronos::common::make_unexpected(
            invalid("--mode must be APPEND_ONLY or LOCAL_SYNC"));
    } else if (key == "--operations") {
      if (!parse_integer(value, options.operations))
        return chronos::common::make_unexpected(invalid("invalid --operations value"));
    } else if (key == "--warmup-operations") {
      if (!parse_integer(value, options.warmup_operations))
        return chronos::common::make_unexpected(invalid("invalid --warmup-operations value"));
    } else if (key == "--repetitions") {
      if (!parse_integer(value, options.repetitions))
        return chronos::common::make_unexpected(invalid("invalid --repetitions value"));
    } else if (key == "--batch-records") {
      if (!parse_integer(value, options.batch_records))
        return chronos::common::make_unexpected(invalid("invalid --batch-records value"));
    } else if (key == "--payload-bytes") {
      if (!parse_integer(value, options.payload_bytes))
        return chronos::common::make_unexpected(invalid("invalid --payload-bytes value"));
    } else if (key == "--logical-groups") {
      if (!parse_integer(value, options.logical_groups))
        return chronos::common::make_unexpected(invalid("invalid --logical-groups value"));
    } else if (key == "--seed") {
      if (!parse_integer(value, options.seed))
        return chronos::common::make_unexpected(invalid("invalid --seed value"));
    } else if (key == "--target-segment-bytes") {
      if (!parse_integer(value, options.target_segment_bytes))
        return chronos::common::make_unexpected(invalid("invalid --target-segment-bytes value"));
    } else if (key == "--maximum-artifact-bytes") {
      if (!parse_integer(value, options.maximum_artifact_bytes))
        return chronos::common::make_unexpected(invalid("invalid --maximum-artifact-bytes value"));
    } else {
      return chronos::common::make_unexpected(invalid("unknown option " + std::string{key}));
    }
  }
  return options;
}

[[nodiscard]] GroupId group_id(const std::size_t ordinal) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(ordinal + 1U);
  return GroupId{bytes};
}

[[nodiscard]] std::vector<std::byte> payload(const Options& options, const std::uint64_t sequence) {
  std::vector<std::byte> bytes(options.payload_bytes);
  std::uint64_t random = options.seed ^ (sequence * 0x9e3779b97f4a7c15ULL);
  for (std::byte& byte : bytes) {
    random ^= random >> 12U;
    random ^= random << 25U;
    random ^= random >> 27U;
    byte = static_cast<std::byte>((random * 0x2545f4914f6cdd1dULL) & 0xffU);
  }
  for (std::size_t index = 0U; index < std::min(bytes.size(), sizeof(sequence)); ++index)
    bytes[index] = static_cast<std::byte>((sequence >> (index * 8U)) & 0xffU);
  return bytes;
}

[[nodiscard]] GroupPersistentState record(const Options& options, const std::uint64_t sequence) {
  chronos::raft::PersistentState state;
  state.current_term = 1U;
  state.log.push_back(chronos::raft::LogEntry{
      .index = 1U, .term = 1U, .type = 1U, .payload = payload(options, sequence)});
  state.commit_index = 1U;
  return GroupPersistentState{
      .group_id = group_id(static_cast<std::size_t>((sequence - 1U) % options.logical_groups)),
      .physical_sequence = sequence,
      .state = std::move(state)};
}

[[nodiscard]] Result<std::size_t> encoded_record_size(const Options& options) {
  auto encoded = chronos::raft::encode_multiplexed_log_record_v1(record(options, 1U));
  if (!encoded.has_value())
    return chronos::common::make_unexpected(encoded.error());
  return encoded->size();
}

[[nodiscard]] Status validate_options(const Options& options) {
  if (options.output_directory.empty())
    return invalid("--output-dir is required");
  if (options.operations == 0U || options.repetitions == 0U || options.batch_records == 0U ||
      options.logical_groups == 0U || options.logical_groups > 255U)
    return invalid(
        "operations, repetitions, batch records, and 1..255 logical groups are required");
  if (options.payload_bytes > chronos::raft::kMaximumMultiplexedLogRecordSize)
    return invalid("payload size exceeds the bounded Raft record allocation limit");
  if (options.operations % options.batch_records != 0U ||
      options.warmup_operations % options.batch_records != 0U)
    return invalid("warmup and measured operations must be divisible by --batch-records");
  if (options.operations > std::numeric_limits<std::uint64_t>::max() - options.warmup_operations)
    return invalid("operation count overflows uint64");
  const std::uint64_t total_records = options.operations + options.warmup_operations;
  if (total_records == std::numeric_limits<std::uint64_t>::max())
    return invalid("operation count leaves no terminating physical sequence");
  if (options.logical_groups > total_records)
    return invalid("logical group count exceeds total generated records");
  if (total_records > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()))
    return invalid("operation count exceeds this platform's addressable state");
  if (options.target_segment_bytes > chronos::raft::kMaximumRaftSegmentSize)
    return invalid("target segment size exceeds the Raft physical limit");
  const Result<std::size_t> record_bytes = encoded_record_size(options);
  if (!record_bytes.has_value())
    return record_bytes.error();
  if (options.target_segment_bytes < chronos::raft::kRaftSegmentHeaderSize + *record_bytes)
    return invalid("target segment cannot hold one configured full-state record");

  constexpr std::uint64_t kFixedArtifactAllowance = 128ULL * 1024ULL;
  constexpr std::uint64_t kTextBytesPerBatch = 512U;
  const std::uint64_t batches = options.operations / options.batch_records;
  if (options.maximum_artifact_bytes <= kFixedArtifactAllowance)
    return invalid("--maximum-artifact-bytes is too small for fixed run metadata");
  const std::uint64_t variable_budget = options.maximum_artifact_bytes - kFixedArtifactAllowance;
  const std::uint64_t bytes_per_record =
      static_cast<std::uint64_t>(*record_bytes) + chronos::raft::kRaftSegmentHeaderSize;
  if (total_records != 0U && bytes_per_record > variable_budget / total_records)
    return invalid("benchmark run exceeds --maximum-artifact-bytes; raise it explicitly");
  const std::uint64_t bytes_per_repetition = total_records * bytes_per_record;
  if (batches != 0U && kTextBytesPerBatch > variable_budget / batches)
    return invalid("benchmark sample artifact exceeds --maximum-artifact-bytes");
  const std::uint64_t repetition_budget = bytes_per_repetition + batches * kTextBytesPerBatch;
  if (repetition_budget != 0U && options.repetitions > variable_budget / repetition_budget)
    return invalid("benchmark repetitions exceed --maximum-artifact-bytes");

  const chronos::common::VersionInfo version = chronos::common::version_info();
  if (version.git_dirty && !options.allow_dirty)
    return invalid("working tree was dirty at build time; use --allow-dirty and retain a diff");
  if (version.build_type != "Release" && !options.allow_non_release)
    return invalid("benchmark build is not Release; use --allow-non-release only for smoke tests");
  return Status::ok();
}

[[nodiscard]] std::vector<GroupPersistentState> make_records(const Options& options) {
  const std::uint64_t count = options.warmup_operations + options.operations;
  std::vector<GroupPersistentState> records;
  records.reserve(static_cast<std::size_t>(count));
  for (std::uint64_t sequence = 1U; sequence <= count; ++sequence)
    records.push_back(record(options, sequence));
  return records;
}

[[nodiscard]] std::optional<std::uint64_t> read_proc_io_value(const std::string_view key) {
#if defined(__linux__)
  std::ifstream input{"/proc/self/io"};
  std::string name;
  std::uint64_t value = 0U;
  while (input >> name >> value) {
    if (name == key)
      return value;
  }
#else
  static_cast<void>(key);
#endif
  return std::nullopt;
}

[[nodiscard]] std::uint64_t timeval_microseconds(const timeval& value) {
  return static_cast<std::uint64_t>(value.tv_sec) * 1'000'000U +
         static_cast<std::uint64_t>(value.tv_usec);
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

[[nodiscard]] Status run_phase(RaftPersistentLog& log, const Options& options,
                               const std::span<const GroupPersistentState> records,
                               std::vector<Sample>* const samples, std::uint64_t& elapsed_ns) {
  elapsed_ns = 0U;
  if (samples != nullptr)
    samples->reserve(records.size() / options.batch_records);
  const auto phase_started = std::chrono::steady_clock::now();
  for (std::size_t offset = 0U; offset < records.size(); offset += options.batch_records) {
    const auto started = std::chrono::steady_clock::now();
    for (std::size_t index = 0U; index < options.batch_records; ++index) {
      auto appended = log.append(records[offset + index]);
      if (!appended.has_value())
        return appended.error();
      if (appended->physical_sequence != records[offset + index].physical_sequence)
        return Status{StatusCode::kInternal, "append returned the wrong physical sequence"};
    }
    if (options.mode == Mode::kLocalSync) {
      auto synchronized = log.synchronize();
      if (!synchronized.has_value())
        return synchronized.error();
      if (synchronized->physical_sequence !=
          records[offset + options.batch_records - 1U].physical_sequence)
        return Status{StatusCode::kInternal, "LOCAL_SYNC returned the wrong durable frontier"};
    }
    const auto finished = std::chrono::steady_clock::now();
    const auto latency = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());
    if (samples != nullptr) {
      samples->push_back(
          {.ordinal = samples->size(),
           .first_physical_sequence = records[offset].physical_sequence,
           .last_physical_sequence = records[offset + options.batch_records - 1U].physical_sequence,
           .latency_ns = latency});
    }
  }
  elapsed_ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - phase_started)
                                              .count());
  return Status::ok();
}

[[nodiscard]] Status validate_recovery(const chronos::raft::RaftPersistentLogRecovery& recovery,
                                       const std::span<const GroupPersistentState> records,
                                       const Options& options) {
  if (recovery.record_count != records.size() ||
      recovery.written_position.physical_sequence != records.size() ||
      recovery.durable_physical_sequence != records.size() || recovery.repaired_bytes != 0U ||
      recovery.latest_group_states.size() != options.logical_groups) {
    return Status{StatusCode::kCorruption,
                  "recovered Raft persistent-log frontiers do not match generated history"};
  }
  std::vector<const GroupPersistentState*> latest(options.logical_groups, nullptr);
  for (const GroupPersistentState& persistent : records) {
    const auto ordinal =
        static_cast<std::size_t>((persistent.physical_sequence - 1U) % options.logical_groups);
    latest[ordinal] = &persistent;
  }
  for (const GroupPersistentState* const expected : latest) {
    if (expected == nullptr)
      return Status{StatusCode::kInternal, "benchmark omitted a configured logical group"};
    const auto found = std::ranges::find_if(recovery.latest_group_states, [&](const auto& actual) {
      return actual.group_id == expected->group_id;
    });
    if (found == recovery.latest_group_states.end() || *found != *expected)
      return Status{StatusCode::kCorruption,
                    "recovery did not retain the exact latest state for every group"};
  }
  return Status::ok();
}

[[nodiscard]] Result<std::uint64_t> physical_log_bytes(const std::filesystem::path& directory) {
  std::uint64_t bytes = 0U;
  std::error_code error;
  for (const auto& entry : std::filesystem::directory_iterator(directory, error)) {
    if (error)
      return chronos::common::make_unexpected(
          Status{StatusCode::kIoError, "cannot enumerate Raft benchmark log"});
    if (entry.path().extension() == ".rlog") {
      const std::uint64_t size = entry.file_size(error);
      if (error || size > std::numeric_limits<std::uint64_t>::max() - bytes)
        return chronos::common::make_unexpected(
            Status{StatusCode::kIoError, "cannot account Raft benchmark log bytes"});
      bytes += size;
    }
  }
  return bytes;
}

[[nodiscard]] Result<RepetitionResult> run_repetition(const Options& options,
                                                      const std::uint64_t repetition) {
  const std::filesystem::path repetition_directory =
      options.output_directory / ("repetition-" + std::to_string(repetition));
  const std::filesystem::path log_directory = repetition_directory / "raft-log";
  std::error_code error;
  if (!std::filesystem::create_directory(repetition_directory, error) || error ||
      !std::filesystem::create_directory(log_directory, error) || error) {
    return chronos::common::make_unexpected(
        Status{StatusCode::kIoError, "cannot create fresh Raft benchmark directories"});
  }
  const std::vector<GroupPersistentState> records = make_records(options);
  const std::uint64_t total_records = options.warmup_operations + options.operations;
  const RaftPersistentLogConfig config{
      .directory_path = log_directory.string(),
      .target_segment_size = options.target_segment_bytes,
      .maximum_segments = static_cast<std::size_t>(total_records),
      .maximum_records = static_cast<std::size_t>(total_records),
      .maximum_groups = options.logical_groups,
  };
  auto created = RaftPersistentLog::create_new(config);
  if (!created.has_value())
    return chronos::common::make_unexpected(created.error());
  RaftPersistentLog log = std::move(*created);

  std::uint64_t ignored_elapsed = 0U;
  Status status = run_phase(
      log, options, std::span{records}.first(static_cast<std::size_t>(options.warmup_operations)),
      nullptr, ignored_elapsed);
  if (!status.is_ok())
    return chronos::common::make_unexpected(status);
  auto warmup_sync = log.synchronize();
  if (!warmup_sync.has_value())
    return chronos::common::make_unexpected(warmup_sync.error());

  RepetitionResult result;
  result.index = repetition;
  result.resources_before = resource_snapshot();
  status = run_phase(
      log, options, std::span{records}.subspan(static_cast<std::size_t>(options.warmup_operations)),
      &result.samples, result.elapsed_ns);
  result.resources_after = resource_snapshot();
  if (!status.is_ok())
    return chronos::common::make_unexpected(status);
  result.measured_explicit_synchronize_calls =
      options.mode == Mode::kLocalSync ? options.operations / options.batch_records : 0U;

  auto cleanup_sync = log.synchronize();
  if (!cleanup_sync.has_value() || cleanup_sync->physical_sequence != total_records)
    return chronos::common::make_unexpected(
        cleanup_sync.has_value()
            ? Status{StatusCode::kInternal, "cleanup sync returned the wrong durable frontier"}
            : cleanup_sync.error());
  status = log.close();
  if (!status.is_ok())
    return chronos::common::make_unexpected(status);

  const auto recovery_started = std::chrono::steady_clock::now();
  auto reopened = RaftPersistentLog::open_existing(config);
  const auto recovery_finished = std::chrono::steady_clock::now();
  if (!reopened.has_value())
    return chronos::common::make_unexpected(reopened.error());
  status = validate_recovery(reopened->recovery(), records, options);
  if (!status.is_ok())
    return chronos::common::make_unexpected(status);
  result.segment_count = reopened->recovery().segment_count;
  status = reopened->close();
  if (!status.is_ok())
    return chronos::common::make_unexpected(status);
  result.recovery_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(recovery_finished - recovery_started)
          .count());
  auto bytes = physical_log_bytes(log_directory);
  if (!bytes.has_value())
    return chronos::common::make_unexpected(bytes.error());
  result.physical_log_bytes = *bytes;
  return result;
}

[[nodiscard]] Status write_text(const std::filesystem::path& path, const std::string_view text) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  output.write(text.data(), static_cast<std::streamsize>(text.size()));
  output.flush();
  if (!output.good())
    return Status{StatusCode::kIoError, "failed to write benchmark artifact " + path.string()};
  return Status::ok();
}

[[nodiscard]] std::uint64_t percentile_nearest_rank(std::vector<std::uint64_t> values,
                                                    const long double percentile) {
  std::sort(values.begin(), values.end());
  const long double rank = std::ceil(percentile * static_cast<long double>(values.size()));
  const std::size_t index = static_cast<std::size_t>(std::max<long double>(1.0L, rank)) - 1U;
  return values[index];
}

[[nodiscard]] Status write_raw_samples(const Options& options,
                                       const std::vector<RepetitionResult>& results) {
  std::ostringstream output;
  output << "repetition,ordinal,first_physical_sequence,last_physical_sequence,batch_records,"
            "payload_bytes,latency_ns,requested_mode,effective_mode\n";
  const std::string mode = mode_name(options.mode);
  for (const RepetitionResult& result : results) {
    for (const Sample& sample : result.samples) {
      output << result.index << ',' << sample.ordinal << ',' << sample.first_physical_sequence
             << ',' << sample.last_physical_sequence << ',' << options.batch_records << ','
             << options.payload_bytes << ',' << sample.latency_ns << ',' << mode << ',' << mode
             << '\n';
    }
  }
  return write_text(options.output_directory / "raw-latencies.csv", output.str());
}

[[nodiscard]] Status write_summary(const Options& options,
                                   const std::vector<RepetitionResult>& results) {
  std::ostringstream output;
  output << "{\n  \"schema_version\":1,\n  \"latency_population\":"
         << quoted("closed-loop batch latency; nearest-rank percentiles")
         << ",\n  \"repetitions\":[\n";
  for (std::size_t index = 0U; index < results.size(); ++index) {
    const RepetitionResult& result = results[index];
    if (result.elapsed_ns == 0U || result.samples.empty())
      return Status{StatusCode::kInternal, "benchmark measured an empty or zero-duration phase"};
    std::vector<std::uint64_t> latencies;
    latencies.reserve(result.samples.size());
    for (const Sample& sample : result.samples)
      latencies.push_back(sample.latency_ns);
    const long double seconds = static_cast<long double>(result.elapsed_ns) / 1'000'000'000.0L;
    const long double throughput = static_cast<long double>(options.operations) / seconds;
    output << "    {\"index\":" << result.index << ",\"elapsed_ns\":" << result.elapsed_ns
           << ",\"records_per_second\":" << std::fixed << std::setprecision(6) << throughput
           << R"(,"batch_latency_ns":{"p50":)" << percentile_nearest_rank(latencies, 0.50L)
           << ",\"p95\":" << percentile_nearest_rank(latencies, 0.95L)
           << ",\"p99\":" << percentile_nearest_rank(latencies, 0.99L) << ",\"p99_9\":";
    if (latencies.size() >= 1000U)
      output << percentile_nearest_rank(latencies, 0.999L);
    else
      output << "null";
    output << "},\"recovery_ns\":" << result.recovery_ns
           << ",\"physical_log_bytes\":" << result.physical_log_bytes
           << ",\"segments\":" << result.segment_count
           << ",\"measured_explicit_synchronize_calls\":"
           << result.measured_explicit_synchronize_calls << ",\"user_cpu_us\":"
           << result.resources_after.user_cpu_us - result.resources_before.user_cpu_us
           << ",\"system_cpu_us\":"
           << result.resources_after.system_cpu_us - result.resources_before.system_cpu_us
           << ",\"peak_rss_bytes\":" << result.resources_after.maximum_rss_bytes
           << ",\"process_disk_read_bytes\":";
    if (result.resources_before.disk_read_bytes.has_value() &&
        result.resources_after.disk_read_bytes.has_value())
      output << *result.resources_after.disk_read_bytes - *result.resources_before.disk_read_bytes;
    else
      output << "null";
    output << ",\"process_disk_written_bytes\":";
    if (result.resources_before.disk_written_bytes.has_value() &&
        result.resources_after.disk_written_bytes.has_value())
      output << *result.resources_after.disk_written_bytes -
                    *result.resources_before.disk_written_bytes;
    else
      output << "null";
    output << '}' << (index + 1U == results.size() ? "\n" : ",\n");
  }
  output << "  ]\n}\n";
  return write_text(options.output_directory / "summary.json", output.str());
}

[[nodiscard]] std::string standard_library() {
#if defined(_LIBCPP_VERSION)
  return "libc++ " + std::to_string(_LIBCPP_VERSION);
#elif defined(__GLIBCXX__)
  return "libstdc++ " + std::to_string(__GLIBCXX__);
#else
  return "unknown (standard library did not expose a recognized version macro)";
#endif
}

[[nodiscard]] std::string cpu_model() {
#if defined(__linux__)
  std::ifstream input{"/proc/cpuinfo"};
  std::string line;
  while (std::getline(input, line)) {
    constexpr std::string_view kName = "model name";
    if (line.starts_with(kName)) {
      const std::size_t colon = line.find(':');
      if (colon != std::string::npos)
        return line.substr(colon + 2U);
    }
  }
#elif defined(__APPLE__)
  std::size_t size = 0U;
  if (::sysctlbyname("machdep.cpu.brand_string", nullptr, &size, nullptr, 0U) == 0 && size > 1U) {
    std::string value(size, '\0');
    if (::sysctlbyname("machdep.cpu.brand_string", value.data(), &size, nullptr, 0U) == 0) {
      value.resize(size - 1U);
      return value;
    }
  }
#endif
  return "unknown (CPU model discovery unavailable)";
}

[[nodiscard]] std::string utc_timestamp() {
  const std::time_t now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::tm utc{};
  if (::gmtime_r(&now, &utc) == nullptr)
    return "unknown";
  std::ostringstream output;
  output << std::put_time(&utc, "%Y-%m-%dT%H:%M:%SZ");
  return output.str();
}

[[nodiscard]] std::string invocation_json(const std::vector<std::string>& invocation) {
  std::ostringstream output;
  output << '[';
  for (std::size_t index = 0U; index < invocation.size(); ++index) {
    if (index != 0U)
      output << ',';
    output << quoted(invocation[index]);
  }
  output << ']';
  return output.str();
}

[[nodiscard]] Status write_manifest(const Options& options, const std::size_t record_bytes,
                                    const bool recovery_validated) {
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
  std::error_code space_error;
  const auto space = std::filesystem::space(options.output_directory, space_error);
  const char* const lang = std::getenv("LANG");
  const char* const lc_all = std::getenv("LC_ALL");
  const char* const timezone = std::getenv("TZ");
  const std::string mode = mode_name(options.mode);
  std::ostringstream output;
  output
      << "{\n  \"schema_version\":1,\n  \"scenario\":" << quoted(kScenarioVersion)
      << ",\n  \"created_utc\":" << quoted(utc_timestamp())
      << ",\n  \"invocation_argv\":" << invocation_json(options.invocation)
      << ",\n  \"source\":{\"git_commit\":" << quoted(version.git_commit)
      << ",\"git_metadata_available\":" << (version.git_metadata_available ? "true" : "false")
      << ",\"git_dirty\":" << (version.git_dirty ? "true" : "false")
      << ",\"dirty_diff_artifact_required\":" << (version.git_dirty ? "true" : "false")
      << "},\n  \"workload\":{\"generator\":"
      << quoted("chronos-raftbench deterministic full-state generator v1")
      << ",\"seed\":" << options.seed << ",\"repetitions\":" << options.repetitions
      << ",\"warmup_records\":" << options.warmup_operations
      << ",\"measured_records\":" << options.operations
      << ",\"batch_records\":" << options.batch_records
      << ",\"logical_groups\":" << options.logical_groups
      << ",\"entry_payload_bytes\":" << options.payload_bytes
      << ",\"encoded_full_state_record_bytes\":" << record_bytes
      << ",\"batch_size_distribution\":" << quoted("fixed batch_records")
      << ",\"arrival_model\":" << quoted("single-owner closed loop")
      << ",\"schema_and_temporal_distribution\":"
      << quoted("not applicable to physical Raft full-state persistence")
      << "},\n  \"raft_log\":{\"requested_mode\":" << quoted(mode)
      << ",\"effective_mode\":" << quoted(mode)
      << ",\"target_segment_bytes\":" << options.target_segment_bytes << ",\"replication\":"
      << quoted("single-node physical mechanism; no quorum traffic exercised")
      << ",\"read_consistency\":" << quoted("not applicable") << ",\"rotation_policy\":"
      << quoted("production append rotation, including predecessor sync and successor install")
      << ",\"internal_rotation_sync_calls\":" << quoted("not separately instrumented")
      << ",\"flush_compaction_snapshot_transfer\":"
      << quoted("outside this persistent-log batch benchmark")
      << "},\n  \"build\":{\"semantic_version\":" << quoted(version.semantic_version)
      << ",\"build_type\":" << quoted(version.build_type)
      << ",\"compiler\":" << quoted(version.compiler)
      << ",\"standard_library\":" << quoted(standard_library())
      << ",\"target_architecture\":" << quoted(version.target_architecture)
      << ",\"operating_system\":" << quoted(version.operating_system)
      << ",\"compiler_linker_flags\":"
      << quoted("wrapper retains CMakeCache.txt and compile_commands.json; unknown for direct "
                "invocation")
      << "},\n  \"host\":{\"uname\":"
      << quoted(have_uname
                    ? std::string{system.sysname} + " " + system.release + " " + system.machine
                    : "unknown (uname failed)")
      << ",\"cpu_model\":" << quoted(cpu_model())
      << ",\"logical_cpus\":" << (logical_cpus > 0 ? logical_cpus : 0)
      << ",\"memory_bytes\":" << memory_bytes
      << ",\"filesystem_capacity_bytes\":" << (space_error ? 0U : space.capacity)
      << ",\"cpu_memory_storage_topology\":"
      << quoted("unknown unless retained in system-inventory.txt by wrapper")
      << ",\"network\":" << quoted("not used") << ",\"container_hypervisor_cloud\":"
      << quoted("unknown; wrapper inventory may provide evidence")
      << "},\n  \"environment\":{\"capture_policy\":"
      << quoted("security allowlist; wrapper retains command and inventory artifacts")
      << ",\"LANG\":" << quoted(lang == nullptr ? "unset" : lang)
      << ",\"LC_ALL\":" << quoted(lc_all == nullptr ? "unset" : lc_all)
      << ",\"TZ\":" << quoted(timezone == nullptr ? "unset" : timezone)
      << ",\"privilege_and_tuning_steps\":" << quoted("none performed by chronos-raftbench")
      << "},\n  \"procedure\":{\"warmup_policy\":"
      << quoted("declared record count on each fresh log, followed by an excluded cleanup sync")
      << ",\"run_order\":" << quoted("sequential fresh log per repetition; no randomization")
      << ",\"cooldown\":" << quoted("none")
      << ",\"outlier_policy\":" << quoted("none; every successful batch retained")
      << ",\"latency_population\":"
      << quoted("successful closed-loop measured batches; any error invalidates the run")
      << ",\"percentiles\":" << quoted("nearest-rank; p99.9 null below 1000 batch samples")
      << ",\"cache_state\":" << quoted("uncontrolled host cache; explicit warmup")
      << ",\"maximum_tool_artifact_bytes\":" << options.maximum_artifact_bytes
      << "},\n  \"correctness\":{\"validation_status\":"
      << quoted(recovery_validated ? "passed" : "pending")
      << ",\"exact_reopen_required\":true,\"exact_physical_sequence_required\":true,"
         "\"exact_latest_group_state_required\":true,\"repaired_bytes_required\":0,"
         "\"acknowledged_write_loss_count\":"
      << (recovery_validated && options.mode == Mode::kLocalSync ? "0" : "null")
      << ",\"acknowledged_write_loss_applicability\":"
      << quoted(options.mode == Mode::kLocalSync
                    ? "LOCAL_SYNC immediate-reopen reconciliation"
                    : "not applicable; APPEND_ONLY makes no durable acknowledgment")
      << ",\"reconciliation\":"
      << quoted("reopen validates every record and exact latest full state for every group")
      << "},\n  \"limitations\":["
      << quoted("APPEND_ONLY samples end at complete write and make no durability claim.") << ','
      << quoted("Segment rotation remains inside append and may synchronize the predecessor.")
      << ',' << quoted("LOCAL_SYNC is node-local durability, not quorum commit latency.") << ','
      << quoted(
             "Immediate reopen does not qualify power-loss behavior, device caches, or firmware.")
      << ','
      << quoted("Snapshot transfer, remote catch-up, network traffic, and client acknowledgment "
                "are not measured.")
      << "]\n}\n";
  return write_text(options.output_directory / "manifest.json", output.str());
}

[[nodiscard]] Status create_output_directory(Options& options) {
  std::error_code error;
  options.output_directory = std::filesystem::absolute(options.output_directory, error);
  if (error)
    return Status{StatusCode::kIoError, "cannot resolve benchmark output path"};
  if (std::filesystem::exists(options.output_directory, error) || error)
    return invalid("benchmark output directory already exists or cannot be inspected");
  const std::filesystem::path parent = options.output_directory.parent_path();
  if (!std::filesystem::is_directory(parent, error) || error)
    return invalid("benchmark output parent must already exist and be a directory");
  if (!std::filesystem::create_directory(options.output_directory, error) || error)
    return Status{StatusCode::kIoError, "cannot create benchmark output directory"};
  return Status::ok();
}

int run_main(const int argc, char** const argv) {
  auto parsed = parse_options(argc, argv);
  if (!parsed.has_value()) {
    if (parsed.error().code() == StatusCode::kCancelled) {
      print_usage(argc > 0 ? std::string_view{argv[0]} : "chronos-raftbench");
      return 0;
    }
    std::cerr << parsed.error().to_string() << '\n';
    print_usage(argc > 0 ? std::string_view{argv[0]} : "chronos-raftbench");
    return 2;
  }
  Options options = std::move(*parsed);
  Status status = validate_options(options);
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 2;
  }
  auto record_bytes = encoded_record_size(options);
  if (!record_bytes.has_value()) {
    std::cerr << record_bytes.error().to_string() << '\n';
    return 2;
  }
  status = create_output_directory(options);
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 2;
  }
  status = write_manifest(options, *record_bytes, false);
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 1;
  }

  std::vector<RepetitionResult> results;
  results.reserve(static_cast<std::size_t>(options.repetitions));
  for (std::uint64_t repetition = 1U; repetition <= options.repetitions; ++repetition) {
    auto result = run_repetition(options, repetition);
    if (!result.has_value()) {
      static_cast<void>(
          write_text(options.output_directory / "failure.txt", result.error().to_string() + "\n"));
      std::cerr << result.error().to_string() << '\n';
      return 1;
    }
    results.push_back(std::move(*result));
  }
  status = write_raw_samples(options, results);
  if (status.is_ok())
    status = write_summary(options, results);
  if (status.is_ok())
    status = write_manifest(options, *record_bytes, true);
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 1;
  }
  std::cout << "Raft persistence benchmark completed with verified recovery; artifacts: "
            << options.output_directory << '\n';
  return 0;
}

} // namespace

int main(const int argc, char** const argv) {
  try {
    return run_main(argc, argv);
  } catch (const std::bad_alloc&) {
    std::cerr << "RESOURCE_EXHAUSTED: chronos-raftbench could not allocate bounded run state\n";
    return 1;
  } catch (const std::exception& error) {
    std::cerr << "INTERNAL: chronos-raftbench encountered an unexpected exception: " << error.what()
              << '\n';
    return 1;
  }
}
