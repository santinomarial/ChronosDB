#include "chronos/common/version.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_commit_coordinator.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_writer.hpp"

#include <algorithm>
#include <atomic>
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
#include <mutex>
#include <new>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <system_error>
#include <thread>
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
using chronos::wal::WalCommitCoordinator;
using chronos::wal::WalCommitCoordinatorConfig;
using chronos::wal::WalCommitMetrics;
using chronos::wal::WalCommitResult;
using chronos::wal::WalDurabilityMode;
using chronos::wal::WalRecoveryReport;
using chronos::wal::WalReplayRecord;
using chronos::wal::WalReplaySink;
using chronos::wal::WalWriter;
using chronos::wal::WalWriterConfig;

constexpr std::string_view kScenarioVersion = "wal-v1-append-recovery/1";
constexpr std::uint64_t kDefaultMaximumArtifactBytes = std::uint64_t{1U} << 30U;

struct Options {
  std::filesystem::path output_directory;
  WalDurabilityMode durability{WalDurabilityMode::kLocalSync};
  std::uint64_t operations{10'000U};
  std::uint64_t warmup_operations{1'000U};
  std::uint64_t repetitions{3U};
  std::uint64_t seed{0x4348524f4e4f5355ULL};
  std::size_t producers{4U};
  std::size_t payload_bytes{256U};
  std::uint64_t target_segment_bytes{chronos::wal::kSegmentSizeLimit};
  std::size_t maximum_pending_requests{1024U};
  std::size_t maximum_pending_bytes{std::size_t{64U} * 1024U * 1024U};
  std::size_t maximum_sync_batch_requests{64U};
  std::size_t maximum_sync_batch_bytes{chronos::wal::kMaximumRecordLength};
  std::chrono::microseconds maximum_sync_delay{1000};
  std::uint64_t maximum_artifact_bytes{kDefaultMaximumArtifactBytes};
  bool allow_dirty{false};
  bool allow_non_release{false};
  std::vector<std::string> invocation;
};

struct Sample {
  std::uint64_t ordinal{};
  std::uint64_t request_id{};
  std::uint64_t latency_ns{};
  std::uint64_t admission_sequence{};
  std::uint64_t record_sequence{};
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
  std::uint64_t physical_wal_bytes{};
  std::uint64_t segment_count{};
  WalCommitMetrics measured_metrics;
  ResourceSnapshot resources_before;
  ResourceSnapshot resources_after;
  std::vector<Sample> samples;
};

struct Phase {
  std::uint64_t first_request_id{};
  std::uint64_t operation_count{};
  std::vector<Sample>* samples{};
};

[[nodiscard]] Status invalid(std::string message) {
  return Status{StatusCode::kInvalidArgument, std::move(message)};
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

void print_usage(const std::string_view program) {
  std::cerr << "Usage: " << program << " --output-dir PATH [options]\n"
            << "  --mode ASYNC|LOCAL_SYNC\n"
            << "  --operations N --warmup-operations N --repetitions N --producers N\n"
            << "  --payload-bytes N --target-segment-bytes N --seed N\n"
            << "  --max-pending-requests N --max-pending-bytes N\n"
            << "  --max-sync-batch-requests N --max-sync-batch-bytes N --max-sync-delay-us N\n"
            << "  --maximum-artifact-bytes N --allow-dirty --allow-non-release\n";
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
    } else if (key == "--mode") {
      if (value == "ASYNC") {
        options.durability = WalDurabilityMode::kAsync;
      } else if (value == "LOCAL_SYNC") {
        options.durability = WalDurabilityMode::kLocalSync;
      } else {
        return chronos::common::make_unexpected(invalid("--mode must be ASYNC or LOCAL_SYNC"));
      }
    } else if (key == "--operations") {
      if (!parse_integer(value, options.operations)) {
        return chronos::common::make_unexpected(invalid("invalid --operations value"));
      }
    } else if (key == "--warmup-operations") {
      if (!parse_integer(value, options.warmup_operations)) {
        return chronos::common::make_unexpected(invalid("invalid --warmup-operations value"));
      }
    } else if (key == "--repetitions") {
      if (!parse_integer(value, options.repetitions)) {
        return chronos::common::make_unexpected(invalid("invalid --repetitions value"));
      }
    } else if (key == "--producers") {
      if (!parse_integer(value, options.producers)) {
        return chronos::common::make_unexpected(invalid("invalid --producers value"));
      }
    } else if (key == "--payload-bytes") {
      if (!parse_integer(value, options.payload_bytes)) {
        return chronos::common::make_unexpected(invalid("invalid --payload-bytes value"));
      }
    } else if (key == "--target-segment-bytes") {
      if (!parse_integer(value, options.target_segment_bytes)) {
        return chronos::common::make_unexpected(invalid("invalid --target-segment-bytes value"));
      }
    } else if (key == "--seed") {
      if (!parse_integer(value, options.seed)) {
        return chronos::common::make_unexpected(invalid("invalid --seed value"));
      }
    } else if (key == "--max-pending-requests") {
      if (!parse_integer(value, options.maximum_pending_requests)) {
        return chronos::common::make_unexpected(invalid("invalid --max-pending-requests value"));
      }
    } else if (key == "--max-pending-bytes") {
      if (!parse_integer(value, options.maximum_pending_bytes)) {
        return chronos::common::make_unexpected(invalid("invalid --max-pending-bytes value"));
      }
    } else if (key == "--max-sync-batch-requests") {
      if (!parse_integer(value, options.maximum_sync_batch_requests)) {
        return chronos::common::make_unexpected(invalid("invalid --max-sync-batch-requests value"));
      }
    } else if (key == "--max-sync-batch-bytes") {
      if (!parse_integer(value, options.maximum_sync_batch_bytes)) {
        return chronos::common::make_unexpected(invalid("invalid --max-sync-batch-bytes value"));
      }
    } else if (key == "--max-sync-delay-us") {
      std::int64_t delay = 0;
      if (!parse_integer(value, delay) || delay < 0) {
        return chronos::common::make_unexpected(invalid("invalid --max-sync-delay-us value"));
      }
      options.maximum_sync_delay = std::chrono::microseconds{delay};
    } else if (key == "--maximum-artifact-bytes") {
      if (!parse_integer(value, options.maximum_artifact_bytes)) {
        return chronos::common::make_unexpected(invalid("invalid --maximum-artifact-bytes value"));
      }
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
  if (options.operations == 0U || options.repetitions == 0U || options.producers == 0U) {
    return invalid("operations, repetitions, and producers must be nonzero");
  }
  constexpr std::size_t kMinimumBenchmarkPayload =
      chronos::wal::kApplicationEnvelopeSize + sizeof(std::uint64_t);
  if (options.payload_bytes < kMinimumBenchmarkPayload ||
      options.payload_bytes > chronos::wal::kMaximumPayloadLength) {
    return invalid("payload bytes cannot hold the WAL application envelope and request identity");
  }
  const Result<chronos::wal::RecordLayout> layout =
      chronos::wal::calculate_record_layout(options.payload_bytes);
  if (!layout.has_value()) {
    return layout.error();
  }
  if (options.producers > options.maximum_pending_requests) {
    return invalid("producer count exceeds the configured pending-request bound");
  }
  if (layout->total_length > options.maximum_pending_bytes / options.producers ||
      layout->total_length > options.maximum_sync_batch_bytes) {
    return invalid("configured pending or sync-batch bytes cannot cover the producer working set");
  }
  if (options.target_segment_bytes <= chronos::wal::kSegmentHeaderSize ||
      options.target_segment_bytes > chronos::wal::kSegmentSizeLimit ||
      options.target_segment_bytes < chronos::wal::kSegmentHeaderSize + layout->total_length) {
    return invalid("target segment bytes cannot hold one configured record within WAL v1");
  }
  if (options.operations > std::numeric_limits<std::uint64_t>::max() - options.warmup_operations) {
    return invalid("operation count overflows uint64");
  }
  const std::uint64_t records_per_run = options.operations + options.warmup_operations;
  if (records_per_run > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max()) - 1U) {
    return invalid("operation count exceeds this platform's addressable recovery state");
  }
  constexpr std::uint64_t kFixedArtifactAllowance = 64ULL * 1024ULL;
  constexpr std::uint64_t kMaximumTextArtifactBytesPerRecord = 1024U;
  if (options.maximum_artifact_bytes <= kFixedArtifactAllowance) {
    return invalid("--maximum-artifact-bytes is too small for fixed run metadata");
  }
  const std::uint64_t variable_artifact_budget =
      options.maximum_artifact_bytes - kFixedArtifactAllowance;
  const std::uint64_t maximum_artifact_bytes_per_record =
      layout->total_length + chronos::wal::kSegmentHeaderSize + kMaximumTextArtifactBytesPerRecord;
  if (records_per_run != 0U &&
      maximum_artifact_bytes_per_record > variable_artifact_budget / records_per_run) {
    return invalid("benchmark run exceeds --maximum-artifact-bytes; raise it explicitly");
  }
  const std::uint64_t maximum_bytes_per_repetition =
      records_per_run * maximum_artifact_bytes_per_record;
  if (maximum_bytes_per_repetition != 0U &&
      options.repetitions > variable_artifact_budget / maximum_bytes_per_repetition) {
    return invalid("benchmark run exceeds --maximum-artifact-bytes; raise it explicitly");
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

[[nodiscard]] std::vector<std::byte> make_payload(const Options& options,
                                                  const std::uint64_t request_id) {
  std::vector<std::byte> payload(options.payload_bytes);
  payload[0] = std::byte{1};
  payload[4] = std::byte{1};
  for (std::size_t index = 0; index < sizeof(request_id); ++index) {
    payload[chronos::wal::kApplicationEnvelopeSize + index] =
        static_cast<std::byte>((request_id >> (index * 8U)) & 0xffU);
  }
  std::uint64_t state = options.seed ^ (request_id * 0x9e3779b97f4a7c15ULL);
  for (std::size_t index = chronos::wal::kApplicationEnvelopeSize + sizeof(request_id);
       index < payload.size(); ++index) {
    state ^= state >> 12U;
    state ^= state << 25U;
    state ^= state >> 27U;
    payload[index] = static_cast<std::byte>((state * 0x2545f4914f6cdd1dULL) & 0xffU);
  }
  return payload;
}

[[nodiscard]] Result<std::uint64_t> payload_request_id(const chronos::common::ByteView payload) {
  if (payload.size() < chronos::wal::kApplicationEnvelopeSize + sizeof(std::uint64_t)) {
    return chronos::common::make_unexpected(
        Status{StatusCode::kCorruption, "benchmark WAL payload is too short"});
  }
  std::uint64_t value = 0;
  for (std::size_t index = 0; index < sizeof(value); ++index) {
    value |= std::to_integer<std::uint64_t>(payload[chronos::wal::kApplicationEnvelopeSize + index])
             << (index * 8U);
  }
  return value;
}

class VerifyingSink final : public WalReplaySink {
public:
  explicit VerifyingSink(const std::uint64_t expected_records)
      : expected_records_(expected_records),
        seen_(static_cast<std::size_t>(expected_records + 1U)) {}

  [[nodiscard]] Status preflight(const WalReplayRecord& record) override {
    return validate_record(record, false);
  }

  [[nodiscard]] Status replay(const WalReplayRecord& record) override {
    return validate_record(record, true);
  }

  [[nodiscard]] Status finish() const {
    if (replayed_ != expected_records_) {
      return Status{StatusCode::kCorruption, "recovery did not replay the expected record count"};
    }
    if (std::any_of(seen_.begin() + 1, seen_.end(),
                    [](const std::uint8_t count) { return count != 1U; })) {
      return Status{StatusCode::kCorruption,
                    "recovery omitted or duplicated a benchmark request identity"};
    }
    return Status::ok();
  }

private:
  [[nodiscard]] Status validate_record(const WalReplayRecord& record, const bool replay) {
    const std::uint64_t expected_sequence = replay ? replayed_ + 1U : preflighted_ + 1U;
    if (record.header.record_sequence != expected_sequence) {
      return Status{StatusCode::kCorruption, "benchmark recovery observed a sequence gap"};
    }
    const Result<std::uint64_t> request_id = payload_request_id(record.payload);
    if (!request_id.has_value()) {
      return request_id.error();
    }
    if (*request_id == 0U || *request_id > expected_records_) {
      return Status{StatusCode::kCorruption,
                    "benchmark recovery observed an out-of-range request identity"};
    }
    if (replay) {
      std::uint8_t& count = seen_[static_cast<std::size_t>(*request_id)];
      if (count != 0U) {
        return Status{StatusCode::kCorruption,
                      "benchmark recovery observed a duplicate request identity"};
      }
      count = 1U;
      ++replayed_;
    } else {
      ++preflighted_;
    }
    return Status::ok();
  }

  std::uint64_t expected_records_{};
  std::uint64_t preflighted_{};
  std::uint64_t replayed_{};
  std::vector<std::uint8_t> seen_;
};

[[nodiscard]] std::optional<std::uint64_t> read_proc_io_value(const std::string_view key) {
#if defined(__linux__)
  std::ifstream input{"/proc/self/io"};
  std::string name;
  std::uint64_t value = 0;
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

[[nodiscard]] std::uint64_t timeval_microseconds(const timeval& value) {
  return (static_cast<std::uint64_t>(value.tv_sec) * 1'000'000U) +
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
  return ResourceSnapshot{.user_cpu_us = timeval_microseconds(usage.ru_utime),
                          .system_cpu_us = timeval_microseconds(usage.ru_stime),
                          .maximum_rss_bytes = maximum_rss,
                          .disk_read_bytes = read_proc_io_value("read_bytes:"),
                          .disk_written_bytes = read_proc_io_value("write_bytes:")};
}

[[nodiscard]] WalCommitMetrics metrics_difference(const WalCommitMetrics& after,
                                                  const WalCommitMetrics& before) {
  WalCommitMetrics result;
  result.admitted_requests = after.admitted_requests - before.admitted_requests;
  result.admitted_encoded_bytes = after.admitted_encoded_bytes - before.admitted_encoded_bytes;
  result.rejected_requests = after.rejected_requests - before.rejected_requests;
  result.appended_requests = after.appended_requests - before.appended_requests;
  result.appended_encoded_bytes = after.appended_encoded_bytes - before.appended_encoded_bytes;
  result.acknowledged_async_requests =
      after.acknowledged_async_requests - before.acknowledged_async_requests;
  result.acknowledged_async_encoded_bytes =
      after.acknowledged_async_encoded_bytes - before.acknowledged_async_encoded_bytes;
  result.acknowledged_local_sync_requests =
      after.acknowledged_local_sync_requests - before.acknowledged_local_sync_requests;
  result.acknowledged_local_sync_encoded_bytes =
      after.acknowledged_local_sync_encoded_bytes - before.acknowledged_local_sync_encoded_bytes;
  result.failed_requests = after.failed_requests - before.failed_requests;
  result.failed_encoded_bytes = after.failed_encoded_bytes - before.failed_encoded_bytes;
  result.synchronization_attempts =
      after.synchronization_attempts - before.synchronization_attempts;
  result.successful_synchronizations =
      after.successful_synchronizations - before.successful_synchronizations;
  result.failed_synchronizations = after.failed_synchronizations - before.failed_synchronizations;
  result.local_sync_batches = after.local_sync_batches - before.local_sync_batches;
  result.local_sync_requests_in_batches =
      after.local_sync_requests_in_batches - before.local_sync_requests_in_batches;
  result.maximum_observed_local_sync_batch_requests =
      after.maximum_observed_local_sync_batch_requests;
  result.maximum_observed_local_sync_batch_encoded_bytes =
      after.maximum_observed_local_sync_batch_encoded_bytes;
  result.accepting = after.accepting;
  result.terminal_failure = after.terminal_failure;
  return result;
}

[[nodiscard]] Status run_phase(WalCommitCoordinator& coordinator, const Options& options,
                               const Phase& phase, std::uint64_t& elapsed_ns) {
  if (phase.operation_count == 0U) {
    elapsed_ns = 0U;
    return Status::ok();
  }
  const std::size_t thread_count = static_cast<std::size_t>(std::min<std::uint64_t>(
      phase.operation_count, static_cast<std::uint64_t>(options.producers)));
  std::atomic<std::uint64_t> next{0U};
  std::atomic<std::size_t> ready{0U};
  std::atomic<bool> begin{false};
  std::mutex failure_mutex;
  Status failure;
  if (phase.samples != nullptr) {
    phase.samples->resize(static_cast<std::size_t>(phase.operation_count));
  }

  std::vector<std::thread> workers;
  workers.reserve(thread_count);
  try {
    for (std::size_t thread_index = 0; thread_index < thread_count; ++thread_index) {
      workers.emplace_back([&] {
        ready.fetch_add(1U, std::memory_order_release);
        while (!begin.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        while (true) {
          const std::uint64_t ordinal = next.fetch_add(1U, std::memory_order_relaxed);
          if (ordinal >= phase.operation_count) {
            return;
          }
          const std::uint64_t request_id = phase.first_request_id + ordinal;
          std::vector<std::byte> payload = make_payload(options, request_id);
          const auto started = std::chrono::steady_clock::now();
          Result<chronos::wal::WalCommitCompletion> submitted =
              coordinator.try_submit(payload, options.durability);
          if (!submitted.has_value()) {
            const std::lock_guard lock{failure_mutex};
            if (failure.is_ok()) {
              failure = submitted.error();
            }
            return;
          }
          const Result<WalCommitResult> completed = submitted->wait();
          const auto finished = std::chrono::steady_clock::now();
          if (!completed.has_value()) {
            const std::lock_guard lock{failure_mutex};
            if (failure.is_ok()) {
              failure = completed.error();
            }
            return;
          }
          if (completed->requested_durability != options.durability ||
              completed->effective_durability != options.durability ||
              (options.durability == WalDurabilityMode::kLocalSync &&
               (!completed->synchronization_position.has_value() ||
                !completed->durable_record_sequence.has_value() ||
                *completed->durable_record_sequence < completed->append.record_sequence))) {
            const std::lock_guard lock{failure_mutex};
            if (failure.is_ok()) {
              failure = Status{StatusCode::kInternal,
                               "benchmark observed a weakened completion durability result"};
            }
            return;
          }
          if (phase.samples != nullptr) {
            (*phase.samples)[static_cast<std::size_t>(ordinal)] = Sample{
                .ordinal = ordinal,
                .request_id = request_id,
                .latency_ns = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started)
                        .count()),
                .admission_sequence = completed->admission_sequence,
                .record_sequence = completed->append.record_sequence,
            };
          }
        }
      });
    }
  } catch (const std::system_error& error) {
    begin.store(true, std::memory_order_release);
    for (std::thread& worker : workers) {
      worker.join();
    }
    return Status{StatusCode::kResourceExhausted,
                  std::string{"cannot start benchmark producer: "} + error.what()};
  }
  while (ready.load(std::memory_order_acquire) != thread_count) {
    std::this_thread::yield();
  }
  const auto started = std::chrono::steady_clock::now();
  begin.store(true, std::memory_order_release);
  for (std::thread& worker : workers) {
    worker.join();
  }
  const auto finished = std::chrono::steady_clock::now();
  elapsed_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());
  if (!failure.is_ok()) {
    return failure;
  }
  return Status::ok();
}

[[nodiscard]] Status validate_measured_sequences(const std::vector<Sample>& samples,
                                                 const std::uint64_t first_sequence) {
  std::vector<std::uint64_t> sequences;
  sequences.reserve(samples.size());
  for (const Sample& sample : samples) {
    sequences.push_back(sample.record_sequence);
  }
  std::sort(sequences.begin(), sequences.end());
  for (std::size_t index = 0; index < sequences.size(); ++index) {
    if (sequences[index] != first_sequence + static_cast<std::uint64_t>(index)) {
      return Status{StatusCode::kCorruption,
                    "measured completions contain a duplicate or record-sequence gap"};
    }
  }
  return Status::ok();
}

[[nodiscard]] Status validate_measured_metrics(const WalCommitMetrics& metrics,
                                               const Options& options) {
  if (metrics.admitted_requests != options.operations ||
      metrics.appended_requests != options.operations || metrics.rejected_requests != 0U ||
      metrics.failed_requests != 0U || metrics.failed_synchronizations != 0U) {
    return Status{StatusCode::kInternal,
                  "benchmark coordinator metrics do not match the measured operation set"};
  }
  if (options.durability == WalDurabilityMode::kAsync) {
    if (metrics.acknowledged_async_requests != options.operations ||
        metrics.acknowledged_local_sync_requests != 0U) {
      return Status{StatusCode::kInternal,
                    "benchmark ASYNC acknowledgment metrics are inconsistent"};
    }
  } else if (metrics.acknowledged_local_sync_requests != options.operations ||
             metrics.acknowledged_async_requests != 0U ||
             metrics.successful_synchronizations == 0U || metrics.local_sync_batches == 0U) {
    return Status{StatusCode::kInternal,
                  "benchmark LOCAL_SYNC acknowledgment metrics are inconsistent"};
  }
  return Status::ok();
}

[[nodiscard]] Status synchronize_parent(const std::filesystem::path& child) {
  Result<chronos::io::PosixDirectory> parent =
      chronos::io::PosixDirectory::open(child.parent_path().string());
  if (!parent.has_value()) {
    return parent.error();
  }
  Status status = parent->sync();
  const Status close_status = parent->close();
  return status.is_ok() ? close_status : status;
}

[[nodiscard]] Result<RepetitionResult> run_repetition(const Options& options,
                                                      const std::uint64_t repetition) {
  const std::filesystem::path repetition_directory =
      options.output_directory / ("repetition-" + std::to_string(repetition));
  const std::filesystem::path wal_directory = repetition_directory / "wal";
  std::error_code error;
  if (!std::filesystem::create_directory(repetition_directory, error) || error ||
      !std::filesystem::create_directory(wal_directory, error) || error) {
    return chronos::common::make_unexpected(
        Status{StatusCode::kIoError, "cannot create fresh benchmark repetition directories"});
  }
  Status status = synchronize_parent(wal_directory);
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }

  const WalWriterConfig writer_config{.directory_path = wal_directory.string(),
                                      .target_segment_size = options.target_segment_bytes,
                                      .maximum_application_payload = options.payload_bytes};
  Result<WalWriter> writer = WalWriter::create_new(writer_config);
  if (!writer.has_value()) {
    return chronos::common::make_unexpected(writer.error());
  }
  const WalCommitCoordinatorConfig coordinator_config{
      .maximum_pending_requests = options.maximum_pending_requests,
      .maximum_pending_encoded_bytes = options.maximum_pending_bytes,
      .maximum_sync_batch_requests = options.maximum_sync_batch_requests,
      .maximum_sync_batch_encoded_bytes = options.maximum_sync_batch_bytes,
      .maximum_sync_batch_delay = options.maximum_sync_delay,
  };
  Result<WalCommitCoordinator> started =
      WalCommitCoordinator::start(std::move(*writer), coordinator_config);
  if (!started.has_value()) {
    return chronos::common::make_unexpected(started.error());
  }
  WalCommitCoordinator coordinator = std::move(*started);

  std::uint64_t ignored_elapsed = 0U;
  status = run_phase(coordinator, options,
                     Phase{.first_request_id = 1U,
                           .operation_count = options.warmup_operations,
                           .samples = nullptr},
                     ignored_elapsed);
  if (!status.is_ok()) {
    static_cast<void>(coordinator.shutdown());
    return chronos::common::make_unexpected(status);
  }
  const WalCommitMetrics before_metrics = coordinator.metrics();
  const ResourceSnapshot before_resources = resource_snapshot();

  RepetitionResult result;
  result.index = repetition;
  status = run_phase(coordinator, options,
                     Phase{.first_request_id = options.warmup_operations + 1U,
                           .operation_count = options.operations,
                           .samples = &result.samples},
                     result.elapsed_ns);
  const ResourceSnapshot after_resources = resource_snapshot();
  const WalCommitMetrics after_metrics = coordinator.metrics();
  const Status shutdown_status = coordinator.shutdown();
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }
  if (!shutdown_status.is_ok()) {
    return chronos::common::make_unexpected(shutdown_status);
  }
  status = validate_measured_sequences(result.samples, options.warmup_operations + 1U);
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }

  const std::uint64_t expected_records = options.warmup_operations + options.operations;
  VerifyingSink sink{expected_records};
  const auto recovery_started = std::chrono::steady_clock::now();
  const Result<WalRecoveryReport> recovered = chronos::wal::recover_wal(writer_config, {}, sink);
  const auto recovery_finished = std::chrono::steady_clock::now();
  if (!recovered.has_value()) {
    return chronos::common::make_unexpected(recovered.error());
  }
  status = sink.finish();
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }
  if (recovered->record_count != expected_records ||
      recovered->last_record_sequence != expected_records || recovered->repaired) {
    return chronos::common::make_unexpected(Status{
        StatusCode::kCorruption, "benchmark recovery report does not match submitted history"});
  }

  result.recovery_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(recovery_finished - recovery_started)
          .count());
  result.physical_wal_bytes = recovered->physical_bytes;
  result.segment_count = recovered->segment_count;
  result.measured_metrics = metrics_difference(after_metrics, before_metrics);
  status = validate_measured_metrics(result.measured_metrics, options);
  if (!status.is_ok()) {
    return chronos::common::make_unexpected(status);
  }
  result.resources_before = before_resources;
  result.resources_after = after_resources;
  return result;
}

[[nodiscard]] std::string durability_name(const WalDurabilityMode mode) {
  return mode == WalDurabilityMode::kAsync ? "ASYNC" : "LOCAL_SYNC";
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
      if (colon != std::string::npos) {
        return line.substr(colon + 2U);
      }
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
  for (std::size_t index = 0; index < invocation.size(); ++index) {
    if (index != 0U) {
      output << ',';
    }
    output << quoted(invocation[index]);
  }
  output << ']';
  return output.str();
}

[[nodiscard]] std::uint64_t percentile_nearest_rank(std::vector<std::uint64_t> values,
                                                    const long double percentile) {
  std::sort(values.begin(), values.end());
  const long double rank = std::ceil(percentile * static_cast<long double>(values.size()));
  const std::size_t index = static_cast<std::size_t>(std::max<long double>(1.0L, rank)) - 1U;
  return values[index];
}

[[nodiscard]] Status write_text(const std::filesystem::path& path, const std::string_view text) {
  std::ofstream output{path, std::ios::binary | std::ios::trunc};
  output.write(text.data(), static_cast<std::streamsize>(text.size()));
  output.flush();
  if (!output.good()) {
    return Status{StatusCode::kIoError, "failed to write benchmark artifact " + path.string()};
  }
  return Status::ok();
}

[[nodiscard]] Status write_raw_samples(const Options& options,
                                       const std::vector<RepetitionResult>& results) {
  std::ostringstream output;
  output << "repetition,ordinal,request_id,latency_ns,admission_sequence,record_sequence,"
            "requested_mode,effective_mode\n";
  const std::string mode = durability_name(options.durability);
  for (const RepetitionResult& result : results) {
    for (const Sample& sample : result.samples) {
      output << result.index << ',' << sample.ordinal << ',' << sample.request_id << ','
             << sample.latency_ns << ',' << sample.admission_sequence << ','
             << sample.record_sequence << ',' << mode << ',' << mode << '\n';
    }
  }
  return write_text(options.output_directory / "raw-latencies.csv", output.str());
}

[[nodiscard]] Status write_summary(const Options& options,
                                   const std::vector<RepetitionResult>& results) {
  std::ostringstream output;
  output << "{\n  \"schema_version\": 1,\n  \"percentile_method\": "
         << quoted("nearest-rank over per-operation closed-loop end-to-end latency")
         << ",\n  \"repetitions\": [\n";
  for (std::size_t index = 0; index < results.size(); ++index) {
    const RepetitionResult& result = results[index];
    std::vector<std::uint64_t> latencies;
    latencies.reserve(result.samples.size());
    for (const Sample& sample : result.samples) {
      latencies.push_back(sample.latency_ns);
    }
    const long double seconds = static_cast<long double>(result.elapsed_ns) / 1'000'000'000.0L;
    if (result.elapsed_ns == 0U) {
      return Status{StatusCode::kInternal, "benchmark measured a zero elapsed duration"};
    }
    const long double throughput = static_cast<long double>(options.operations) / seconds;
    output << "    {\"index\":" << result.index << ",\"elapsed_ns\":" << result.elapsed_ns
           << ",\"operations_per_second\":" << std::fixed << std::setprecision(6) << throughput
           << R"(,"latency_ns":{"p50":)" << percentile_nearest_rank(latencies, 0.50L)
           << ",\"p95\":" << percentile_nearest_rank(latencies, 0.95L)
           << ",\"p99\":" << percentile_nearest_rank(latencies, 0.99L) << ",\"p99_9\":";
    if (latencies.size() >= 1000U) {
      output << percentile_nearest_rank(latencies, 0.999L);
    } else {
      output << "null";
    }
    output << "},\"recovery_ns\":" << result.recovery_ns
           << ",\"physical_wal_bytes\":" << result.physical_wal_bytes
           << ",\"segments\":" << result.segment_count
           << ",\"sync_attempts\":" << result.measured_metrics.synchronization_attempts
           << ",\"local_sync_batches\":" << result.measured_metrics.local_sync_batches
           << ",\"local_sync_requests\":" << result.measured_metrics.local_sync_requests_in_batches
           << ",\"rejected_requests\":" << result.measured_metrics.rejected_requests
           << ",\"failed_requests\":" << result.measured_metrics.failed_requests
           << ",\"user_cpu_us\":"
           << result.resources_after.user_cpu_us - result.resources_before.user_cpu_us
           << ",\"system_cpu_us\":"
           << result.resources_after.system_cpu_us - result.resources_before.system_cpu_us
           << ",\"peak_rss_bytes\":" << result.resources_after.maximum_rss_bytes
           << ",\"process_disk_read_bytes\":";
    if (result.resources_before.disk_read_bytes.has_value() &&
        result.resources_after.disk_read_bytes.has_value()) {
      output << *result.resources_after.disk_read_bytes - *result.resources_before.disk_read_bytes;
    } else {
      output << "null";
    }
    output << ",\"process_disk_written_bytes\":";
    if (result.resources_before.disk_written_bytes.has_value() &&
        result.resources_after.disk_written_bytes.has_value()) {
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

[[nodiscard]] Status write_manifest(const Options& options, const bool recovery_validated) {
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
  const Result<chronos::wal::RecordLayout> layout =
      chronos::wal::calculate_record_layout(options.payload_bytes);
  if (!layout.has_value()) {
    return layout.error();
  }
  std::error_code space_error;
  const std::filesystem::space_info space =
      std::filesystem::space(options.output_directory, space_error);

  const char* const lang = std::getenv("LANG");
  const char* const lc_all = std::getenv("LC_ALL");
  const char* const timezone = std::getenv("TZ");
  std::ostringstream output;
  output << "{\n"
         << "  \"schema_version\": 1,\n"
         << "  \"scenario\": " << quoted(kScenarioVersion) << ",\n"
         << "  \"created_utc\": " << quoted(utc_timestamp()) << ",\n"
         << "  \"invocation_argv\": " << invocation_json(options.invocation) << ",\n"
         << R"(  "source": {"git_commit":)" << quoted(version.git_commit)
         << ",\"git_metadata_available\":" << (version.git_metadata_available ? "true" : "false")
         << ",\"git_dirty\":" << (version.git_dirty ? "true" : "false")
         << ",\"dirty_diff_artifact_required\":" << (version.git_dirty ? "true" : "false") << "},\n"
         << "  \"workload\": {\"generator\":\"chronos-walbench deterministic xorshift64* "
            "payload generator v1\",\"seed\":"
         << options.seed << ",\"repetitions\":" << options.repetitions
         << ",\"warmup_operations\":" << options.warmup_operations
         << ",\"measured_operations\":" << options.operations
         << ",\"producers\":" << options.producers << ",\"payload_bytes\":" << options.payload_bytes
         << ",\"encoded_record_bytes\":" << layout->total_length
         << ",\"batch_size_distribution\":\"one WAL application entry per request\","
            "\"arrival_model\":\"closed loop; one outstanding request per producer\","
            "\"schema\":\"test-only WAL application envelope plus request identity and "
            "deterministic bytes; no production application kind is allocated\","
            "\"event_time_out_of_order_duplicates_corrections_tombstones\":\"not applicable to "
            "physical WAL benchmark\"},\n"
         << R"(  "wal": {"requested_mode":)" << quoted(durability_name(options.durability))
         << ",\"effective_mode\":" << quoted(durability_name(options.durability))
         << ",\"target_segment_bytes\":" << options.target_segment_bytes
         << ",\"maximum_pending_requests\":" << options.maximum_pending_requests
         << ",\"maximum_pending_bytes\":" << options.maximum_pending_bytes
         << ",\"maximum_sync_batch_requests\":" << options.maximum_sync_batch_requests
         << ",\"maximum_sync_batch_bytes\":" << options.maximum_sync_batch_bytes
         << ",\"maximum_sync_delay_us\":" << options.maximum_sync_delay.count()
         << ",\"replication\":\"single-node\",\"read_consistency\":\"not applicable\","
            "\"flush_compaction_checkpoint_reclamation\":\"outside this physical WAL "
            "benchmark and not benchmarked\"},\n"
         << R"(  "build": {"semantic_version":)" << quoted(version.semantic_version)
         << ",\"build_type\":" << quoted(version.build_type)
         << ",\"compiler\":" << quoted(version.compiler)
         << ",\"standard_library\":" << quoted(standard_library())
         << ",\"target_architecture\":" << quoted(version.target_architecture)
         << ",\"operating_system\":" << quoted(version.operating_system)
         << ",\"compiler_linker_flags\":\"retained by scripts/benchmark-wal.sh in "
            "CMakeCache.txt and compile_commands.json; unknown for direct invocation\"},\n"
         << R"(  "host": {"uname":)"
         << quoted(have_uname
                       ? std::string{system.sysname} + " " + system.release + " " + system.machine
                       : "unknown (uname failed)")
         << ",\"cpu_model\":" << quoted(cpu_model())
         << ",\"logical_cpus\":" << (logical_cpus > 0 ? logical_cpus : 0)
         << ",\"memory_bytes\":" << memory_bytes
         << ",\"filesystem_capacity_bytes\":" << (space_error ? 0U : space.capacity)
         << ",\"cpu_topology_frequency_power_numa\":\"unknown unless retained in "
            "system-inventory.txt by wrapper\",\"memory_channels_hugepages_swap\":\"unknown "
            "unless retained in system-inventory.txt by wrapper\",\"storage_model_firmware_cache_"
            "plp_mount_filesystem\":\"unknown unless retained in system-inventory.txt by "
            "wrapper\",\"network\":\"not used\",\"container_hypervisor_cloud\":\"unknown; "
            "wrapper inventory may provide evidence\"},\n"
         << "  \"environment\": {\"capture_policy\":\"security allowlist; wrapper uses only "
            "declared variables and retains command/inventory artifacts\",\"LANG\":"
         << quoted(lang == nullptr ? "unset" : lang)
         << ",\"LC_ALL\":" << quoted(lc_all == nullptr ? "unset" : lc_all)
         << ",\"TZ\":" << quoted(timezone == nullptr ? "unset" : timezone)
         << ",\"privilege_and_tuning_steps\":\"none performed by chronos-walbench\"},\n"
         << "  \"procedure\": {\"warmup_policy\":\"declared operation count on the same fresh "
            "WAL before measurement\",\"run_order\":\"sequential fresh WAL per repetition; no "
            "randomization\",\"cooldown\":\"none\",\"outlier_policy\":\"none; every successful "
            "sample retained\",\"latency_population\":\"successful measured requests only; any "
            "error invalidates the run\",\"percentiles\":\"nearest-rank; p99.9 null below 1000 "
            "samples\",\"cache_state\":\"uncontrolled host cache; warmup count is explicit\","
            "\"maximum_artifact_bytes_total\":"
         << options.maximum_artifact_bytes << "},\n"
         << R"(  "correctness": {"validation_status":)"
         << quoted(recovery_validated ? "passed" : "pending")
         << ",\"complete_physical_recovery_required\":true,"
            "\"exact_record_sequence_required\":true,\"unique_request_identity_required\":true,"
            "\"zero_rejections_failures_retries_required\":true,\"acknowledged_write_loss_count\":"
         << (recovery_validated ? "0" : "null")
         << ",\"reconciliation\":\"recover_wal preflight/replay checks every submitted warmup "
            "and measured identity exactly once\"},\n"
         << "  \"limitations\": [\"Results are local measurements, not published performance "
            "claims.\",\"ASYNC acknowledgment provides no durability guarantee.\",\"Process "
            "recovery does not qualify power-loss "
            "behavior, firmware, controller caches, filesystems, hypervisors, or network "
            "filesystems.\",\"CPU utilization, allocator counts, device I/O operations, and "
            "steady-state RSS require external profilers when not exposed by this host.\"]\n"
         << "}\n";
  return write_text(options.output_directory / "manifest.json", output.str());
}

[[nodiscard]] Status create_output_directory(Options& options) {
  std::error_code error;
  options.output_directory = std::filesystem::absolute(options.output_directory, error);
  if (error) {
    return Status{StatusCode::kIoError, "cannot resolve benchmark output path"};
  }
  if (std::filesystem::exists(options.output_directory, error) || error) {
    return invalid("benchmark output directory already exists or cannot be inspected");
  }
  const std::filesystem::path parent = options.output_directory.parent_path();
  if (!std::filesystem::is_directory(parent, error) || error) {
    return invalid("benchmark output parent must already exist and be a directory");
  }
  if (!std::filesystem::create_directory(options.output_directory, error) || error) {
    return Status{StatusCode::kIoError, "cannot create benchmark output directory"};
  }
  return Status::ok();
}

int run_main(const int argc, char** const argv) {
  Result<Options> parsed = parse_options(argc, argv);
  if (!parsed.has_value()) {
    if (parsed.error().code() == StatusCode::kCancelled) {
      print_usage(argc > 0 ? std::string_view{argv[0]} : "chronos-walbench");
      return 0;
    }
    std::cerr << parsed.error().to_string() << '\n';
    print_usage(argc > 0 ? std::string_view{argv[0]} : "chronos-walbench");
    return 2;
  }
  Options options = std::move(*parsed);
  Status status = validate_options(options);
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 2;
  }
  status = create_output_directory(options);
  if (!status.is_ok()) {
    std::cerr << status.to_string() << '\n';
    return 2;
  }
  status = write_manifest(options, false);
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

  std::cout << "WAL benchmark completed with verified recovery; artifacts: "
            << options.output_directory << '\n';
  return 0;
}

} // namespace

int main(const int argc, char** const argv) {
  try {
    return run_main(argc, argv);
  } catch (const std::bad_alloc&) {
    std::cerr << "RESOURCE_EXHAUSTED: chronos-walbench could not allocate bounded run state\n";
    return 1;
  } catch (const std::exception& error) {
    std::cerr << "INTERNAL: chronos-walbench encountered an unexpected exception: " << error.what()
              << '\n';
    return 1;
  }
}
