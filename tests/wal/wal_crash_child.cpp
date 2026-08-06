#include "chronos/wal/wal_commit_coordinator.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "io/posix_syscalls.hpp"
#include "wal/wal_crash_protocol.hpp"
#include "wal/wal_writer_internal.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace chronos::wal::test {
namespace {

enum class ChildMode : std::uint8_t {
  kCreate,
  kReopen,
  kReclaim,
};

class AcceptingReplaySink final : public WalReplaySink {
public:
  [[nodiscard]] common::Status preflight(const WalReplayRecord&) override {
    return common::Status::ok();
  }

  [[nodiscard]] common::Status replay(const WalReplayRecord&) override {
    return common::Status::ok();
  }
};

class ProtocolWriter {
public:
  [[nodiscard]] bool send(const std::string_view line) {
    const std::lock_guard lock{mutex_};
    std::cout << line << '\n' << std::flush;
    return std::cout.good();
  }

private:
  std::mutex mutex_;
};

struct ChildConfig {
  std::string directory;
  ChildMode mode{ChildMode::kCreate};
  std::uint64_t target_segment_size{kSegmentSizeLimit};
  std::size_t maximum_sync_batch_requests{64U};
  std::size_t maximum_sync_batch_encoded_bytes{kMaximumRecordLength};
  std::chrono::microseconds maximum_sync_batch_delay{1'000'000};
  std::string pause_after;
  std::uint64_t pause_occurrence{1U};
  std::size_t short_record_prefix{};
};

template <typename Integer>
[[nodiscard]] bool parse_integer(const std::string_view text, Integer& value) {
  if (text.empty()) {
    return false;
  }
  const char* const begin = text.data();
  const char* const end = text.data() + text.size();
  const auto [position, error] = std::from_chars(begin, end, value);
  return error == std::errc{} && position == end;
}

[[nodiscard]] std::optional<ChildConfig> parse_arguments(const int count, char** const values) {
  ChildConfig config;
  for (int index = 1; index < count; index += 2) {
    if (index + 1 >= count) {
      return std::nullopt;
    }
    const std::string_view key{values[index]};
    const std::string_view value{values[index + 1]};
    if (key == "--directory") {
      config.directory = value;
    } else if (key == "--mode") {
      if (value == "create") {
        config.mode = ChildMode::kCreate;
      } else if (value == "reopen") {
        config.mode = ChildMode::kReopen;
      } else if (value == "reclaim") {
        config.mode = ChildMode::kReclaim;
      } else {
        return std::nullopt;
      }
    } else if (key == "--target-segment-size") {
      if (!parse_integer(value, config.target_segment_size)) {
        return std::nullopt;
      }
    } else if (key == "--max-batch-requests") {
      if (!parse_integer(value, config.maximum_sync_batch_requests)) {
        return std::nullopt;
      }
    } else if (key == "--max-batch-bytes") {
      if (!parse_integer(value, config.maximum_sync_batch_encoded_bytes)) {
        return std::nullopt;
      }
    } else if (key == "--max-delay-us") {
      std::int64_t delay = 0;
      if (!parse_integer(value, delay)) {
        return std::nullopt;
      }
      config.maximum_sync_batch_delay = std::chrono::microseconds{delay};
    } else if (key == "--pause-after") {
      config.pause_after = value;
    } else if (key == "--pause-occurrence") {
      if (!parse_integer(value, config.pause_occurrence)) {
        return std::nullopt;
      }
    } else if (key == "--short-record-prefix") {
      if (!parse_integer(value, config.short_record_prefix)) {
        return std::nullopt;
      }
    } else {
      return std::nullopt;
    }
  }
  if (config.directory.empty() || config.pause_occurrence == 0U) {
    return std::nullopt;
  }
  return config;
}

class ObservingPosixSyscalls final : public io::detail::PosixSyscalls {
public:
  struct Config {
    std::string pause_after;
    std::uint64_t pause_occurrence;
    std::size_t short_record_prefix;
  };

  ObservingPosixSyscalls(io::detail::PosixSyscalls& delegate, ProtocolWriter& protocol,
                         Config config)
      : delegate_(delegate), protocol_(protocol), config_(std::move(config)) {}

  int open_directory(const char* const path, const int flags) override {
    const int descriptor = delegate_.open_directory(path, flags);
    if (descriptor >= 0) {
      directory_descriptor_ = descriptor;
    }
    return descriptor;
  }

  int open_at(const io::detail::OpenAtRequest& request) override {
    const int descriptor = delegate_.open_at(request);
    return descriptor;
  }

  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }

  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    if (request.offset >= static_cast<off_t>(kSegmentHeaderSize) &&
        config_.short_record_prefix != 0U && !short_record_write_used_) {
      short_record_write_used_ = true;
      io::detail::WriteAtRequest shortened = request;
      shortened.size = std::min(config_.short_record_prefix, request.size);
      const ssize_t result = delegate_.pwrite(shortened);
      if (result >= 0) {
        observe(kAfterShortRecordWrite);
      }
      return result;
    }
    const ssize_t result = delegate_.pwrite(request);
    if (result >= 0 && static_cast<std::size_t>(result) == request.size) {
      observe(request.offset == 0 ? kAfterSegmentHeaderWrite : kAfterRecordWrite);
    }
    return result;
  }

  int fstat(const int descriptor, struct stat* const metadata) override {
    return delegate_.fstat(descriptor, metadata);
  }

  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }

  int fdatasync(const int descriptor) override {
    const int result = delegate_.fdatasync(descriptor);
    if (result == 0) {
      observe(kAfterDataSync);
    }
    return result;
  }

  int fsync(const int descriptor) override {
    const int result = delegate_.fsync(descriptor);
    if (result == 0) {
      if (reclamation_started_ && descriptor == directory_descriptor_) {
        observe(kAfterReclamationDirectorySync);
      } else {
        observe(descriptor == directory_descriptor_ ? kAfterSegmentDirectorySync
                                                    : kAfterSegmentFileSync);
      }
    }
    return result;
  }

  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    const int result = delegate_.rename_no_replace(request);
    if (result == 0) {
      observe(kAfterSegmentRename);
    }
    return result;
  }

  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }

  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }

  int unlink_at(const int directory_descriptor, const char* const name) override {
    const int result = delegate_.unlink_at(directory_descriptor, name);
    if (result == 0 && reclamation_started_) {
      observe(kAfterReclamationRemove);
    }
    return result;
  }

  int close(const int descriptor) override {
    return delegate_.close(descriptor);
  }

  void begin_reclamation() noexcept {
    reclamation_started_ = true;
  }

private:
  void observe(const std::string_view point) {
    std::uint64_t& occurrence = occurrences_[std::string{point}];
    if (occurrence != std::numeric_limits<std::uint64_t>::max()) {
      ++occurrence;
    }
    if (point != config_.pause_after || occurrence != config_.pause_occurrence) {
      return;
    }
    static_cast<void>(
        protocol_.send("FAILPOINT " + std::string{point} + " " + std::to_string(occurrence)));
    for (;;) {
      static_cast<void>(::pause());
    }
  }

  io::detail::PosixSyscalls& delegate_;
  ProtocolWriter& protocol_;
  Config config_;
  int directory_descriptor_{-1};
  bool short_record_write_used_{false};
  bool reclamation_started_{false};
  std::unordered_map<std::string, std::uint64_t> occurrences_;
};

[[nodiscard]] std::string mode_name(const WalDurabilityMode mode) {
  return mode == WalDurabilityMode::kAsync ? "ASYNC" : "LOCAL_SYNC";
}

[[nodiscard]] std::optional<WalDurabilityMode> parse_mode(const std::string_view text) {
  if (text == "ASYNC") {
    return WalDurabilityMode::kAsync;
  }
  if (text == "LOCAL_SYNC") {
    return WalDurabilityMode::kLocalSync;
  }
  return std::nullopt;
}

[[nodiscard]] int report_start_failure(ProtocolWriter& protocol, const common::Status& status) {
  static_cast<void>(
      protocol.send("ERROR " + std::to_string(static_cast<int>(status.code())) + " startup"));
  return 2;
}

} // namespace
} // namespace chronos::wal::test

int main(const int argc, char** const argv) {
  try {
    using namespace chronos;
    using namespace chronos::wal;
    using namespace chronos::wal::test;

    ProtocolWriter protocol;
    const std::optional<ChildConfig> child_config = parse_arguments(argc, argv);
    if (!child_config.has_value()) {
      static_cast<void>(protocol.send("ERROR 0 arguments"));
      return 64;
    }

    const WalWriterConfig writer_config{
        .directory_path = child_config->directory,
        .target_segment_size = child_config->target_segment_size,
        .maximum_application_payload = kCrashPayloadSize,
    };
    ObservingPosixSyscalls syscalls{io::detail::system_posix_syscalls(), protocol,
                                    ObservingPosixSyscalls::Config{
                                        .pause_after = child_config->pause_after,
                                        .pause_occurrence = child_config->pause_occurrence,
                                        .short_record_prefix = child_config->short_record_prefix,
                                    }};
    common::Result<WalWriter> writer = common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "writer startup was not attempted"});
    AcceptingReplaySink replay_sink;
    if (child_config->mode != ChildMode::kCreate) {
      writer = chronos::wal::detail::WalWriterTestAccess::open_existing(
          writer_config, WalRecoveryOptions{}, replay_sink, syscalls);
    } else {
      FixedWalIdGenerator generator{make_wal_id(0x42U)};
      writer =
          chronos::wal::detail::WalWriterTestAccess::create_new(writer_config, generator, syscalls);
    }
    if (!writer.has_value()) {
      return report_start_failure(protocol, writer.error());
    }

    if (child_config->mode == ChildMode::kReclaim) {
      if (!protocol.send("READY")) {
        return 3;
      }
      std::string command;
      if (!std::getline(std::cin, command)) {
        return 4;
      }
      std::istringstream input{command};
      std::string operation;
      std::uint64_t record_sequence = 0U;
      std::uint64_t segment_number = 0U;
      std::uint64_t byte_offset = 0U;
      std::string extra;
      if (!(input >> operation >> record_sequence >> segment_number >> byte_offset) ||
          operation != "RECLAIM" || (input >> extra)) {
        static_cast<void>(protocol.send("ERROR 0 reclaim-command"));
        return 5;
      }
      syscalls.begin_reclamation();
      const common::Result<WalSegmentReclamationReport> reclaimed =
          writer->reclaim_checkpointed_segments({.wal_id = writer->wal_id(),
                                                 .record_sequence = record_sequence,
                                                 .segment_number = segment_number,
                                                 .byte_offset = byte_offset});
      if (!reclaimed.has_value()) {
        static_cast<void>(protocol.send("ERROR " +
                                        std::to_string(static_cast<int>(reclaimed.error().code())) +
                                        " reclamation"));
        return 6;
      }
      static_cast<void>(protocol.send("RECLAIMED " +
                                      std::to_string(reclaimed->removed_segment_count) + " " +
                                      std::to_string(reclaimed->removed_physical_bytes) + " " +
                                      std::to_string(reclaimed->directory_sync_count)));
      return writer->close().is_ok() ? 0 : 7;
    }

    const WalCommitCoordinatorConfig coordinator_config{
        .maximum_pending_requests = 128U,
        .maximum_pending_encoded_bytes = std::size_t{8U} * 1024U * 1024U,
        .maximum_sync_batch_requests = child_config->maximum_sync_batch_requests,
        .maximum_sync_batch_encoded_bytes = child_config->maximum_sync_batch_encoded_bytes,
        .maximum_sync_batch_delay = child_config->maximum_sync_batch_delay,
    };
    common::Result<WalCommitCoordinator> started =
        WalCommitCoordinator::start(std::move(*writer), coordinator_config);
    if (!started.has_value()) {
      return report_start_failure(protocol, started.error());
    }
    WalCommitCoordinator coordinator = std::move(*started);
    if (!protocol.send("READY")) {
      return 3;
    }

    std::vector<std::thread> completion_threads;
    std::string command;
    while (std::getline(std::cin, command)) {
      std::istringstream input{command};
      std::string operation;
      input >> operation;
      if (operation == "SUBMIT") {
        std::uint64_t request_id = 0;
        std::string durability_text;
        std::string extra;
        if (!(input >> request_id >> durability_text) || (input >> extra)) {
          static_cast<void>(protocol.send("ERROR 0 command"));
          continue;
        }
        const std::optional<WalDurabilityMode> durability = parse_mode(durability_text);
        if (!durability.has_value()) {
          static_cast<void>(protocol.send("ERROR 0 durability"));
          continue;
        }
        const std::vector<std::byte> payload = make_crash_payload(request_id);
        common::Result<WalCommitCompletion> submitted =
            coordinator.try_submit(payload, *durability);
        if (!submitted.has_value()) {
          static_cast<void>(
              protocol.send("REJECTED " + std::to_string(request_id) + " " +
                            std::to_string(static_cast<int>(submitted.error().code()))));
          continue;
        }
        if (!protocol.send("ADMITTED " + std::to_string(request_id))) {
          return 4;
        }
        try {
          completion_threads.emplace_back([request_id, durability = *durability,
                                           completion = std::move(*submitted), &protocol] {
            const common::Result<WalCommitResult> result = completion.wait();
            if (!result.has_value()) {
              static_cast<void>(
                  protocol.send("FAILED " + std::to_string(request_id) + " " +
                                std::to_string(static_cast<int>(result.error().code()))));
              return;
            }
            static_cast<void>(protocol.send("COMPLETED " + std::to_string(request_id) + " " +
                                            mode_name(durability) + " " +
                                            std::to_string(result->append.record_sequence) + " " +
                                            std::to_string(result->admission_sequence)));
          });
        } catch (const std::system_error&) {
          static_cast<void>(protocol.send("ERROR 0 thread"));
          return 5;
        }
        continue;
      }
      if (operation == "SHUTDOWN") {
        const common::Status status = coordinator.shutdown();
        for (std::thread& thread : completion_threads) {
          if (thread.joinable()) {
            thread.join();
          }
        }
        const WalCommitMetrics metrics = coordinator.metrics();
        static_cast<void>(
            protocol.send("METRICS " + std::to_string(metrics.synchronization_attempts) + " " +
                          std::to_string(metrics.local_sync_batches) + " " +
                          std::to_string(metrics.local_sync_requests_in_batches) + " " +
                          std::to_string(metrics.acknowledged_async_requests) + " " +
                          std::to_string(metrics.acknowledged_local_sync_requests)));
        static_cast<void>(
            protocol.send(std::string{"SHUTDOWN "} + (status.is_ok() ? "OK" : "ERROR")));
        return status.is_ok() ? 0 : 6;
      }
      static_cast<void>(protocol.send("ERROR 0 unknown_command"));
    }

    static_cast<void>(coordinator.shutdown());
    for (std::thread& thread : completion_threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
    return 0;
  } catch (const std::exception&) {
    return 70;
  } catch (...) {
    return 71;
  }
}
