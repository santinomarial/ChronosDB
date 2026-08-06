#ifndef CHRONOS_TESTS_WAL_WAL_CRASH_PROTOCOL_HPP_
#define CHRONOS_TESTS_WAL_WAL_CRASH_PROTOCOL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"
#include "chronos/wal/types.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(CHRONOS_WAL_CRASH_CHILD_PATH) && defined(CHRONOS_TEST_CRASH_CHILD_PATH)
#error "Only one crash-child executable path may be configured"
#endif

#if defined(CHRONOS_WAL_CRASH_CHILD_PATH) || defined(CHRONOS_TEST_CRASH_CHILD_PATH)
#include <csignal>
#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#if defined(__APPLE__)
extern char** environ;
#endif
#endif

namespace chronos::wal::test {

inline constexpr std::size_t kCrashPayloadSize = kApplicationEnvelopeSize + sizeof(std::uint64_t);
inline constexpr std::string_view kAfterSegmentHeaderWrite = "after_segment_header_write";
inline constexpr std::string_view kAfterSegmentFileSync = "after_segment_file_sync";
inline constexpr std::string_view kAfterSegmentRename = "after_segment_rename";
inline constexpr std::string_view kAfterSegmentDirectorySync = "after_segment_directory_sync";
inline constexpr std::string_view kAfterRecordWrite = "after_record_write";
inline constexpr std::string_view kAfterShortRecordWrite = "after_short_record_write";
inline constexpr std::string_view kAfterDataSync = "after_data_sync";
inline constexpr std::string_view kAfterReclamationRemove = "after_reclamation_remove";
inline constexpr std::string_view kAfterReclamationDirectorySync =
    "after_reclamation_directory_sync";

[[nodiscard]] inline std::vector<std::byte> make_crash_payload(const std::uint64_t request_id) {
  std::vector<std::byte> payload(kCrashPayloadSize);
  payload[0] = std::byte{1};
  payload[4] = std::byte{1};
  for (std::size_t index = 0; index < sizeof(request_id); ++index) {
    const auto shift = static_cast<unsigned int>(index * 8U);
    payload[kApplicationEnvelopeSize + index] =
        static_cast<std::byte>((request_id >> shift) & 0xffU);
  }
  return payload;
}

[[nodiscard]] inline common::Result<std::uint64_t>
crash_payload_request_id(const common::ByteView payload) {
  if (payload.size() != kCrashPayloadSize) {
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "crash-harness payload has an unexpected size"});
  }
  std::uint64_t request_id = 0;
  for (std::size_t index = 0; index < sizeof(request_id); ++index) {
    const auto shift = static_cast<unsigned int>(index * 8U);
    request_id |= static_cast<std::uint64_t>(
                      std::to_integer<unsigned int>(payload[kApplicationEnvelopeSize + index]))
                  << shift;
  }
  return request_id;
}

struct CrashEvent {
  std::string name;
  std::vector<std::string> fields;
  std::string raw;
};

[[nodiscard]] inline common::Result<CrashEvent> parse_crash_event(std::string line) {
  if (line.size() > 4096U) {
    return common::make_unexpected(common::Status{common::StatusCode::kOutOfRange,
                                                  "crash-child protocol line exceeds 4096 bytes"});
  }
  std::istringstream input{line};
  CrashEvent event;
  event.raw = std::move(line);
  if (!(input >> event.name)) {
    return common::make_unexpected(common::Status{common::StatusCode::kCorruption,
                                                  "crash-child emitted an empty protocol line"});
  }
  std::string field;
  while (input >> field) {
    event.fields.push_back(std::move(field));
  }
  return event;
}

[[nodiscard]] inline common::Result<std::uint64_t> parse_protocol_u64(const std::string_view text) {
  if (text.empty()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "empty protocol integer"});
  }
  std::uint64_t value = 0;
  for (const char character : text) {
    if (character < '0' || character > '9') {
      return common::make_unexpected(common::Status{
          common::StatusCode::kCorruption, "protocol integer contains a non-decimal byte"});
    }
    const auto digit = static_cast<std::uint64_t>(character - '0');
    if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10U) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kOutOfRange, "protocol integer exceeds uint64"});
    }
    value = (value * 10U) + digit;
  }
  return value;
}

#if defined(CHRONOS_WAL_CRASH_CHILD_PATH) || defined(CHRONOS_TEST_CRASH_CHILD_PATH)

struct CrashChildOptions {
  std::filesystem::path directory;
  bool reopen{false};
  bool reclaim{false};
  std::uint64_t target_segment_size{kSegmentSizeLimit};
  std::size_t maximum_sync_batch_requests{64U};
  std::size_t maximum_sync_batch_encoded_bytes{kMaximumRecordLength};
  std::chrono::microseconds maximum_sync_batch_delay{1'000'000};
  // Clang 18 requires this explicit default for omitted designated-initializer fields.
  // NOLINTNEXTLINE(readability-redundant-member-init)
  std::string pause_after{};
  std::uint64_t pause_occurrence{1U};
  std::size_t short_record_prefix{};
};

class CrashChildProcess {
public:
  CrashChildProcess() noexcept = default;
  ~CrashChildProcess() {
    terminate_best_effort();
  }

  CrashChildProcess(const CrashChildProcess&) = delete;
  CrashChildProcess& operator=(const CrashChildProcess&) = delete;

  CrashChildProcess(CrashChildProcess&& other) noexcept {
    move_from(std::move(other));
  }

  CrashChildProcess& operator=(CrashChildProcess&& other) noexcept {
    if (this != &other) {
      terminate_best_effort();
      move_from(std::move(other));
    }
    return *this;
  }

  [[nodiscard]] static common::Result<CrashChildProcess> spawn(const CrashChildOptions& options) {
    if (options.directory.empty()) {
      return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                    "crash child requires a WAL directory"});
    }
    if (options.reopen && options.reclaim) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kInvalidArgument,
                         "crash child cannot request reopen and reclamation modes together"});
    }
    std::array<int, 2> commands{-1, -1};
    std::array<int, 2> events{-1, -1};
    if (::pipe(commands.data()) != 0) {
      return common::make_unexpected(errno_status("create crash-child command pipe", errno));
    }
    if (::pipe(events.data()) != 0) {
      const int error_number = errno;
      close_fd(commands[0]);
      close_fd(commands[1]);
      return common::make_unexpected(errno_status("create crash-child event pipe", error_number));
    }

    posix_spawn_file_actions_t actions{};
    int spawn_error = ::posix_spawn_file_actions_init(&actions);
    if (spawn_error == 0) {
      spawn_error = ::posix_spawn_file_actions_adddup2(&actions, commands[0], STDIN_FILENO);
    }
    if (spawn_error == 0) {
      spawn_error = ::posix_spawn_file_actions_adddup2(&actions, events[1], STDOUT_FILENO);
    }
    for (const int descriptor : {commands[0], commands[1], events[0], events[1]}) {
      if (spawn_error == 0 && descriptor != STDIN_FILENO && descriptor != STDOUT_FILENO) {
        spawn_error = ::posix_spawn_file_actions_addclose(&actions, descriptor);
      }
    }
    if (spawn_error != 0) {
      ::posix_spawn_file_actions_destroy(&actions);
      close_fd(commands[0]);
      close_fd(commands[1]);
      close_fd(events[0]);
      close_fd(events[1]);
      return common::make_unexpected(errno_status("configure crash-child process", spawn_error));
    }

    std::vector<std::string> arguments;
    arguments.reserve(19U);
#ifdef CHRONOS_TEST_CRASH_CHILD_PATH
    arguments.emplace_back(CHRONOS_TEST_CRASH_CHILD_PATH);
#else
    arguments.emplace_back(CHRONOS_WAL_CRASH_CHILD_PATH);
#endif
    append_argument(arguments, "--directory", options.directory.string());
    append_argument(arguments, "--mode",
                    options.reclaim ? "reclaim" : (options.reopen ? "reopen" : "create"));
    append_argument(arguments, "--target-segment-size",
                    std::to_string(options.target_segment_size));
    append_argument(arguments, "--max-batch-requests",
                    std::to_string(options.maximum_sync_batch_requests));
    append_argument(arguments, "--max-batch-bytes",
                    std::to_string(options.maximum_sync_batch_encoded_bytes));
    append_argument(arguments, "--max-delay-us",
                    std::to_string(options.maximum_sync_batch_delay.count()));
    if (!options.pause_after.empty()) {
      append_argument(arguments, "--pause-after", options.pause_after);
      append_argument(arguments, "--pause-occurrence", std::to_string(options.pause_occurrence));
    }
    if (options.short_record_prefix != 0U) {
      append_argument(arguments, "--short-record-prefix",
                      std::to_string(options.short_record_prefix));
    }
    std::vector<char*> argv;
    argv.reserve(arguments.size() + 1U);
    for (std::string& argument : arguments) {
      argv.push_back(argument.data());
    }
    argv.push_back(nullptr);

    pid_t process = -1;
    spawn_error =
        ::posix_spawn(&process, arguments.front().c_str(), &actions, nullptr, argv.data(), environ);
    ::posix_spawn_file_actions_destroy(&actions);
    close_fd(commands[0]);
    close_fd(events[1]);
    if (spawn_error != 0) {
      close_fd(commands[1]);
      close_fd(events[0]);
      return common::make_unexpected(errno_status("spawn WAL crash child", spawn_error));
    }

    CrashChildProcess child;
    child.process_ = process;
    child.command_descriptor_ = commands[1];
    child.event_descriptor_ = events[0];
    return child;
  }

  [[nodiscard]] pid_t process_id() const noexcept {
    return process_;
  }

  [[nodiscard]] common::Status send(std::string command) const {
    if (command_descriptor_ < 0) {
      return common::Status{common::StatusCode::kInvalidArgument,
                            "crash-child command pipe is closed"};
    }
    if (command.find('\n') != std::string::npos || command.size() > 4095U) {
      return common::Status{common::StatusCode::kInvalidArgument,
                            "crash-child command is not one bounded line"};
    }
    command.push_back('\n');
    std::size_t completed = 0;
    while (completed < command.size()) {
      const ssize_t count =
          ::write(command_descriptor_, command.data() + completed, command.size() - completed);
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        return errno_status("write crash-child command", errno);
      }
      if (count == 0) {
        return common::Status{common::StatusCode::kIoError,
                              "crash-child command pipe made no progress"};
      }
      completed += static_cast<std::size_t>(count);
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Result<CrashEvent>
  wait_for(const std::string_view name,
           const std::optional<std::uint64_t> request_id = std::nullopt,
           const std::chrono::milliseconds timeout = std::chrono::seconds{20}) {
    const auto matches = [name, request_id](const CrashEvent& event) {
      if (event.name != name) {
        return false;
      }
      if (!request_id.has_value()) {
        return true;
      }
      if (event.fields.empty()) {
        return false;
      }
      const common::Result<std::uint64_t> parsed = parse_protocol_u64(event.fields.front());
      return parsed.has_value() && *parsed == *request_id;
    };
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
      for (auto iterator = buffered_events_.begin(); iterator != buffered_events_.end();
           ++iterator) {
        if (matches(*iterator)) {
          CrashEvent event = std::move(*iterator);
          buffered_events_.erase(iterator);
          return event;
        }
      }
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kUnavailable,
                           "timed out waiting for crash-child event " + std::string{name}});
      }
      const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
      common::Result<CrashEvent> event = read_event(remaining);
      if (!event.has_value()) {
        return event;
      }
      if (matches(*event)) {
        return event;
      }
      buffered_events_.push_back(std::move(*event));
    }
  }

  [[nodiscard]] common::Status kill_abruptly() {
    if (process_ <= 0) {
      return common::Status{common::StatusCode::kInvalidArgument, "crash child is not running"};
    }
    if (::kill(process_, SIGKILL) != 0 && errno != ESRCH) {
      return errno_status("kill WAL crash child", errno);
    }
    const common::Result<int> status = wait_for_exit(std::chrono::seconds{20});
    if (!status.has_value()) {
      return status.error();
    }
    if (!WIFSIGNALED(*status) || WTERMSIG(*status) != SIGKILL) {
      return common::Status{common::StatusCode::kInternal,
                            "crash child did not terminate through SIGKILL"};
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Result<int>
  wait_for_exit(const std::chrono::milliseconds timeout = std::chrono::seconds{20}) {
    if (process_ <= 0) {
      return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                    "crash child has no active process"});
    }
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
      int status = 0;
      const pid_t waited = ::waitpid(process_, &status, WNOHANG);
      if (waited == process_) {
        process_ = -1;
        close_fd(command_descriptor_);
        close_fd(event_descriptor_);
        command_descriptor_ = -1;
        event_descriptor_ = -1;
        return status;
      }
      if (waited < 0 && errno != EINTR) {
        return common::make_unexpected(errno_status("wait for WAL crash child", errno));
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                      "timed out waiting for WAL crash child"});
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{2});
    }
  }

private:
  [[nodiscard]] common::Result<CrashEvent> read_event(const std::chrono::milliseconds timeout) {
    while (true) {
      const std::size_t newline = partial_event_.find('\n');
      if (newline != std::string::npos) {
        std::string line = partial_event_.substr(0U, newline);
        partial_event_.erase(0U, newline + 1U);
        return parse_crash_event(std::move(line));
      }
      pollfd descriptor{.fd = event_descriptor_, .events = POLLIN, .revents = 0};
      const auto bounded_timeout = std::clamp<std::int64_t>(timeout.count(), 0, INT_MAX);
      const int poll_result = ::poll(&descriptor, 1, static_cast<int>(bounded_timeout));
      if (poll_result == 0) {
        return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                      "timed out reading crash-child event"});
      }
      if (poll_result < 0) {
        if (errno == EINTR) {
          continue;
        }
        return common::make_unexpected(errno_status("poll crash-child event", errno));
      }
      std::array<char, 512> buffer{};
      const ssize_t count = ::read(event_descriptor_, buffer.data(), buffer.size());
      if (count == 0) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kIoError, "crash-child event pipe reached EOF"});
      }
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        return common::make_unexpected(errno_status("read crash-child event", errno));
      }
      partial_event_.append(buffer.data(), static_cast<std::size_t>(count));
      if (partial_event_.size() > 4096U) {
        return common::make_unexpected(common::Status{
            common::StatusCode::kOutOfRange, "crash-child protocol line exceeds 4096 bytes"});
      }
    }
  }

  static void append_argument(std::vector<std::string>& arguments, std::string key,
                              std::string value) {
    arguments.push_back(std::move(key));
    arguments.push_back(std::move(value));
  }

  [[nodiscard]] static common::Status errno_status(const std::string_view operation,
                                                   const int error_number) {
    return common::Status{common::StatusCode::kIoError, std::string{operation} +
                                                            " failed with errno " +
                                                            std::to_string(error_number)};
  }

  static void close_fd(const int descriptor) noexcept {
    if (descriptor >= 0) {
      static_cast<void>(::close(descriptor));
    }
  }

  void terminate_best_effort() noexcept {
    if (process_ > 0) {
      static_cast<void>(::kill(process_, SIGKILL));
      int status = 0;
      while (::waitpid(process_, &status, 0) < 0 && errno == EINTR) {
      }
    }
    close_fd(command_descriptor_);
    close_fd(event_descriptor_);
    process_ = -1;
    command_descriptor_ = -1;
    event_descriptor_ = -1;
  }

  void move_from(CrashChildProcess&& other) noexcept {
    process_ = std::exchange(other.process_, -1);
    command_descriptor_ = std::exchange(other.command_descriptor_, -1);
    event_descriptor_ = std::exchange(other.event_descriptor_, -1);
    partial_event_ = std::move(other.partial_event_);
    buffered_events_ = std::move(other.buffered_events_);
  }

  pid_t process_{-1};
  int command_descriptor_{-1};
  int event_descriptor_{-1};
  std::string partial_event_;
  std::vector<CrashEvent> buffered_events_;
};

#endif // defined(CHRONOS_WAL_CRASH_CHILD_PATH) || defined(CHRONOS_TEST_CRASH_CHILD_PATH)

} // namespace chronos::wal::test

#endif // CHRONOS_TESTS_WAL_WAL_CRASH_PROTOCOL_HPP_
