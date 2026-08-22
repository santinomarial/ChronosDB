#include "chronos/raft/persistent_log.hpp"
#include "io/posix_syscalls.hpp"
#include "raft/persistent_log_crash_protocol.hpp"
#include "raft/persistent_log_internal.hpp"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft::test {
namespace {

enum class CrashOperation : std::uint8_t {
  kInitialization,
  kRotation,
  kCheckpoint,
};

struct ChildConfig {
  std::string directory;
  std::string pause_after;
  std::uint64_t target_segment_size{300U};
  CrashOperation operation{CrashOperation::kRotation};
  bool valid_mode{false};
};

[[nodiscard]] ChildConfig parse_arguments(const int count, char** const values) {
  ChildConfig config;
  for (int index = 1; index + 1 < count; index += 2) {
    const std::string_view key{values[index]};
    const std::string_view value{values[index + 1]};
    if (key == "--directory") {
      config.directory = value;
    } else if (key == "--pause-after") {
      config.pause_after = value;
    } else if (key == "--mode") {
      if (value == "create") {
        config.operation = CrashOperation::kRotation;
        config.valid_mode = true;
      } else if (value == "reopen") {
        config.operation = CrashOperation::kInitialization;
        config.valid_mode = true;
      } else if (value == "reclaim") {
        config.operation = CrashOperation::kCheckpoint;
        config.valid_mode = true;
      }
    } else if (key == "--target-segment-size") {
      const char* const begin = value.data();
      const char* const end = begin + value.size();
      const auto parsed = std::from_chars(begin, end, config.target_segment_size);
      if (parsed.ec != std::errc{} || parsed.ptr != end) {
        config.target_segment_size = 0U;
      }
    }
  }
  return config;
}

[[nodiscard]] GroupId group_id() {
  common::Uuid::Bytes bytes{};
  bytes.fill(std::byte{0xE1U});
  return GroupId{bytes};
}

[[nodiscard]] GroupPersistentState state(const std::uint64_t sequence, const std::byte value) {
  PersistentState persistent{};
  persistent.current_term = 1U;
  persistent.log.push_back(LogEntry{1U, 1U, 1U, {value}});
  persistent.commit_index = 1U;
  return GroupPersistentState{group_id(), sequence, std::move(persistent)};
}

class ObservingSyscalls final : public io::detail::PosixSyscalls {
public:
  ObservingSyscalls(io::detail::PosixSyscalls& delegate, std::string pause_after)
      : delegate_(delegate), pause_after_(std::move(pause_after)) {}

  void begin(const CrashOperation operation) noexcept {
    operation_ = operation;
    active_ = true;
    writes_ = 0U;
    opens_ = 0U;
    data_syncs_ = 0U;
    regular_syncs_ = 0U;
    directory_syncs_ = 0U;
    renames_ = 0U;
    closes_ = 0U;
    unlinks_ = 0U;
  }

  int open_directory(const char* path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }
  int open_at(const io::detail::OpenAtRequest& request) override {
    const int result = delegate_.open_at(request);
    if (active_ && result >= 0 && operation_ == CrashOperation::kInitialization && opens_++ == 0U)
      observe(kAfterInitialLockCreate);
    return result;
  }
  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }
  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }
  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    const ssize_t result = delegate_.pwrite(request);
    if (active_ && result >= 0 && static_cast<std::size_t>(result) == request.size) {
      const std::uint64_t occurrence = writes_++;
      if (occurrence == 0U) {
        observe(operation_ == CrashOperation::kInitialization ? kAfterInitialHeaderWrite
                                                              : kAfterSuccessorHeaderWrite);
      } else if (occurrence == 1U) {
        observe(operation_ == CrashOperation::kRotation ? kAfterRotatedRecordWrite
                                                        : kAfterCheckpointRecordWrite);
      } else if (operation_ == CrashOperation::kCheckpoint && occurrence == 2U) {
        observe(kAfterAnchorWrite);
      }
    }
    return result;
  }
  int fstat(const int descriptor, struct stat* metadata) override {
    return delegate_.fstat(descriptor, metadata);
  }
  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }
  int fdatasync(const int descriptor) override {
    const int result = delegate_.fdatasync(descriptor);
    if (active_ && result == 0) {
      const std::uint64_t occurrence = data_syncs_++;
      if (occurrence == 0U) {
        observe(kAfterPredecessorDataSync);
      } else if (occurrence == 1U) {
        observe(operation_ == CrashOperation::kRotation ? kAfterRotatedRecordDataSync
                                                        : kAfterCheckpointDataSync);
      }
    }
    return result;
  }
  int fsync(const int descriptor) override {
    const int result = delegate_.fsync(descriptor);
    if (!active_ || result != 0)
      return result;
    struct stat metadata {};
    if (delegate_.fstat(descriptor, &metadata) != 0)
      return result;
    if (S_ISREG(metadata.st_mode)) {
      const std::uint64_t occurrence = regular_syncs_++;
      if (occurrence == 0U) {
        observe(operation_ == CrashOperation::kInitialization ? kAfterInitialFileSync
                                                              : kAfterSuccessorFileSync);
      } else if (operation_ == CrashOperation::kCheckpoint && occurrence == 1U) {
        observe(kAfterAnchorFileSync);
      }
    } else if (S_ISDIR(metadata.st_mode)) {
      const std::uint64_t occurrence = directory_syncs_++;
      if (operation_ == CrashOperation::kInitialization && occurrence == 0U) {
        observe(kAfterInitialLockDirectorySync);
      } else if (operation_ == CrashOperation::kInitialization && occurrence == 1U) {
        observe(kAfterInitialDirectorySync);
      } else if (occurrence == 0U) {
        observe(kAfterSuccessorDirectorySync);
      } else if (operation_ == CrashOperation::kCheckpoint && occurrence == 1U) {
        observe(kAfterAnchorDirectorySync);
      } else if (operation_ == CrashOperation::kCheckpoint && occurrence == 2U) {
        observe(kAfterObsoleteSegmentDirectorySync);
      } else if (operation_ == CrashOperation::kCheckpoint && occurrence == 3U) {
        observe(kAfterObsoleteAnchorDirectorySync);
      }
    }
    return result;
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    const int result = delegate_.rename_no_replace(request);
    if (active_ && result == 0) {
      const std::uint64_t occurrence = renames_++;
      if (occurrence == 0U) {
        observe(operation_ == CrashOperation::kInitialization ? kAfterInitialRename
                                                              : kAfterSuccessorRename);
      } else if (operation_ == CrashOperation::kCheckpoint && occurrence == 1U) {
        observe(kAfterAnchorRename);
      }
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
  int unlink_at(const int directory_descriptor, const char* name) override {
    const int result = delegate_.unlink_at(directory_descriptor, name);
    if (active_ && result == 0 && operation_ == CrashOperation::kCheckpoint) {
      const std::uint64_t occurrence = unlinks_++;
      observe(occurrence == 0U ? kAfterObsoleteSegmentUnlink : kAfterObsoleteAnchorUnlink);
    }
    return result;
  }
  int close(const int descriptor) override {
    const int result = delegate_.close(descriptor);
    if (active_ && result == 0) {
      const std::uint64_t occurrence = closes_++;
      if (occurrence == 0U) {
        observe(kAfterPredecessorClose);
      } else if (operation_ == CrashOperation::kCheckpoint && occurrence == 1U) {
        observe(kAfterAnchorClose);
      }
    }
    return result;
  }

private:
  void observe(const std::string_view point) const {
    if (point != pause_after_)
      return;
    std::cout << "FAILPOINT " << point << '\n' << std::flush;
    for (;;) {
      static_cast<void>(::pause());
    }
  }

  io::detail::PosixSyscalls& delegate_;
  std::string pause_after_;
  CrashOperation operation_{CrashOperation::kRotation};
  bool active_{false};
  std::uint64_t opens_{};
  std::uint64_t writes_{};
  std::uint64_t data_syncs_{};
  std::uint64_t regular_syncs_{};
  std::uint64_t directory_syncs_{};
  std::uint64_t renames_{};
  std::uint64_t closes_{};
  std::uint64_t unlinks_{};
};

[[nodiscard]] int run(const ChildConfig& config) {
  if (config.directory.empty() || config.pause_after.empty() || !config.valid_mode ||
      config.target_segment_size == 0U) {
    return 2;
  }
  ObservingSyscalls syscalls{io::detail::system_posix_syscalls(), config.pause_after};
  const RaftPersistentLogConfig log_config{.directory_path = config.directory,
                                           .target_segment_size = config.target_segment_size};
  if (config.operation == CrashOperation::kInitialization)
    syscalls.begin(config.operation);
  auto created = detail::RaftPersistentLogTestAccess::create_new(log_config, syscalls);
  if (!created.has_value())
    return 3;
  RaftPersistentLog log = std::move(*created);
  if (config.operation == CrashOperation::kInitialization)
    return 8;
  if (!log.append(state(1U, std::byte{0xC1U})).has_value() || !log.synchronize().has_value()) {
    return 4;
  }
  if (config.operation == CrashOperation::kCheckpoint) {
    auto first = log.checkpoint_and_reclaim({state(2U, std::byte{0xC2U})});
    if (!first.has_value() || first->base_segment_number != 2U)
      return 5;
  }

  syscalls.begin(config.operation);
  if (config.operation == CrashOperation::kRotation) {
    if (!log.append(state(2U, std::byte{0xC2U})).has_value() || !log.synchronize().has_value()) {
      return 6;
    }
  } else {
    auto second = log.checkpoint_and_reclaim({state(3U, std::byte{0xC3U})});
    if (!second.has_value() || second->base_segment_number != 3U)
      return 7;
  }
  return 8;
}

} // namespace
} // namespace chronos::raft::test

int main(const int argc, char** const argv) {
  try {
    return chronos::raft::test::run(chronos::raft::test::parse_arguments(argc, argv));
  } catch (const std::exception&) {
    return 70;
  } catch (...) {
    return 71;
  }
}
