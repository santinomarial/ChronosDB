#ifndef CHRONOS_TESTS_WAL_WAL_WRITER_TEST_SUPPORT_HPP_
#define CHRONOS_TESTS_WAL_WAL_WRITER_TEST_SUPPORT_HPP_

#include "chronos/common/status.hpp"
#include "chronos/wal/types.hpp"
#include "chronos/wal/wal_log_id_generator.hpp"
#include "io/posix_syscalls.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace chronos::wal::test {

struct SyscallOutcome {
  std::int64_t result{};
  int error_number{};
};

[[nodiscard]] inline SyscallOutcome pop_or(std::deque<SyscallOutcome>& outcomes,
                                           const std::int64_t fallback) {
  if (outcomes.empty()) {
    return SyscallOutcome{.result = fallback, .error_number = 0};
  }
  const SyscallOutcome outcome = outcomes.front();
  outcomes.pop_front();
  return outcome;
}

class ScriptedWalSyscalls final : public io::detail::PosixSyscalls {
public:
  int open_directory(const char* path, int flags) override {
    static_cast<void>(flags);
    events.emplace_back(std::string{"open_directory:"} + path);
    return integer_result(pop_or(open_directory_outcomes, directory_descriptor));
  }

  int open_at(const io::detail::OpenAtRequest& request) override {
    events.emplace_back(std::string{"open_at:"} + request.name);
    const SyscallOutcome outcome = pop_or(open_at_outcomes, next_descriptor++);
    return integer_result(outcome);
  }

  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    events.emplace_back(std::string{"mkdir_at:"} + request.name);
    return integer_result(pop_or(mkdir_outcomes, 0));
  }

  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    events.emplace_back("pread:" + std::to_string(request.descriptor));
    return signed_result(pop_or(pread_outcomes, 0));
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    events.emplace_back("pwrite:" + std::to_string(request.descriptor) + "@" +
                        std::to_string(request.offset));
    const SyscallOutcome outcome = pop_or(pwrite_outcomes, static_cast<std::int64_t>(request.size));
    if (outcome.result > 0) {
      const off_t end = request.offset + static_cast<off_t>(outcome.result);
      file_sizes[request.descriptor] = std::max(file_sizes[request.descriptor], end);
    }
    return signed_result(outcome);
  }

  int fstat(const int descriptor, struct stat* metadata) override {
    events.emplace_back("fstat:" + std::to_string(descriptor));
    const SyscallOutcome outcome = pop_or(fstat_outcomes, 0);
    if (outcome.result == 0) {
      *metadata = {};
      if (descriptor == directory_descriptor) {
        metadata->st_mode = S_IFDIR | 0700;
      } else {
        metadata->st_mode = S_IFREG | 0600;
        metadata->st_size = file_sizes[descriptor];
      }
    }
    return integer_result(outcome);
  }

  int ftruncate(const io::detail::TruncateRequest& request) override {
    events.emplace_back("ftruncate:" + std::to_string(request.descriptor));
    const SyscallOutcome outcome = pop_or(ftruncate_outcomes, 0);
    if (outcome.result == 0) {
      file_sizes[request.descriptor] = request.size;
    }
    return integer_result(outcome);
  }

  int fdatasync(const int descriptor) override {
    events.emplace_back("fdatasync:" + std::to_string(descriptor));
    if (fdatasync_hook) {
      fdatasync_hook();
    }
    return integer_result(pop_or(fdatasync_outcomes, 0));
  }

  int fsync(const int descriptor) override {
    events.emplace_back("fsync:" + std::to_string(descriptor));
    return integer_result(pop_or(fsync_outcomes, 0));
  }

  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    events.emplace_back(std::string{"rename:"} + request.old_name + "->" + request.new_name);
    return integer_result(pop_or(rename_outcomes, 0));
  }

  int try_lock_exclusive(const int descriptor) override {
    events.emplace_back("lock:" + std::to_string(descriptor));
    return integer_result(pop_or(lock_outcomes, 0));
  }

  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    events.emplace_back("list_directory:" + std::to_string(descriptor));
    const SyscallOutcome outcome = pop_or(list_directory_outcomes, 0);
    if (outcome.result == 0) {
      if (!directory_entry_snapshots.empty()) {
        entries = std::move(directory_entry_snapshots.front());
        directory_entry_snapshots.pop_front();
      } else {
        entries = directory_entries;
      }
    }
    return integer_result(outcome);
  }

  int unlink_at(const int parent_descriptor, const char* const name) override {
    events.emplace_back("unlink_at:" + std::to_string(parent_descriptor) + ":" + name);
    return integer_result(pop_or(unlink_outcomes, 0));
  }

  int close(const int descriptor) override {
    events.emplace_back("close:" + std::to_string(descriptor));
    return integer_result(pop_or(close_outcomes, 0));
  }

  std::deque<SyscallOutcome> open_directory_outcomes;
  std::deque<SyscallOutcome> open_at_outcomes;
  std::deque<SyscallOutcome> mkdir_outcomes;
  std::deque<SyscallOutcome> pread_outcomes;
  std::deque<SyscallOutcome> pwrite_outcomes;
  std::deque<SyscallOutcome> fstat_outcomes;
  std::deque<SyscallOutcome> ftruncate_outcomes;
  std::deque<SyscallOutcome> fdatasync_outcomes;
  std::deque<SyscallOutcome> fsync_outcomes;
  std::deque<SyscallOutcome> rename_outcomes;
  std::deque<SyscallOutcome> lock_outcomes;
  std::deque<SyscallOutcome> list_directory_outcomes;
  std::deque<SyscallOutcome> unlink_outcomes;
  std::deque<SyscallOutcome> close_outcomes;
  std::function<void()> fdatasync_hook;
  std::vector<io::DirectoryEntry> directory_entries;
  std::deque<std::vector<io::DirectoryEntry>> directory_entry_snapshots;
  std::vector<std::string> events;
  std::unordered_map<int, off_t> file_sizes;
  int directory_descriptor{10};
  int next_descriptor{11};

private:
  static void set_errno(const SyscallOutcome& outcome) noexcept {
    if (outcome.result == -1) {
      errno = outcome.error_number;
    }
  }

  static int integer_result(const SyscallOutcome& outcome) noexcept {
    set_errno(outcome);
    return static_cast<int>(outcome.result);
  }

  static ssize_t signed_result(const SyscallOutcome& outcome) noexcept {
    set_errno(outcome);
    return static_cast<ssize_t>(outcome.result);
  }
};

[[nodiscard]] inline WalId make_wal_id(const std::uint8_t first_byte = 1U) {
  WalId id;
  id.bytes[0] = static_cast<std::byte>(first_byte);
  for (std::size_t index = 1; index < id.bytes.size(); ++index) {
    id.bytes[index] = static_cast<std::byte>(index);
  }
  return id;
}

class FixedWalIdGenerator final : public WalLogIdGenerator {
public:
  explicit FixedWalIdGenerator(WalId id) : id_(id) {}
  explicit FixedWalIdGenerator(common::Status failure) : failure_(std::move(failure)) {}

  [[nodiscard]] common::Result<WalId> generate() override {
    ++calls;
    if (!failure_.is_ok()) {
      return common::make_unexpected(failure_);
    }
    return id_;
  }

  std::size_t calls{};

private:
  WalId id_{};
  common::Status failure_;
};

[[nodiscard]] inline std::vector<std::byte>
make_application_payload(const std::size_t size = kApplicationEnvelopeSize) {
  std::vector<std::byte> payload(size);
  if (size >= kApplicationEnvelopeSize) {
    payload[0] = std::byte{1};
    payload[4] = std::byte{1};
  }
  return payload;
}

class TemporaryDirectory {
public:
  explicit TemporaryDirectory(std::string_view prefix) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / (std::string{prefix} + "-XXXXXX")).string();
    char* const created = ::mkdtemp(pattern.data());
    if (created != nullptr) {
      path_ = created;
    }
  }

  ~TemporaryDirectory() {
    if (!path_.empty()) {
      std::error_code ignored;
      std::filesystem::remove_all(path_, ignored);
    }
  }

  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return !path_.empty();
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

} // namespace chronos::wal::test

#endif // CHRONOS_TESTS_WAL_WAL_WRITER_TEST_SUPPORT_HPP_
