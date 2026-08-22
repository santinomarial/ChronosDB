#ifndef CHRONOS_TESTS_RAFT_RAFT_TEST_POSIX_HPP_
#define CHRONOS_TESTS_RAFT_RAFT_TEST_POSIX_HPP_

#include "io/posix_syscalls.hpp"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::raft::test {

enum class AmbiguousDurableIoFault : std::uint8_t {
  kNone,
  kRecordWrite,
  kDataSync,
  kFileClose,
};

// Performs the selected real durable-file operation and then reports EIO once. This models an
// ambiguous storage result: the bytes or synchronization may have completed even though the owner
// must not release a transition. Arm only after construction so segment installation is unaffected.
class AmbiguousDurableIoFaultPosixSyscalls final : public io::detail::PosixSyscalls {
public:
  AmbiguousDurableIoFaultPosixSyscalls() noexcept
      : delegate_(io::detail::system_posix_syscalls()) {}

  void arm(const AmbiguousDurableIoFault fault) noexcept {
    armed_.store(fault);
  }

  int open_directory(const char* const path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }
  int open_at(const io::detail::OpenAtRequest& request) override {
    return delegate_.open_at(request);
  }
  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }
  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }
  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    const ssize_t result = delegate_.pwrite(request);
    if (result >= 0 && consume(AmbiguousDurableIoFault::kRecordWrite)) {
      errno = EIO;
      return -1;
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
    if (result == 0 && consume(AmbiguousDurableIoFault::kDataSync)) {
      errno = EIO;
      return -1;
    }
    return result;
  }
  int fsync(const int descriptor) override {
    return delegate_.fsync(descriptor);
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    return delegate_.rename_no_replace(request);
  }
  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }
  int list_directory_entries(int descriptor, std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }
  int unlink_at(const int directory_descriptor, const char* const name) override {
    return delegate_.unlink_at(directory_descriptor, name);
  }
  int close(const int descriptor) override {
    const int result = delegate_.close(descriptor);
    if (result == 0 && consume(AmbiguousDurableIoFault::kFileClose)) {
      errno = EIO;
      return -1;
    }
    return result;
  }

  [[nodiscard]] std::size_t injected_faults() const noexcept {
    return injected_faults_.load();
  }

private:
  [[nodiscard]] bool consume(const AmbiguousDurableIoFault expected) noexcept {
    AmbiguousDurableIoFault armed = expected;
    if (!armed_.compare_exchange_strong(armed, AmbiguousDurableIoFault::kNone))
      return false;
    injected_faults_.fetch_add(1U);
    return true;
  }

  io::detail::PosixSyscalls& delegate_;
  std::atomic<AmbiguousDurableIoFault> armed_;
  std::atomic<std::size_t> injected_faults_;
};

// Delegates every operation to the real POSIX adapter, then reports EIO after selected close calls.
// This models the ambiguous close boundary while still releasing the real descriptor and lock.
class CloseFaultPosixSyscalls final : public io::detail::PosixSyscalls {
public:
  explicit CloseFaultPosixSyscalls(const std::uint8_t failure_mask) noexcept
      : delegate_(io::detail::system_posix_syscalls()), failure_mask_(failure_mask) {}

  int open_directory(const char* const path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }
  int open_at(const io::detail::OpenAtRequest& request) override {
    return delegate_.open_at(request);
  }
  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }
  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }
  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    return delegate_.pwrite(request);
  }
  int fstat(const int descriptor, struct stat* const metadata) override {
    return delegate_.fstat(descriptor, metadata);
  }
  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }
  int fdatasync(const int descriptor) override {
    return delegate_.fdatasync(descriptor);
  }
  int fsync(const int descriptor) override {
    return delegate_.fsync(descriptor);
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    return delegate_.rename_no_replace(request);
  }
  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }
  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }
  int unlink_at(const int directory_descriptor, const char* const name) override {
    return delegate_.unlink_at(directory_descriptor, name);
  }
  int close(const int descriptor) override {
    ++close_calls_;
    const int result = delegate_.close(descriptor);
    if (close_calls_ <= 3U && (failure_mask_ & (std::uint8_t{1U} << (close_calls_ - 1U))) != 0U) {
      errno = EIO;
      return -1;
    }
    return result;
  }

  [[nodiscard]] std::size_t close_calls() const noexcept {
    return close_calls_;
  }

private:
  io::detail::PosixSyscalls& delegate_;
  std::uint8_t failure_mask_{};
  std::size_t close_calls_{};
};

} // namespace chronos::raft::test

#endif // CHRONOS_TESTS_RAFT_RAFT_TEST_POSIX_HPP_
