#ifndef CHRONOS_TESTS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_FAULT_HPP_
#define CHRONOS_TESTS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_FAULT_HPP_

#include "io/posix_syscalls.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <sys/stat.h>
#include <vector>

namespace chronos::ingest::test {

enum class SnapshotStorageFault : std::uint8_t {
  kPriorTemporaryUnlink,
  kTemporaryCreate,
  kTemporaryValidationStat,
  kTemporaryWrite,
  kTemporaryPartialWrite,
  kTemporarySizeStat,
  kTemporaryReadback,
  kTemporaryFileSync,
  kTemporaryClose,
  kFinalRename,
  kFinalDirectorySync,
};

class OneShotSnapshotStorageFault final : public io::detail::PosixSyscalls {
public:
  explicit OneShotSnapshotStorageFault(const SnapshotStorageFault fault)
      : delegate_(io::detail::system_posix_syscalls()), fault_(fault) {}

  void arm() noexcept {
    armed_ = true;
  }

  [[nodiscard]] bool fired() const noexcept {
    return fired_;
  }

  int open_directory(const char* path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }
  int open_at(const io::detail::OpenAtRequest& request) override {
    if (armed_ && fault_ == SnapshotStorageFault::kTemporaryCreate &&
        std::string_view{request.name}.ends_with(".rtas.tmp")) {
      return fail();
    }
    return delegate_.open_at(request);
  }
  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }
  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    if (armed_ && fault_ == SnapshotStorageFault::kTemporaryReadback)
      return fail_ssize();
    return delegate_.pread(request);
  }
  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    if (armed_ && fault_ == SnapshotStorageFault::kTemporaryWrite)
      return fail_ssize();
    if (armed_ && fault_ == SnapshotStorageFault::kTemporaryPartialWrite) {
      if (!partial_write_started_) {
        partial_write_started_ = true;
        constexpr std::size_t kPrefixSize = 16U;
        const io::detail::WriteAtRequest prefix{request.descriptor, request.source, kPrefixSize,
                                                request.offset};
        return delegate_.pwrite(prefix);
      }
      return fail_ssize();
    }
    return delegate_.pwrite(request);
  }
  int fstat(const int descriptor, struct stat* metadata) override {
    const int result = delegate_.fstat(descriptor, metadata);
    if (!armed_ || result != 0 || !S_ISREG(metadata->st_mode))
      return result;
    ++regular_stat_calls_;
    if ((fault_ == SnapshotStorageFault::kTemporaryValidationStat && regular_stat_calls_ == 1U) ||
        (fault_ == SnapshotStorageFault::kTemporarySizeStat && regular_stat_calls_ == 2U)) {
      return fail();
    }
    return result;
  }
  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }
  int fdatasync(const int descriptor) override {
    return delegate_.fdatasync(descriptor);
  }
  int fsync(const int descriptor) override {
    if (armed_ && (fault_ == SnapshotStorageFault::kTemporaryFileSync ||
                   fault_ == SnapshotStorageFault::kFinalDirectorySync)) {
      struct stat metadata {};
      if (delegate_.fstat(descriptor, &metadata) == 0 &&
          ((fault_ == SnapshotStorageFault::kTemporaryFileSync && S_ISREG(metadata.st_mode)) ||
           (fault_ == SnapshotStorageFault::kFinalDirectorySync && S_ISDIR(metadata.st_mode)))) {
        return fail();
      }
    }
    return delegate_.fsync(descriptor);
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    if (armed_ && fault_ == SnapshotStorageFault::kFinalRename)
      return fail();
    return delegate_.rename_no_replace(request);
  }
  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }
  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }
  int unlink_at(const int directory_descriptor, const char* name) override {
    if (armed_ && fault_ == SnapshotStorageFault::kPriorTemporaryUnlink &&
        std::string_view{name}.ends_with(".rtas.tmp")) {
      return fail();
    }
    return delegate_.unlink_at(directory_descriptor, name);
  }
  int close(const int descriptor) override {
    const int result = delegate_.close(descriptor);
    if (armed_ && result == 0 && fault_ == SnapshotStorageFault::kTemporaryClose)
      return fail();
    return result;
  }

private:
  int fail() noexcept {
    armed_ = false;
    fired_ = true;
    errno = EIO;
    return -1;
  }

  ssize_t fail_ssize() noexcept {
    static_cast<void>(fail());
    return -1;
  }

  io::detail::PosixSyscalls& delegate_;
  SnapshotStorageFault fault_;
  std::size_t regular_stat_calls_{};
  bool partial_write_started_{false};
  bool armed_{false};
  bool fired_{false};
};

} // namespace chronos::ingest::test

#endif // CHRONOS_TESTS_INGEST_RAFT_TABLET_SNAPSHOT_STORAGE_FAULT_HPP_
