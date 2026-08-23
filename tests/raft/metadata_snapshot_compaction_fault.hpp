#ifndef CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_COMPACTION_FAULT_HPP_
#define CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_COMPACTION_FAULT_HPP_

#include "io/posix_syscalls.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <sys/stat.h>
#include <vector>

namespace chronos::raft::test {

inline constexpr std::size_t kMetadataCompactionPartialRecordBytes = 16U;

enum class MetadataCompactionApplicationFault : std::uint8_t {
  kPriorTemporaryUnlink,
  kTemporaryPartialWrite,
  kFinalDirectorySync,
};

class MetadataCompactionApplicationFaultSyscalls final : public io::detail::PosixSyscalls {
public:
  explicit MetadataCompactionApplicationFaultSyscalls(
      const MetadataCompactionApplicationFault fault)
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
    return delegate_.open_at(request);
  }

  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }

  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    if (armed_ && fault_ == MetadataCompactionApplicationFault::kTemporaryPartialWrite) {
      if (!partial_write_started_) {
        partial_write_started_ = true;
        const io::detail::WriteAtRequest prefix{request.descriptor, request.source,
                                                kMetadataCompactionPartialRecordBytes,
                                                request.offset};
        return delegate_.pwrite(prefix);
      }
      return fail_ssize();
    }
    return delegate_.pwrite(request);
  }

  int fstat(const int descriptor, struct stat* metadata) override {
    return delegate_.fstat(descriptor, metadata);
  }

  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }

  int fdatasync(const int descriptor) override {
    return delegate_.fdatasync(descriptor);
  }

  int fsync(const int descriptor) override {
    if (armed_ && fault_ == MetadataCompactionApplicationFault::kFinalDirectorySync) {
      struct stat metadata {};
      if (delegate_.fstat(descriptor, &metadata) == 0 && S_ISDIR(metadata.st_mode))
        return fail();
    }
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

  int unlink_at(const int directory_descriptor, const char* name) override {
    if (armed_ && fault_ == MetadataCompactionApplicationFault::kPriorTemporaryUnlink &&
        std::string_view{name}.ends_with(".rmas.tmp")) {
      return fail();
    }
    return delegate_.unlink_at(directory_descriptor, name);
  }

  int close(const int descriptor) override {
    return delegate_.close(descriptor);
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
  MetadataCompactionApplicationFault fault_;
  bool partial_write_started_{false};
  bool armed_{false};
  bool fired_{false};
};

enum class MetadataCompactionRaftFault : std::uint8_t {
  kWriteBefore,
  kWritePrefixThenError,
  kWriteAfter,
  kDataSyncBefore,
  kDataSyncAfter,
};

class MetadataCompactionRaftFaultSyscalls final : public io::detail::PosixSyscalls {
public:
  explicit MetadataCompactionRaftFaultSyscalls(const MetadataCompactionRaftFault fault)
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
    return delegate_.open_at(request);
  }

  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }

  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    if (armed_ && fault_ == MetadataCompactionRaftFault::kWriteBefore)
      return fail_ssize();
    if (armed_ && fault_ == MetadataCompactionRaftFault::kWritePrefixThenError) {
      if (!partial_write_started_) {
        partial_write_started_ = true;
        const io::detail::WriteAtRequest prefix{request.descriptor, request.source,
                                                kMetadataCompactionPartialRecordBytes,
                                                request.offset};
        return delegate_.pwrite(prefix);
      }
      return fail_ssize();
    }
    const ssize_t result = delegate_.pwrite(request);
    if (armed_ && fault_ == MetadataCompactionRaftFault::kWriteAfter && result >= 0 &&
        static_cast<std::size_t>(result) == request.size) {
      return fail_ssize();
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
    if (armed_ && fault_ == MetadataCompactionRaftFault::kDataSyncBefore)
      return fail();
    const int result = delegate_.fdatasync(descriptor);
    if (armed_ && fault_ == MetadataCompactionRaftFault::kDataSyncAfter && result == 0)
      return fail();
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

  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }

  int unlink_at(const int directory_descriptor, const char* name) override {
    return delegate_.unlink_at(directory_descriptor, name);
  }

  int close(const int descriptor) override {
    return delegate_.close(descriptor);
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
  MetadataCompactionRaftFault fault_;
  bool partial_write_started_{false};
  bool armed_{false};
  bool fired_{false};
};

} // namespace chronos::raft::test

#endif // CHRONOS_TESTS_RAFT_METADATA_SNAPSHOT_COMPACTION_FAULT_HPP_
