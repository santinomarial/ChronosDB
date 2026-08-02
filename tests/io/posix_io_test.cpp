#include "chronos/io/posix_io.hpp"
#include "io/posix_syscalls.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::io {
namespace {

struct Outcome {
  std::int64_t result{};
  int error_number{};
};

[[nodiscard]] Outcome pop_or(std::deque<Outcome>& outcomes, const std::int64_t fallback) {
  if (outcomes.empty()) {
    return Outcome{.result = fallback, .error_number = 0};
  }
  const Outcome outcome = outcomes.front();
  outcomes.pop_front();
  return outcome;
}

class ScriptedSyscalls final : public detail::PosixSyscalls {
public:
  int open_directory(const char* const path, const int flags) override {
    opened_paths.emplace_back(path);
    open_directory_flags.push_back(flags);
    return integer_result(pop_or(open_directory_outcomes, next_descriptor++));
  }

  int open_at(const detail::OpenAtRequest& request) override {
    open_at_directories.push_back(request.directory_descriptor);
    open_at_names.emplace_back(request.name);
    open_at_flags.push_back(request.flags);
    open_at_permissions.push_back(request.permissions);
    return integer_result(pop_or(open_at_outcomes, next_descriptor++));
  }

  ssize_t pread(const detail::ReadAtRequest& request) override {
    pread_descriptors.push_back(request.descriptor);
    pread_sizes.push_back(request.size);
    pread_offsets.push_back(request.offset);
    const Outcome outcome = pop_or(pread_outcomes, 0);
    if (outcome.result > 0) {
      const auto transferred = static_cast<std::size_t>(outcome.result);
      auto* const bytes = static_cast<std::byte*>(request.destination);
      for (std::size_t index = 0; index < transferred; ++index) {
        bytes[index] = static_cast<std::byte>(next_read_byte++);
      }
    }
    set_errno(outcome);
    return static_cast<ssize_t>(outcome.result);
  }

  ssize_t pwrite(const detail::WriteAtRequest& request) override {
    pwrite_descriptors.push_back(request.descriptor);
    pwrite_sizes.push_back(request.size);
    pwrite_offsets.push_back(request.offset);
    const auto* const bytes = static_cast<const std::byte*>(request.source);
    if (request.size != 0U) {
      pwrite_first_bytes.push_back(bytes[0]);
    }
    const Outcome outcome = pop_or(pwrite_outcomes, static_cast<std::int64_t>(request.size));
    set_errno(outcome);
    return static_cast<ssize_t>(outcome.result);
  }

  int fstat(const int descriptor, struct stat* const metadata) override {
    fstat_descriptors.push_back(descriptor);
    const Outcome outcome = pop_or(fstat_outcomes, 0);
    if (outcome.result == 0) {
      *metadata = {};
      metadata->st_mode = metadata_mode;
      metadata->st_size = metadata_size;
    }
    return integer_result(outcome);
  }

  int ftruncate(const detail::TruncateRequest& request) override {
    ftruncate_descriptors.push_back(request.descriptor);
    ftruncate_sizes.push_back(request.size);
    return integer_result(pop_or(ftruncate_outcomes, 0));
  }

  int fdatasync(const int descriptor) override {
    fdatasync_descriptors.push_back(descriptor);
    return integer_result(pop_or(fdatasync_outcomes, 0));
  }

  int fsync(const int descriptor) override {
    fsync_descriptors.push_back(descriptor);
    return integer_result(pop_or(fsync_outcomes, 0));
  }

  int rename_no_replace(const detail::RenameAtRequest& request) override {
    rename_directories.push_back(request.directory_descriptor);
    rename_old_names.emplace_back(request.old_name);
    rename_new_names.emplace_back(request.new_name);
    return integer_result(pop_or(rename_outcomes, 0));
  }

  int try_lock_exclusive(const int descriptor) override {
    lock_descriptors.push_back(descriptor);
    return integer_result(pop_or(lock_outcomes, 0));
  }

  int close(const int descriptor) override {
    close_descriptors.push_back(descriptor);
    return integer_result(pop_or(close_outcomes, 0));
  }

  std::deque<Outcome> open_directory_outcomes;
  std::deque<Outcome> open_at_outcomes;
  std::deque<Outcome> pread_outcomes;
  std::deque<Outcome> pwrite_outcomes;
  std::deque<Outcome> fstat_outcomes;
  std::deque<Outcome> ftruncate_outcomes;
  std::deque<Outcome> fdatasync_outcomes;
  std::deque<Outcome> fsync_outcomes;
  std::deque<Outcome> rename_outcomes;
  std::deque<Outcome> lock_outcomes;
  std::deque<Outcome> close_outcomes;

  std::vector<std::string> opened_paths;
  std::vector<int> open_directory_flags;
  std::vector<int> open_at_directories;
  std::vector<std::string> open_at_names;
  std::vector<int> open_at_flags;
  std::vector<mode_t> open_at_permissions;
  std::vector<int> pread_descriptors;
  std::vector<std::size_t> pread_sizes;
  std::vector<off_t> pread_offsets;
  std::vector<int> pwrite_descriptors;
  std::vector<std::size_t> pwrite_sizes;
  std::vector<off_t> pwrite_offsets;
  std::vector<std::byte> pwrite_first_bytes;
  std::vector<int> fstat_descriptors;
  std::vector<int> ftruncate_descriptors;
  std::vector<off_t> ftruncate_sizes;
  std::vector<int> fdatasync_descriptors;
  std::vector<int> fsync_descriptors;
  std::vector<int> rename_directories;
  std::vector<std::string> rename_old_names;
  std::vector<std::string> rename_new_names;
  std::vector<int> lock_descriptors;
  std::vector<int> close_descriptors;

  int next_descriptor{10};
  mode_t metadata_mode{S_IFREG | 0600};
  off_t metadata_size{0};
  std::uint8_t next_read_byte{1};

private:
  static void set_errno(const Outcome& outcome) noexcept {
    if (outcome.result == -1) {
      errno = outcome.error_number;
    }
  }

  static int integer_result(const Outcome& outcome) noexcept {
    set_errno(outcome);
    return static_cast<int>(outcome.result);
  }
};

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-io-test-XXXXXX").string();
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

TEST(PosixFileInjectedTest, RetriesEintrAndShortTransfersAtCheckedExplicitOffsets) {
  ScriptedSyscalls syscalls;
  syscalls.pread_outcomes = {{-1, EINTR}, {2, 0}, {1, 0}, {0, 0}};
  syscalls.pwrite_outcomes = {{-1, EINTR}, {2, 0}, {2, 0}, {1, 0}};
  PosixFile file = detail::PosixHandleFactory::file(7, syscalls);

  std::array<std::byte, 5> destination{};
  const common::Result<std::size_t> read = file.read_at(100, destination);
  ASSERT_TRUE(read.has_value()) << read.error().to_string();
  EXPECT_EQ(*read, 3U);
  EXPECT_EQ((std::vector<off_t>{100, 100, 102, 103}), syscalls.pread_offsets);
  EXPECT_EQ((std::vector<std::size_t>{5, 5, 3, 2}), syscalls.pread_sizes);
  EXPECT_EQ(destination[0], std::byte{1});
  EXPECT_EQ(destination[2], std::byte{3});

  const std::array<std::byte, 5> source{std::byte{9}, std::byte{8}, std::byte{7}, std::byte{6},
                                        std::byte{5}};
  const common::Status write = file.write_all_at(200, source);
  ASSERT_TRUE(write.is_ok()) << write.to_string();
  EXPECT_EQ((std::vector<off_t>{200, 200, 202, 204}), syscalls.pwrite_offsets);
  EXPECT_EQ((std::vector<std::size_t>{5, 5, 3, 1}), syscalls.pwrite_sizes);
  EXPECT_EQ((std::vector<std::byte>{std::byte{9}, std::byte{9}, std::byte{7}, std::byte{5}}),
            syscalls.pwrite_first_bytes);
}

TEST(PosixFileInjectedTest, ReportsHardErrorsAfterPartialTransferAndRejectsZeroProgress) {
  ScriptedSyscalls syscalls;
  syscalls.pwrite_outcomes = {{2, 0}, {-1, EIO}, {0, 0}};
  PosixFile file = detail::PosixHandleFactory::file(7, syscalls);
  const std::array<std::byte, 4> source{};

  const common::Status partial_write = file.write_all_at(10, source);
  EXPECT_EQ(partial_write.code(), common::StatusCode::kIoError);
  EXPECT_NE(partial_write.message().find("after 2 of 4 bytes"), std::string::npos);

  const common::Status no_progress = file.write_all_at(20, source);
  EXPECT_EQ(no_progress.code(), common::StatusCode::kIoError);
  EXPECT_NE(no_progress.message().find("no progress"), std::string::npos);

  syscalls.pread_outcomes = {{2, 0}, {-1, EIO}};
  std::array<std::byte, 4> destination{};
  const common::Result<std::size_t> partial_read = file.read_at(30, destination);
  ASSERT_FALSE(partial_read.has_value());
  EXPECT_EQ(partial_read.error().code(), common::StatusCode::kIoError);
  EXPECT_NE(partial_read.error().message().find("after 2 of 4 bytes"), std::string::npos);
  EXPECT_EQ(destination[0], std::byte{1});
  EXPECT_EQ(destination[1], std::byte{2});
}

TEST(PosixFileInjectedTest, RejectsUnrepresentableTransferRangeBeforeTheFirstSyscall) {
  ScriptedSyscalls syscalls;
  PosixFile file = detail::PosixHandleFactory::file(7, syscalls);
  std::array<std::byte, 2> bytes{};
  const auto maximum = static_cast<std::uint64_t>(std::numeric_limits<off_t>::max());

  const common::Result<std::size_t> read = file.read_at(maximum, bytes);
  ASSERT_FALSE(read.has_value());
  EXPECT_EQ(read.error().code(), common::StatusCode::kOutOfRange);
  EXPECT_TRUE(syscalls.pread_offsets.empty());

  const common::Status write = file.write_all_at(maximum, bytes);
  EXPECT_EQ(write.code(), common::StatusCode::kOutOfRange);
  EXPECT_TRUE(syscalls.pwrite_offsets.empty());

  const common::Result<std::size_t> empty_read = file.read_at(maximum, common::MutableByteView{});
  ASSERT_TRUE(empty_read.has_value());
  EXPECT_EQ(*empty_read, 0U);
}

TEST(PosixFileInjectedTest, RetriesMetadataTruncateAndSynchronizationEintr) {
  ScriptedSyscalls syscalls;
  syscalls.metadata_size = 12;
  syscalls.fstat_outcomes = {{-1, EINTR}, {0, 0}, {0, 0}, {0, 0}};
  syscalls.ftruncate_outcomes = {{-1, EINTR}, {0, 0}};
  syscalls.fdatasync_outcomes = {{-1, EINTR}, {0, 0}};
  syscalls.fsync_outcomes = {{-1, EINTR}, {0, 0}};
  PosixFile file = detail::PosixHandleFactory::file(7, syscalls);

  const common::Result<std::uint64_t> size = file.size();
  ASSERT_TRUE(size.has_value());
  EXPECT_EQ(*size, 12U);

  const common::Status growth = file.truncate(13);
  EXPECT_EQ(growth.code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(syscalls.ftruncate_sizes.empty());

  EXPECT_TRUE(file.truncate(5).is_ok());
  EXPECT_EQ((std::vector<off_t>{5, 5}), syscalls.ftruncate_sizes);
  EXPECT_TRUE(file.sync_data().is_ok());
  EXPECT_EQ(syscalls.fdatasync_descriptors.size(), 2U);
  EXPECT_TRUE(file.sync_all().is_ok());
  EXPECT_EQ(syscalls.fsync_descriptors.size(), 2U);
}

TEST(PosixFileInjectedTest, SurfacesMetadataTruncateSyncAndInvalidSyscallOutcomes) {
  ScriptedSyscalls syscalls;
  PosixFile file = detail::PosixHandleFactory::file(7, syscalls);

  syscalls.metadata_size = -1;
  EXPECT_EQ(file.size().error().code(), common::StatusCode::kIoError);

  syscalls.metadata_size = 8;
  syscalls.metadata_mode = S_IFDIR | 0700;
  EXPECT_EQ(file.size().error().code(), common::StatusCode::kInvalidArgument);

  syscalls.metadata_mode = S_IFREG | 0600;
  syscalls.fstat_outcomes = {{-1, EIO}};
  EXPECT_EQ(file.size().error().code(), common::StatusCode::kIoError);

  syscalls.ftruncate_outcomes = {{-1, EIO}};
  EXPECT_EQ(file.truncate(4).code(), common::StatusCode::kIoError);

  syscalls.fdatasync_outcomes = {{-1, ENOSPC}};
  EXPECT_EQ(file.sync_data().code(), common::StatusCode::kResourceExhausted);
  syscalls.fsync_outcomes = {{-1, EIO}};
  EXPECT_EQ(file.sync_all().code(), common::StatusCode::kIoError);

  const std::array<std::byte, 2> source{};
  syscalls.pwrite_outcomes = {{3, 0}};
  EXPECT_EQ(file.write_all_at(0, source).code(), common::StatusCode::kIoError);
}

TEST(PosixDirectoryInjectedTest, UsesValidatedRelativeExclusiveAndNoReplaceOperations) {
  ScriptedSyscalls syscalls;
  syscalls.open_directory_outcomes = {{-1, EINTR}, {10, 0}};
  syscalls.fstat_outcomes = {{-1, EINTR}, {0, 0}, {0, 0}};
  syscalls.open_at_outcomes = {{-1, EINTR}, {20, 0}};
  syscalls.rename_outcomes = {{-1, EINTR}, {0, 0}, {-1, EEXIST}, {-1, ENOSYS}, {-1, EINVAL}};
  syscalls.fsync_outcomes = {{-1, EINTR}, {0, 0}};

  syscalls.metadata_mode = S_IFDIR | 0700;
  common::Result<PosixDirectory> opened =
      detail::PosixHandleFactory::open_directory("/database/wal", syscalls);
  ASSERT_TRUE(opened.has_value()) << opened.error().to_string();
  EXPECT_EQ(syscalls.opened_paths.size(), 2U);
  EXPECT_NE(syscalls.open_directory_flags.back() & O_DIRECTORY, 0);
  EXPECT_NE(syscalls.open_directory_flags.back() & O_NOFOLLOW, 0);

  syscalls.metadata_mode = S_IFREG | 0600;
  common::Result<PosixFile> created = opened->create_exclusive_regular_file("segment.tmp", 0640U);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  ASSERT_EQ(syscalls.open_at_flags.size(), 2U);
  EXPECT_NE(syscalls.open_at_flags.back() & O_CREAT, 0);
  EXPECT_NE(syscalls.open_at_flags.back() & O_EXCL, 0);
  EXPECT_NE(syscalls.open_at_flags.back() & O_NOFOLLOW, 0);
  EXPECT_EQ(syscalls.open_at_permissions.back(), static_cast<mode_t>(0640));

  EXPECT_EQ(opened->create_exclusive_regular_file("../escape").error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(opened->open_regular_file("nested/file", FileOpenMode::kReadOnly).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(opened->rename_no_replace({.old_name = ".", .new_name = "final"}).code(),
            common::StatusCode::kInvalidArgument);

  EXPECT_TRUE(
      opened->rename_no_replace({.old_name = "segment.tmp", .new_name = "segment.cwal"}).is_ok());
  EXPECT_EQ(syscalls.rename_directories, (std::vector<int>{10, 10}));
  EXPECT_EQ(
      opened->rename_no_replace({.old_name = "another.tmp", .new_name = "segment.cwal"}).code(),
      common::StatusCode::kAlreadyExists);
  EXPECT_EQ(opened->rename_no_replace({.old_name = "another.tmp", .new_name = "other.cwal"}).code(),
            common::StatusCode::kNotSupported);
  EXPECT_EQ(opened->rename_no_replace({.old_name = "another.tmp", .new_name = "third.cwal"}).code(),
            common::StatusCode::kNotSupported);
  EXPECT_TRUE(opened->sync().is_ok());
  EXPECT_EQ(syscalls.fsync_descriptors, (std::vector<int>{10, 10}));
}

TEST(PosixDirectoryInjectedTest, ValidatesRegularFilesAndMapsLockContention) {
  ScriptedSyscalls syscalls;
  PosixDirectory directory = detail::PosixHandleFactory::directory(10, syscalls);

  EXPECT_EQ(directory.create_exclusive_regular_file("entry", 01000U).error().code(),
            common::StatusCode::kInvalidArgument);

  syscalls.metadata_mode = S_IFDIR | 0700;
  const common::Result<PosixFile> not_regular =
      directory.open_regular_file("entry", FileOpenMode::kReadOnly);
  ASSERT_FALSE(not_regular.has_value());
  EXPECT_EQ(not_regular.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(syscalls.close_descriptors, (std::vector<int>{10}));

  syscalls.metadata_mode = S_IFREG | 0600;
  syscalls.lock_outcomes = {{-1, EINTR}, {0, 0}};
  common::Result<PosixAdvisoryLock> lock = directory.acquire_exclusive_lock("LOCK");
  ASSERT_TRUE(lock.has_value()) << lock.error().to_string();
  EXPECT_EQ(syscalls.lock_descriptors.size(), 2U);

  syscalls.lock_outcomes = {{-1, EAGAIN}};
  const common::Result<PosixAdvisoryLock> contention = directory.acquire_exclusive_lock("LOCK");
  ASSERT_FALSE(contention.has_value());
  EXPECT_EQ(contention.error().code(), common::StatusCode::kUnavailable);
  EXPECT_NE(contention.error().message().find("another process"), std::string::npos);
}

TEST(PosixHandleInjectedTest, CloseIsNeverRetriedAndMoveTransfersSingleOwnership) {
  ScriptedSyscalls syscalls;
  syscalls.close_outcomes = {{-1, EINTR}, {0, 0}};
  PosixFile file = detail::PosixHandleFactory::file(7, syscalls);

  const common::Status close_status = file.close();
  EXPECT_EQ(close_status.code(), common::StatusCode::kIoError);
  EXPECT_FALSE(file.is_open());
  EXPECT_EQ(syscalls.close_descriptors, (std::vector<int>{7}));
  EXPECT_TRUE(file.close().is_ok());
  EXPECT_EQ(syscalls.close_descriptors.size(), 1U);

  PosixFile first = detail::PosixHandleFactory::file(8, syscalls);
  PosixFile second = detail::PosixHandleFactory::file(9, syscalls);
  second = std::move(first);
  EXPECT_TRUE(second.is_open());
  EXPECT_EQ(syscalls.close_descriptors.back(), 9);
  EXPECT_TRUE(second.close().is_ok());
  EXPECT_EQ(syscalls.close_descriptors.back(), 8);
}

TEST(PosixIoIntegrationTest, PerformsDurableDirectoryRelativeFileLifecycle) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  common::Result<PosixDirectory> directory = PosixDirectory::open(temporary.path().string());
  ASSERT_TRUE(directory.has_value()) << directory.error().to_string();

  common::Result<PosixFile> file = directory->create_exclusive_regular_file("segment.tmp");
  ASSERT_TRUE(file.has_value()) << file.error().to_string();
  EXPECT_EQ(directory->create_exclusive_regular_file("segment.tmp").error().code(),
            common::StatusCode::kAlreadyExists);

  const std::array<std::byte, 6> first{std::byte{1}, std::byte{2}, std::byte{3},
                                       std::byte{4}, std::byte{5}, std::byte{6}};
  const std::array<std::byte, 2> replacement{std::byte{9}, std::byte{8}};
  ASSERT_TRUE(file->write_all_at(0, first).is_ok());
  ASSERT_TRUE(file->write_all_at(2, replacement).is_ok());
  ASSERT_TRUE(file->sync_data().is_ok());
  ASSERT_TRUE(file->sync_all().is_ok());
  const common::Result<std::uint64_t> original_size = file->size();
  ASSERT_TRUE(original_size.has_value());
  EXPECT_EQ(*original_size, 6U);

  std::array<std::byte, 8> read_buffer{};
  const common::Result<std::size_t> read = file->read_at(0, read_buffer);
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(*read, 6U);
  const std::array<std::byte, 6> expected{std::byte{1}, std::byte{2}, std::byte{9},
                                          std::byte{8}, std::byte{5}, std::byte{6}};
  EXPECT_TRUE(std::equal(expected.begin(), expected.end(), read_buffer.begin()));

  EXPECT_EQ(file->truncate(7).code(), common::StatusCode::kInvalidArgument);
  ASSERT_TRUE(file->truncate(4).is_ok());
  ASSERT_TRUE(file->sync_all().is_ok());
  ASSERT_TRUE(file->close().is_ok());

  ASSERT_TRUE(directory->rename_no_replace({.old_name = "segment.tmp", .new_name = "segment.cwal"})
                  .is_ok());
  ASSERT_TRUE(directory->sync().is_ok());
  common::Result<PosixFile> reopened =
      directory->open_regular_file("segment.cwal", FileOpenMode::kReadOnly);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  const common::Result<std::uint64_t> truncated_size = reopened->size();
  ASSERT_TRUE(truncated_size.has_value());
  EXPECT_EQ(*truncated_size, 4U);

  common::Result<PosixFile> collision = directory->create_exclusive_regular_file("collision");
  ASSERT_TRUE(collision.has_value());
  ASSERT_TRUE(collision->close().is_ok());
  EXPECT_EQ(
      directory->rename_no_replace({.old_name = "segment.cwal", .new_name = "collision"}).code(),
      common::StatusCode::kAlreadyExists);

  std::error_code symlink_error;
  std::filesystem::create_symlink(temporary.path() / "segment.cwal",
                                  temporary.path() / "segment-link", symlink_error);
  ASSERT_FALSE(symlink_error);
  const common::Result<PosixFile> symlink =
      directory->open_regular_file("segment-link", FileOpenMode::kReadOnly);
  EXPECT_FALSE(symlink.has_value());
}

TEST(PosixIoIntegrationTest, HoldsTheAdvisoryLockAgainstAnotherProcess) {
  TemporaryDirectory temporary;
  ASSERT_TRUE(temporary.valid());
  common::Result<PosixDirectory> directory = PosixDirectory::open(temporary.path().string());
  ASSERT_TRUE(directory.has_value());
  common::Result<PosixAdvisoryLock> lock = directory->acquire_exclusive_lock("LOCK");
  ASSERT_TRUE(lock.has_value()) << lock.error().to_string();

  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    common::Result<PosixDirectory> child_directory =
        PosixDirectory::open(temporary.path().string());
    if (!child_directory.has_value()) {
      ::_exit(2);
    }
    const common::Result<PosixAdvisoryLock> child_lock =
        child_directory->acquire_exclusive_lock("LOCK");
    ::_exit(!child_lock.has_value() && child_lock.error().code() == common::StatusCode::kUnavailable
                ? 0
                : 3);
  }

  int child_status = 0;
  ASSERT_EQ(::waitpid(child, &child_status, 0), child);
  ASSERT_TRUE(WIFEXITED(child_status));
  EXPECT_EQ(WEXITSTATUS(child_status), 0);
  EXPECT_TRUE(lock->close().is_ok());
}

} // namespace
} // namespace chronos::io
