#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "ingest/raft_tablet_snapshot_storage_internal.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"
#include "io/posix_syscalls.hpp"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

class TemporarySnapshotRoot {
public:
  TemporarySnapshotRoot() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-rtas-failure-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr) {
      path_ = created;
      std::error_code error;
      static_cast<void>(std::filesystem::create_directory(path_ / "snapshots", error));
      if (error) {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
        path_.clear();
      }
    }
  }

  ~TemporarySnapshotRoot() {
    std::error_code ignored;
    if (!path_.empty())
      std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

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

struct InstallFailureCase {
  SnapshotStorageFault fault;
  std::string_view name;
  bool final_visible;
  bool owner_poisoned;
};

constexpr std::array<InstallFailureCase, 11U> kInstallFailures{
    InstallFailureCase{SnapshotStorageFault::kPriorTemporaryUnlink, "prior_temporary_unlink", false,
                       false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryCreate, "temporary_create", false, false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryValidationStat, "temporary_validation_stat",
                       false, false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryWrite, "temporary_write", false, false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryPartialWrite, "temporary_partial_write",
                       false, false},
    InstallFailureCase{SnapshotStorageFault::kTemporarySizeStat, "temporary_size_stat", false,
                       false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryReadback, "temporary_readback", false,
                       false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryFileSync, "temporary_file_sync", false,
                       false},
    InstallFailureCase{SnapshotStorageFault::kTemporaryClose, "temporary_close", false, false},
    InstallFailureCase{SnapshotStorageFault::kFinalRename, "final_rename", false, false},
    InstallFailureCase{SnapshotStorageFault::kFinalDirectorySync, "final_directory_sync", true,
                       true},
};

class RaftTabletSnapshotStorageFailureTest : public ::testing::TestWithParam<InstallFailureCase> {};

TEST_P(RaftTabletSnapshotStorageFailureTest, FailsClosedAtEachInstallSyscallAndReopenConverges) {
  TemporarySnapshotRoot root;
  ASSERT_FALSE(root.path().empty());
  const InstallFailureCase failure = GetParam();
  const RaftTabletApplicationSnapshot expected = test::crash_application_snapshot();
  OneShotSnapshotStorageFault syscalls{failure.fault};
  {
    auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
        test::crash_snapshot_config(root.path()), syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    syscalls.arm();

    auto installed = storage->install(expected);

    ASSERT_FALSE(installed.has_value());
    EXPECT_EQ(installed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(syscalls.fired());
    EXPECT_EQ(storage->is_usable(), !failure.owner_poisoned);
    EXPECT_EQ(storage->poison_status().is_ok(), !failure.owner_poisoned);
  }

  auto reopened =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(root.path() / "snapshots" /
                                       "snapshot-00000000000000000009.rtas.tmp"));
  auto before_retry = reopened->load(expected.raft_snapshot.last_included_index);
  EXPECT_EQ(before_retry.has_value(), failure.final_visible);
  if (!failure.final_visible)
    EXPECT_EQ(before_retry.error().code(), common::StatusCode::kNotFound);

  auto retried = reopened->install(expected);

  ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
  EXPECT_EQ(retried->already_present, failure.final_visible);
  auto loaded = reopened->load(expected.raft_snapshot.last_included_index);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().to_string();
  EXPECT_EQ(loaded->snapshot, expected);
}

INSTANTIATE_TEST_SUITE_P(EveryInstallSyscall, RaftTabletSnapshotStorageFailureTest,
                         ::testing::ValuesIn(kInstallFailures),
                         [](const ::testing::TestParamInfo<InstallFailureCase>& parameter) {
                           return std::string{parameter.param.name};
                         });

void leave_interrupted_temporary(const std::filesystem::path& root) {
  OneShotSnapshotStorageFault syscalls{SnapshotStorageFault::kTemporaryWrite};
  auto storage = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
      test::crash_snapshot_config(root), syscalls);
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  syscalls.arm();
  auto failed = storage->install(test::crash_application_snapshot());
  ASSERT_FALSE(failed.has_value());
  ASSERT_TRUE(syscalls.fired());
}

TEST(RaftTabletSnapshotStorageCleanupFailureTest, FailedTemporaryUnlinkIsRetryableOnNextOpen) {
  TemporarySnapshotRoot root;
  ASSERT_FALSE(root.path().empty());
  {
    auto storage = RaftTabletSnapshotStorage::create(test::crash_snapshot_config(root.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  }
  leave_interrupted_temporary(root.path());
  const std::filesystem::path temporary =
      root.path() / "snapshots" / "snapshot-00000000000000000009.rtas.tmp";
  ASSERT_TRUE(std::filesystem::exists(temporary));
  OneShotSnapshotStorageFault syscalls{SnapshotStorageFault::kPriorTemporaryUnlink};
  syscalls.arm();

  auto failed = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
      test::crash_snapshot_config(root.path()), syscalls);

  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(syscalls.fired());
  EXPECT_TRUE(std::filesystem::exists(temporary));
  auto reopened =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(temporary));
}

TEST(RaftTabletSnapshotStorageCleanupFailureTest, FailedCleanupSyncIsRetryableOnNextOpen) {
  TemporarySnapshotRoot root;
  ASSERT_FALSE(root.path().empty());
  {
    auto storage = RaftTabletSnapshotStorage::create(test::crash_snapshot_config(root.path()));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  }
  leave_interrupted_temporary(root.path());
  const std::filesystem::path temporary =
      root.path() / "snapshots" / "snapshot-00000000000000000009.rtas.tmp";
  ASSERT_TRUE(std::filesystem::exists(temporary));
  OneShotSnapshotStorageFault syscalls{SnapshotStorageFault::kFinalDirectorySync};
  syscalls.arm();

  auto failed = detail::RaftTabletSnapshotStorageTestAccess::open_existing(
      test::crash_snapshot_config(root.path()), syscalls);

  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(syscalls.fired());
  EXPECT_FALSE(std::filesystem::exists(temporary));
  auto reopened =
      RaftTabletSnapshotStorage::open_existing(test::crash_snapshot_config(root.path()));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
}

} // namespace
} // namespace chronos::ingest
