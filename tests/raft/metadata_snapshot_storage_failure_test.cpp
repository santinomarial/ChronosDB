#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "io/posix_syscalls.hpp"
#include "raft/metadata_snapshot_storage_internal.hpp"

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

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-metadata-snapshot-failure-XXXXXX")
            .string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~TemporaryDirectory() {
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

[[nodiscard]] GroupId group() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{8U};
  return GroupId{bytes};
}

[[nodiscard]] MetadataApplicationSnapshot snapshot() {
  SnapshotMetadata metadata{.last_included_index = 7U,
                            .last_included_term = 2U,
                            .manifest_generation = 7U,
                            .part_set_checksum = {},
                            .configuration_index = 4U,
                            .voters = {1U, 2U}};
  metadata.part_set_checksum.front() = std::byte{0x5AU};
  return {.group_id = group(),
          .raft_snapshot = std::move(metadata),
          .entries = {{.index = 3U,
                       .term = 1U,
                       .type = kRaftMetadataCommandEntryType,
                       .payload = {std::byte{1U}, std::byte{2U}}},
                      {.index = 6U,
                       .term = 2U,
                       .type = kRaftMetadataCommandEntryType,
                       .payload = {std::byte{3U}}}}};
}

[[nodiscard]] MetadataSnapshotStorageConfig config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .group_id = group()};
}

enum class StorageFault : std::uint8_t {
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

class OneShotStorageFault final : public io::detail::PosixSyscalls {
public:
  explicit OneShotStorageFault(const StorageFault fault)
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
    if (armed_ && fault_ == StorageFault::kTemporaryCreate &&
        std::string_view{request.name}.ends_with(".rmas.tmp")) {
      return fail();
    }
    return delegate_.open_at(request);
  }

  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }

  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    if (armed_ && fault_ == StorageFault::kTemporaryReadback)
      return fail_ssize();
    return delegate_.pread(request);
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    if (armed_ && fault_ == StorageFault::kTemporaryWrite)
      return fail_ssize();
    if (armed_ && fault_ == StorageFault::kTemporaryPartialWrite) {
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
    if ((fault_ == StorageFault::kTemporaryValidationStat && regular_stat_calls_ == 1U) ||
        (fault_ == StorageFault::kTemporarySizeStat && regular_stat_calls_ == 2U)) {
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
    if (armed_ && (fault_ == StorageFault::kTemporaryFileSync ||
                   fault_ == StorageFault::kFinalDirectorySync)) {
      struct stat metadata {};
      if (delegate_.fstat(descriptor, &metadata) == 0 &&
          ((fault_ == StorageFault::kTemporaryFileSync && S_ISREG(metadata.st_mode)) ||
           (fault_ == StorageFault::kFinalDirectorySync && S_ISDIR(metadata.st_mode)))) {
        return fail();
      }
    }
    return delegate_.fsync(descriptor);
  }

  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    if (armed_ && fault_ == StorageFault::kFinalRename)
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
    if (armed_ && fault_ == StorageFault::kPriorTemporaryUnlink &&
        std::string_view{name}.ends_with(".rmas.tmp")) {
      return fail();
    }
    return delegate_.unlink_at(directory_descriptor, name);
  }

  int close(const int descriptor) override {
    const int result = delegate_.close(descriptor);
    if (armed_ && result == 0 && fault_ == StorageFault::kTemporaryClose)
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
  StorageFault fault_;
  std::size_t regular_stat_calls_{};
  bool partial_write_started_{false};
  bool armed_{false};
  bool fired_{false};
};

struct InstallFailureCase {
  StorageFault fault;
  std::string_view name;
  bool final_visible;
  bool owner_poisoned;
};

constexpr std::array<InstallFailureCase, 11U> kInstallFailures{
    InstallFailureCase{StorageFault::kPriorTemporaryUnlink, "prior_temporary_unlink", false, false},
    InstallFailureCase{StorageFault::kTemporaryCreate, "temporary_create", false, false},
    InstallFailureCase{StorageFault::kTemporaryValidationStat, "temporary_validation_stat", false,
                       false},
    InstallFailureCase{StorageFault::kTemporaryWrite, "temporary_write", false, false},
    InstallFailureCase{StorageFault::kTemporaryPartialWrite, "temporary_partial_write", false,
                       false},
    InstallFailureCase{StorageFault::kTemporarySizeStat, "temporary_size_stat", false, false},
    InstallFailureCase{StorageFault::kTemporaryReadback, "temporary_readback", false, false},
    InstallFailureCase{StorageFault::kTemporaryFileSync, "temporary_file_sync", false, false},
    InstallFailureCase{StorageFault::kTemporaryClose, "temporary_close", false, false},
    InstallFailureCase{StorageFault::kFinalRename, "final_rename", false, false},
    InstallFailureCase{StorageFault::kFinalDirectorySync, "final_directory_sync", true, true},
};

class MetadataSnapshotStorageFailureTest : public ::testing::TestWithParam<InstallFailureCase> {};

TEST_P(MetadataSnapshotStorageFailureTest, FailsClosedAtEachInstallSyscallAndReopenConverges) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const InstallFailureCase failure = GetParam();
  const MetadataApplicationSnapshot expected = snapshot();
  OneShotStorageFault syscalls{failure.fault};
  {
    auto storage = detail::MetadataSnapshotStorageTestAccess::create(config(directory), syscalls);
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
    syscalls.arm();

    auto installed = storage->install(expected);

    ASSERT_FALSE(installed.has_value());
    EXPECT_EQ(installed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(syscalls.fired());
    EXPECT_EQ(storage->is_usable(), !failure.owner_poisoned);
    EXPECT_EQ(storage->poison_status().is_ok(), !failure.owner_poisoned);
  }

  auto reopened = MetadataSnapshotStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(directory.path() /
                                       "metadata-snapshot-00000000000000000007.rmas.tmp"));
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

INSTANTIATE_TEST_SUITE_P(EveryInstallSyscall, MetadataSnapshotStorageFailureTest,
                         ::testing::ValuesIn(kInstallFailures),
                         [](const ::testing::TestParamInfo<InstallFailureCase>& parameter) {
                           return std::string{parameter.param.name};
                         });

void leave_interrupted_temporary(const TemporaryDirectory& directory) {
  OneShotStorageFault syscalls{StorageFault::kTemporaryWrite};
  auto storage =
      detail::MetadataSnapshotStorageTestAccess::open_existing(config(directory), syscalls);
  ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  syscalls.arm();
  auto failed = storage->install(snapshot());
  ASSERT_FALSE(failed.has_value());
  ASSERT_TRUE(syscalls.fired());
}

TEST(MetadataSnapshotStorageCleanupFailureTest, FailedTemporaryUnlinkIsRetryableOnNextOpen) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto storage = MetadataSnapshotStorage::create(config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  }
  leave_interrupted_temporary(directory);
  const std::filesystem::path temporary =
      directory.path() / "metadata-snapshot-00000000000000000007.rmas.tmp";
  ASSERT_TRUE(std::filesystem::exists(temporary));
  OneShotStorageFault syscalls{StorageFault::kPriorTemporaryUnlink};
  syscalls.arm();

  auto failed =
      detail::MetadataSnapshotStorageTestAccess::open_existing(config(directory), syscalls);

  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(syscalls.fired());
  EXPECT_TRUE(std::filesystem::exists(temporary));
  auto reopened = MetadataSnapshotStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(temporary));
}

TEST(MetadataSnapshotStorageCleanupFailureTest, FailedCleanupSyncIsRetryableOnNextOpen) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  {
    auto storage = MetadataSnapshotStorage::create(config(directory));
    ASSERT_TRUE(storage.has_value()) << storage.error().to_string();
  }
  leave_interrupted_temporary(directory);
  const std::filesystem::path temporary =
      directory.path() / "metadata-snapshot-00000000000000000007.rmas.tmp";
  ASSERT_TRUE(std::filesystem::exists(temporary));
  OneShotStorageFault syscalls{StorageFault::kFinalDirectorySync};
  syscalls.arm();

  auto failed =
      detail::MetadataSnapshotStorageTestAccess::open_existing(config(directory), syscalls);

  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(syscalls.fired());
  EXPECT_FALSE(std::filesystem::exists(temporary));
  auto reopened = MetadataSnapshotStorage::open_existing(config(directory));
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
}

} // namespace
} // namespace chronos::raft
