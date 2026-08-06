#include "chronos/wal/wal_paths.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "io/posix_syscalls.hpp"
#include "wal/wal_recovery_internal.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <array>
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace chronos::wal {
namespace {

class FailingFsyncSyscalls final : public io::detail::PosixSyscalls {
public:
  explicit FailingFsyncSyscalls(const std::size_t fail_call)
      : delegate_(io::detail::system_posix_syscalls()), fail_call_(fail_call) {}

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
    ++fsync_calls;
    if (fsync_calls == fail_call_) {
      errno = EIO;
      return -1;
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
  int unlink_at(const int directory_descriptor, const char* const name) override {
    return delegate_.unlink_at(directory_descriptor, name);
  }
  int close(const int descriptor) override {
    return delegate_.close(descriptor);
  }

  std::size_t fsync_calls{};

private:
  io::detail::PosixSyscalls& delegate_;
  std::size_t fail_call_;
};

TEST(WalRecoveryTest, RepeatedCleanRecoveryProducesTheSameReportAndReplay) {
  test::TemporaryDirectory temporary{"chronos-wal-recovery-repeat"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 3U);
  test::CollectingReplaySink first_sink;
  test::CollectingReplaySink second_sink;

  const common::Result<WalRecoveryReport> first =
      recover_wal({.directory_path = temporary.path().string()}, {}, first_sink);
  const common::Result<WalRecoveryReport> second =
      recover_wal({.directory_path = temporary.path().string()}, {}, second_sink);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_EQ(*first, *second);
  EXPECT_EQ(first_sink.preflight_sequences, second_sink.preflight_sequences);
  EXPECT_EQ(first_sink.replay_sequences, second_sink.replay_sequences);
}

TEST(WalRecoveryTest, FailedRepairFileSyncFailsClosedAndARepeatConverges) {
  test::TemporaryDirectory temporary{"chronos-wal-repair-sync-failure"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 1U);
  const std::array<std::byte, 5> tail{std::byte{0x55}};
  test::append_bytes(temporary.path() / "wal-00000000000000000001.cwal", tail);
  FailingFsyncSyscalls syscalls{1U};
  test::CollectingReplaySink failed_sink;

  const common::Result<WalRecoveryReport> failed = detail::WalRecoveryTestAccess::recover(
      {.directory_path = temporary.path().string()}, {.repair_incomplete_final_tail = true},
      failed_sink, syscalls);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_NE(failed.error().message().find("synchronize repaired"), std::string::npos);
  EXPECT_TRUE(failed_sink.replay_sequences.empty());

  test::CollectingReplaySink retry_sink;
  const common::Result<WalRecoveryReport> retry =
      recover_wal({.directory_path = temporary.path().string()},
                  {.repair_incomplete_final_tail = true}, retry_sink);
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  EXPECT_EQ(retry->classification, WalScanClassification::kClean);
  EXPECT_EQ(retry_sink.replay_sequences, (std::vector<std::uint64_t>{1U}));
}

TEST(WalRecoveryTest, StartupNamespaceSyncFailureNeverReturnsAUsableWriterState) {
  test::TemporaryDirectory temporary{"chronos-wal-startup-sync-failure"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 1U);
  FailingFsyncSyscalls syscalls{2U};
  test::CollectingReplaySink sink;

  const common::Result<WalRecoveryReport> failed = detail::WalRecoveryTestAccess::recover(
      {.directory_path = temporary.path().string()}, {}, sink, syscalls);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_NE(failed.error().message().find("namespace barrier"), std::string::npos);
  EXPECT_EQ(sink.replay_sequences, (std::vector<std::uint64_t>{1U}));

  test::CollectingReplaySink retry_sink;
  EXPECT_TRUE(
      recover_wal({.directory_path = temporary.path().string()}, {}, retry_sink).has_value());
}

TEST(WalRecoveryTest, CheckpointRecoveryCleansTemporariesAndReleasesWriterOwnership) {
  test::TemporaryDirectory temporary{"chronos-wal-recovery-checkpoint-cleanup"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 3U, .target_segment_size = kSegmentHeaderSize + 64U});
  ASSERT_TRUE(std::filesystem::remove(temporary.path() / "wal-00000000000000000001.cwal"));
  const common::Result<std::string> temporary_name =
      wal_temporary_segment_file_name(4U, test::make_wal_id());
  ASSERT_TRUE(temporary_name.has_value());
  test::write_file(temporary.path() / *temporary_name, EncodedSegmentHeader{});
  test::CollectingReplaySink sink;

  const common::Result<WalRecoveryReport> recovered =
      recover_wal_from_checkpoint({.directory_path = temporary.path().string()}, {},
                                  {.wal_id = test::make_wal_id(),
                                   .record_sequence = 1U,
                                   .segment_number = 1U,
                                   .byte_offset = kSegmentHeaderSize + 64U},
                                  sink);
  ASSERT_TRUE(recovered.has_value()) << recovered.error().to_string();
  EXPECT_EQ(sink.replay_sequences, (std::vector<std::uint64_t>{2U, 3U}));
  EXPECT_EQ(recovered->temporary_files_removed, 1U);
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / *temporary_name));

  test::CollectingReplaySink repeat_sink;
  EXPECT_TRUE(recover_wal_from_checkpoint({.directory_path = temporary.path().string()}, {},
                                          {.wal_id = test::make_wal_id(),
                                           .record_sequence = 1U,
                                           .segment_number = 1U,
                                           .byte_offset = kSegmentHeaderSize + 64U},
                                          repeat_sink)
                  .has_value());
  EXPECT_EQ(repeat_sink.replay_sequences, sink.replay_sequences);
}

} // namespace
} // namespace chronos::wal
