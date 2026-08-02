#include "chronos/wal/wal_paths.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_writer_internal.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <cerrno>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <vector>

namespace chronos::wal {
namespace {

TEST(WalPathsTest, ProducesTheExactAcceptedFinalAndTemporaryNames) {
  const common::Result<std::string> first = wal_segment_file_name(1U);
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(*first, "wal-00000000000000000001.cwal");
  const common::Result<std::string> maximum =
      wal_segment_file_name(std::numeric_limits<std::uint64_t>::max());
  ASSERT_TRUE(maximum.has_value());
  EXPECT_EQ(*maximum, "wal-18446744073709551615.cwal");
  EXPECT_EQ(wal_segment_file_name(0U).error().code(), common::StatusCode::kInvalidArgument);

  const WalId nonce = test::make_wal_id(0xabU);
  const common::Result<std::string> temporary = wal_temporary_segment_file_name(1U, nonce);
  ASSERT_TRUE(temporary.has_value());
  EXPECT_EQ(*temporary, ".wal-00000000000000000001.cwal.tmp-ab0102030405060708090a0b0c0d0e0f");
  EXPECT_EQ(wal_temporary_segment_file_name(1U, WalId{}).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(WalSegmentInstallationTest, OrdersHeaderWriteFileSyncRenameAndDirectorySync) {
  test::ScriptedWalSyscalls syscalls;
  test::FixedWalIdGenerator generator{test::make_wal_id(0xabU)};
  common::Result<WalWriter> created = detail::WalWriterTestAccess::create_new(
      {.directory_path = "/database/wal"}, generator, syscalls);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();

  const std::vector<std::string> expected{
      "open_directory:/database/wal",
      "fstat:10",
      "fstat:10",
      "open_at:LOCK",
      "fstat:11",
      "lock:11",
      "open_at:.wal-00000000000000000001.cwal.tmp-ab0102030405060708090a0b0c0d0e0f",
      "fstat:12",
      "pwrite:12@0",
      "fstat:12",
      "fsync:12",
      std::string{
          "rename:.wal-00000000000000000001.cwal.tmp-ab0102030405060708090a0b0c0d0e0f->wal-"} +
          "00000000000000000001.cwal",
      "fsync:10",
  };
  ASSERT_GE(syscalls.events.size(), expected.size());
  EXPECT_TRUE(std::equal(expected.begin(), expected.end(), syscalls.events.begin()));
  EXPECT_EQ(created->durable_position().byte_offset, kSegmentHeaderSize);
}

TEST(WalSegmentInstallationTest, StopsAtEachFailedDurabilityTransition) {
  {
    test::ScriptedWalSyscalls syscalls;
    syscalls.pwrite_outcomes = {{-1, EIO}};
    test::FixedWalIdGenerator generator{test::make_wal_id()};
    const common::Result<WalWriter> failed = detail::WalWriterTestAccess::create_new(
        {.directory_path = "/database/wal"}, generator, syscalls);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(
        std::none_of(syscalls.events.begin(), syscalls.events.end(), [](const std::string& event) {
          return event.starts_with("fsync:") || event.starts_with("rename:");
        }));
  }
  {
    test::ScriptedWalSyscalls syscalls;
    syscalls.fsync_outcomes = {{-1, EIO}};
    test::FixedWalIdGenerator generator{test::make_wal_id()};
    const common::Result<WalWriter> failed = detail::WalWriterTestAccess::create_new(
        {.directory_path = "/database/wal"}, generator, syscalls);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(
        std::none_of(syscalls.events.begin(), syscalls.events.end(),
                     [](const std::string& event) { return event.starts_with("rename:"); }));
  }
  {
    test::ScriptedWalSyscalls syscalls;
    syscalls.rename_outcomes = {{-1, EEXIST}};
    test::FixedWalIdGenerator generator{test::make_wal_id()};
    const common::Result<WalWriter> failed = detail::WalWriterTestAccess::create_new(
        {.directory_path = "/database/wal"}, generator, syscalls);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kAlreadyExists);
    EXPECT_EQ(syscalls.events.back(), "close:10");
    EXPECT_EQ(static_cast<std::size_t>(std::count_if(
                  syscalls.events.begin(), syscalls.events.end(),
                  [](const std::string& event) { return event.starts_with("fsync:"); })),
              1U);
  }
  {
    test::ScriptedWalSyscalls syscalls;
    syscalls.fsync_outcomes = {{0, 0}, {-1, EIO}};
    test::FixedWalIdGenerator generator{test::make_wal_id()};
    const common::Result<WalWriter> failed = detail::WalWriterTestAccess::create_new(
        {.directory_path = "/database/wal"}, generator, syscalls);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
    EXPECT_TRUE(std::any_of(syscalls.events.begin(), syscalls.events.end(),
                            [](const std::string& event) { return event.starts_with("rename:"); }));
  }
}

TEST(WalSegmentInstallationTest, LockContentionPreventsIdentityGenerationAndFileCreation) {
  test::ScriptedWalSyscalls syscalls;
  syscalls.lock_outcomes = {{-1, EAGAIN}};
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  const common::Result<WalWriter> failed = detail::WalWriterTestAccess::create_new(
      {.directory_path = "/database/wal"}, generator, syscalls);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(generator.calls, 0U);
  EXPECT_EQ(static_cast<std::size_t>(std::count_if(
                syscalls.events.begin(), syscalls.events.end(),
                [](const std::string& event) { return event.starts_with("open_at:"); })),
            1U);
}

} // namespace
} // namespace chronos::wal
