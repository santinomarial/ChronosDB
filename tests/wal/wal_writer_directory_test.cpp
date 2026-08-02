#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_writer_internal.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace chronos::wal {
namespace {

constexpr std::string_view kFirstSegmentName = "wal-00000000000000000001.cwal";
constexpr std::string_view kOrphanTemporaryName =
    ".wal-00000000000000000001.cwal.tmp-0102030405060708090a0b0c0d0e0f10";

void create_file(const std::filesystem::path& path) {
  std::ofstream output{path, std::ios::binary};
  output.put('x');
  ASSERT_TRUE(output.good());
}

void expect_rejected_without_identity(const std::filesystem::path& path,
                                      const common::StatusCode expected_code,
                                      const std::string_view diagnostic) {
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  const common::Result<WalWriter> result =
      WalWriter::create_new({.directory_path = path.string()}, generator);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), expected_code);
  EXPECT_NE(result.error().message().find(diagnostic), std::string::npos);
  EXPECT_EQ(generator.calls, 0U);
  EXPECT_FALSE(std::filesystem::exists(path / "LOCK"));
}

TEST(WalWriterDirectoryTest, RejectsExistingFinalHistoryBeforeIdentityGeneration) {
  test::TemporaryDirectory temporary{"chronos-wal-existing-history"};
  ASSERT_TRUE(temporary.valid());
  create_file(temporary.path() / kFirstSegmentName);

  expect_rejected_without_identity(temporary.path(), common::StatusCode::kAlreadyExists,
                                   "installed history");
  EXPECT_EQ(std::filesystem::file_size(temporary.path() / kFirstSegmentName), 1U);
}

TEST(WalWriterDirectoryTest, ClassifiesReservedTemporaryAndUnrelatedEntriesStrictly) {
  {
    test::TemporaryDirectory temporary{"chronos-wal-malformed-final"};
    ASSERT_TRUE(temporary.valid());
    create_file(temporary.path() / "wal-1.cwal");
    expect_rejected_without_identity(temporary.path(), common::StatusCode::kCorruption,
                                     "malformed entry");
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-orphan-temporary"};
    ASSERT_TRUE(temporary.valid());
    create_file(temporary.path() / kOrphanTemporaryName);
    expect_rejected_without_identity(temporary.path(), common::StatusCode::kAlreadyExists,
                                     "orphan temporary segment");
    EXPECT_TRUE(std::filesystem::exists(temporary.path() / kOrphanTemporaryName));
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-malformed-temporary"};
    ASSERT_TRUE(temporary.valid());
    create_file(temporary.path() / ".wal-00000000000000000001.cwal.tmp-NOT-HEX");
    expect_rejected_without_identity(temporary.path(), common::StatusCode::kCorruption,
                                     "malformed entry");
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-unrelated-entry"};
    ASSERT_TRUE(temporary.valid());
    create_file(temporary.path() / "operator-note.txt");
    expect_rejected_without_identity(temporary.path(), common::StatusCode::kInvalidArgument,
                                     "unrelated entry");
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-mixed-invalid-entry"};
    ASSERT_TRUE(temporary.valid());
    create_file(temporary.path() / "000-unrelated");
    create_file(temporary.path() / "wal-malformed");
    expect_rejected_without_identity(temporary.path(), common::StatusCode::kCorruption,
                                     "reserved WAL namespace");
  }
}

TEST(WalWriterDirectoryTest, RejectsSymlinksAndNonregularReservedEntries) {
  {
    test::TemporaryDirectory temporary{"chronos-wal-lock-symlink"};
    ASSERT_TRUE(temporary.valid());
    std::error_code error;
    std::filesystem::create_symlink("missing-target", temporary.path() / "LOCK", error);
    ASSERT_FALSE(error);
    test::FixedWalIdGenerator generator{test::make_wal_id()};
    const common::Result<WalWriter> result =
        WalWriter::create_new({.directory_path = temporary.path().string()}, generator);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kCorruption);
    EXPECT_EQ(generator.calls, 0U);
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-final-directory"};
    ASSERT_TRUE(temporary.valid());
    ASSERT_TRUE(std::filesystem::create_directory(temporary.path() / kFirstSegmentName));
    expect_rejected_without_identity(temporary.path(), common::StatusCode::kCorruption,
                                     "not a regular file");
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-final-symlink"};
    ASSERT_TRUE(temporary.valid());
    std::error_code error;
    std::filesystem::create_symlink("missing-target", temporary.path() / kFirstSegmentName, error);
    ASSERT_FALSE(error);
    expect_rejected_without_identity(temporary.path(), common::StatusCode::kCorruption,
                                     "not a regular file");
  }
}

TEST(WalWriterDirectoryTest, AcceptsARegularLockAsTheOnlyExistingEntry) {
  test::TemporaryDirectory temporary{"chronos-wal-existing-lock"};
  ASSERT_TRUE(temporary.valid());
  create_file(temporary.path() / "LOCK");
  test::FixedWalIdGenerator generator{test::make_wal_id()};

  common::Result<WalWriter> created =
      WalWriter::create_new({.directory_path = temporary.path().string()}, generator);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_EQ(generator.calls, 1U);
  EXPECT_TRUE(created->close().is_ok());
}

TEST(WalWriterDirectoryTest, RechecksContentsAfterLockAcquisition) {
  test::ScriptedWalSyscalls syscalls;
  syscalls.directory_entry_snapshots = {
      {},
      {{.name = std::string{kFirstSegmentName}, .type = io::DirectoryEntryType::kRegularFile}},
  };
  test::FixedWalIdGenerator generator{test::make_wal_id()};

  const common::Result<WalWriter> result = detail::WalWriterTestAccess::create_new(
      {.directory_path = "/database/wal"}, generator, syscalls);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kAlreadyExists);
  EXPECT_EQ(generator.calls, 1U);
  EXPECT_TRUE(std::none_of(syscalls.events.begin(), syscalls.events.end(), [](const auto& event) {
    return event.find(".cwal.tmp-") != std::string::npos;
  }));
}

TEST(WalWriterDirectoryTest, GeneratorFailuresAndZeroIdentityDoNotMutateTheDirectory) {
  {
    test::TemporaryDirectory temporary{"chronos-wal-generator-failure"};
    ASSERT_TRUE(temporary.valid());
    test::FixedWalIdGenerator generator{
        common::Status{common::StatusCode::kIoError, "injected entropy failure"}};
    const common::Result<WalWriter> result =
        WalWriter::create_new({.directory_path = temporary.path().string()}, generator);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kIoError);
    EXPECT_EQ(generator.calls, 1U);
    EXPECT_TRUE(std::filesystem::is_empty(temporary.path()));
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-zero-identity"};
    ASSERT_TRUE(temporary.valid());
    test::FixedWalIdGenerator generator{WalId{}};
    const common::Result<WalWriter> result =
        WalWriter::create_new({.directory_path = temporary.path().string()}, generator);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kInvalidArgument);
    EXPECT_EQ(generator.calls, 1U);
    EXPECT_TRUE(std::filesystem::is_empty(temporary.path()));
  }
}

} // namespace
} // namespace chronos::wal
