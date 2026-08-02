#include "chronos/wal/wal_scan.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>

namespace chronos::wal {
namespace {

void create_marker(const std::filesystem::path& path) {
  std::ofstream output{path, std::ios::binary};
  output.put('x');
  ASSERT_TRUE(output.good());
}

TEST(WalDiscoveryTest, DiscoversConsecutiveSegmentsAndRecognizedTemporaryFiles) {
  test::TemporaryDirectory temporary{"chronos-wal-discovery"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 2U, kSegmentHeaderSize + 64U);
  create_marker(temporary.path() /
                ".wal-00000000000000000003.cwal.tmp-0102030405060708090a0b0c0d0e0f10");

  const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(report->classification, WalScanClassification::kClean);
  EXPECT_EQ(report->segment_count, 2U);
  EXPECT_EQ(report->temporary_file_count, 1U);
  EXPECT_EQ(report->record_count, 2U);
}

TEST(WalDiscoveryTest, RejectsGapsMalformedNamesAndUnrelatedEntries) {
  {
    test::TemporaryDirectory temporary{"chronos-wal-gap"};
    ASSERT_TRUE(temporary.valid());
    test::create_wal(temporary.path(), 0U);
    std::filesystem::rename(temporary.path() / "wal-00000000000000000001.cwal",
                            temporary.path() / "wal-00000000000000000002.cwal");
    const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
    ASSERT_FALSE(report.has_value());
    EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-malformed"};
    ASSERT_TRUE(temporary.valid());
    test::create_wal(temporary.path(), 0U);
    create_marker(temporary.path() / "wal-malformed.cwal");
    const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
    ASSERT_FALSE(report.has_value());
    EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-unrelated"};
    ASSERT_TRUE(temporary.valid());
    test::create_wal(temporary.path(), 0U);
    create_marker(temporary.path() / "notes.txt");
    const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
    ASSERT_FALSE(report.has_value());
    EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  }
}

TEST(WalDiscoveryTest, RejectsSymlinkedFinalSegmentAndActiveWriter) {
  {
    test::TemporaryDirectory temporary{"chronos-wal-symlink"};
    ASSERT_TRUE(temporary.valid());
    test::create_wal(temporary.path(), 0U);
    std::filesystem::remove(temporary.path() / "wal-00000000000000000001.cwal");
    std::error_code error;
    std::filesystem::create_symlink("missing", temporary.path() / "wal-00000000000000000001.cwal",
                                    error);
    ASSERT_FALSE(error);
    const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
    ASSERT_FALSE(report.has_value());
    EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-locked-scan"};
    ASSERT_TRUE(temporary.valid());
    test::FixedWalIdGenerator generator{test::make_wal_id()};
    common::Result<WalWriter> writer =
        WalWriter::create_new({.directory_path = temporary.path().string()}, generator);
    ASSERT_TRUE(writer.has_value());
    const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
    ASSERT_FALSE(report.has_value());
    EXPECT_EQ(report.error().code(), common::StatusCode::kUnavailable);
    EXPECT_TRUE(writer->close().is_ok());
  }
}

} // namespace
} // namespace chronos::wal
