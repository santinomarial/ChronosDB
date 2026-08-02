#include "chronos/wal/wal_scan.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::wal {
namespace {

TEST(WalCorruptionTest, RejectsCompleteChecksumInvalidFinalRecordWithoutRepairClassification) {
  test::TemporaryDirectory temporary{"chronos-wal-bad-final-crc"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 1U);
  const std::filesystem::path segment = temporary.path() / "wal-00000000000000000001.cwal";
  std::vector<std::byte> bytes = test::read_file(segment);
  ASSERT_GT(bytes.size(), kSegmentHeaderSize + kRecordHeaderSize);
  bytes[kSegmentHeaderSize + kRecordHeaderSize] ^= std::byte{0x80};
  test::write_file(segment, bytes);

  const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  EXPECT_NE(report.error().message().find("record"), std::string::npos);
}

TEST(WalCorruptionTest, RejectsIncompleteBytesInClosedSegment) {
  test::TemporaryDirectory temporary{"chronos-wal-middle-tail"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 2U, .target_segment_size = kSegmentHeaderSize + 64U});
  const std::array<std::byte, 7> suffix{std::byte{0x01}};
  test::append_bytes(temporary.path() / "wal-00000000000000000001.cwal", suffix);

  const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  EXPECT_NE(report.error().message().find("non-final"), std::string::npos);
}

TEST(WalCorruptionTest, RejectsMixedWalIdentityEvenWithValidHeaderChecksum) {
  test::TemporaryDirectory temporary{"chronos-wal-mixed-id"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 2U, .target_segment_size = kSegmentHeaderSize + 64U});
  const std::filesystem::path second = temporary.path() / "wal-00000000000000000002.cwal";
  std::vector<std::byte> bytes = test::read_file(second);
  ASSERT_GE(bytes.size(), kSegmentHeaderSize);
  const common::Result<SegmentHeader> decoded =
      decode_segment_header(common::ByteView{bytes}.first(kSegmentHeaderSize));
  ASSERT_TRUE(decoded.has_value());
  SegmentHeader changed = *decoded;
  changed.wal_id = test::make_wal_id(0x7fU);
  const common::Result<EncodedSegmentHeader> header = encode_segment_header(changed);
  ASSERT_TRUE(header.has_value());
  std::copy(header->begin(), header->end(), bytes.begin());
  test::write_file(second, bytes);

  const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  EXPECT_NE(report.error().message().find("mixed identities"), std::string::npos);
}

TEST(WalCorruptionTest, RejectsRecordSequenceDiscontinuity) {
  test::TemporaryDirectory temporary{"chronos-wal-record-gap"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 0U);
  const std::vector<std::byte> record = test::encode_physical_record(2U, 2U, {});
  test::append_bytes(temporary.path() / "wal-00000000000000000001.cwal", record);

  const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  EXPECT_NE(report.error().message().find("sequence"), std::string::npos);
}

} // namespace
} // namespace chronos::wal
