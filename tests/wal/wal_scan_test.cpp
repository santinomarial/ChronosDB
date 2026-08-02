#include "chronos/wal/wal_scan.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::wal {
namespace {

TEST(WalScanTest, ReportsExactCleanPhysicalEndAndSequenceHistory) {
  test::TemporaryDirectory temporary{"chronos-wal-clean-scan"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 3U);

  const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(report->classification, WalScanClassification::kClean);
  EXPECT_EQ(report->wal_id, test::make_wal_id());
  EXPECT_EQ(report->record_count, 3U);
  EXPECT_EQ(report->last_record_sequence, 3U);
  EXPECT_EQ(report->valid_end.byte_offset, kSegmentHeaderSize + (std::uint64_t{3U} * 64U));
  EXPECT_EQ(report->observed_final_size, report->valid_end.byte_offset);
}

TEST(WalScanTest, ClassifiesOnlyTheTwoAcceptedIncompleteFinalTailShapes) {
  {
    test::TemporaryDirectory temporary{"chronos-wal-short-tail"};
    ASSERT_TRUE(temporary.valid());
    test::create_wal(temporary.path(), 1U);
    const std::array<std::byte, 9> suffix{std::byte{0x01}, std::byte{0x02}};
    test::append_bytes(temporary.path() / "wal-00000000000000000001.cwal", suffix);

    const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
    ASSERT_TRUE(report.has_value()) << report.error().to_string();
    EXPECT_EQ(report->classification, WalScanClassification::kIncompleteFinalTail);
    EXPECT_EQ(report->record_count, 1U);
    EXPECT_EQ(report->valid_end.byte_offset, kSegmentHeaderSize + 64U);
  }
  {
    test::TemporaryDirectory temporary{"chronos-wal-declared-tail"};
    ASSERT_TRUE(temporary.valid());
    test::create_wal(temporary.path(), 1U);
    const common::Result<RecordHeader> header =
        make_record_header({.record_type = kApplicationEntryRecordType,
                            .record_sequence = 2U,
                            .payload_length = kApplicationEnvelopeSize});
    ASSERT_TRUE(header.has_value());
    const common::Result<EncodedRecordHeader> encoded = encode_record_header(*header);
    ASSERT_TRUE(encoded.has_value());
    test::append_bytes(temporary.path() / "wal-00000000000000000001.cwal", *encoded);

    const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
    ASSERT_TRUE(report.has_value()) << report.error().to_string();
    EXPECT_EQ(report->classification, WalScanClassification::kIncompleteFinalTail);
    EXPECT_EQ(report->valid_end.byte_offset, kSegmentHeaderSize + 64U);
  }
}

TEST(WalScanTest, ScansManyRecordsWithoutRetainingReplayPayloads) {
  test::TemporaryDirectory temporary{"chronos-wal-bounded-scan"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 128U);

  const common::Result<WalRecoveryReport> report = scan_wal(temporary.path().string());
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(report->record_count, 128U);
  EXPECT_EQ(report->classification, WalScanClassification::kClean);
}

} // namespace
} // namespace chronos::wal
