#include "chronos/wal/wal_paths.hpp"
#include "chronos/wal/wal_scan.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <cstdint>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace chronos::wal {
namespace {

TEST(WalReopenTest, PreservesIdentitySequenceAndExactPhysicalEndWhenAppending) {
  test::TemporaryDirectory temporary{"chronos-wal-reopen"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 2U);
  const common::Result<WalRecoveryReport> before = scan_wal(temporary.path().string());
  ASSERT_TRUE(before.has_value());
  test::CollectingReplaySink sink;

  common::Result<WalWriter> reopened =
      WalWriter::open_existing({.directory_path = temporary.path().string()}, {}, sink);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->wal_id(), before->wal_id);
  EXPECT_EQ(reopened->written_position(), before->valid_end);
  EXPECT_EQ(reopened->durable_position(), before->valid_end);
  EXPECT_EQ(reopened->written_record_sequence(), 2U);
  ASSERT_TRUE(reopened->next_record_sequence().has_value());
  EXPECT_EQ(*reopened->next_record_sequence(), 3U);
  EXPECT_EQ(sink.replay_sequences, (std::vector<std::uint64_t>{1U, 2U}));

  const common::Result<WalAppendResult> appended =
      reopened->append_application_entry(test::make_application_payload());
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->record_sequence, 3U);
  EXPECT_EQ(appended->record_start, before->valid_end);
  ASSERT_TRUE(reopened->synchronize().has_value());
  EXPECT_TRUE(reopened->close().is_ok());

  const common::Result<WalRecoveryReport> after = scan_wal(temporary.path().string());
  ASSERT_TRUE(after.has_value());
  EXPECT_EQ(after->wal_id, before->wal_id);
  EXPECT_EQ(after->record_count, 3U);
  EXPECT_EQ(after->last_record_sequence, 3U);
}

TEST(WalReopenTest, SmallerRuntimeTargetRotatesInsteadOfUnderflowingExistingEnd) {
  test::TemporaryDirectory temporary{"chronos-wal-reopen-smaller-target"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 2U);
  test::CollectingReplaySink sink;

  common::Result<WalWriter> reopened =
      WalWriter::open_existing({.directory_path = temporary.path().string(),
                                .target_segment_size = kSegmentHeaderSize + 64U,
                                .maximum_application_payload = kApplicationEnvelopeSize},
                               {}, sink);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  ASSERT_GT(reopened->active_segment().end_offset, kSegmentHeaderSize + 64U);
  const common::Result<WalAppendResult> appended =
      reopened->append_application_entry(test::make_application_payload());
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->record_sequence, 3U);
  EXPECT_EQ(appended->record_start.segment_number, 2U);
  EXPECT_EQ(appended->record_start.byte_offset, kSegmentHeaderSize);
  EXPECT_TRUE(reopened->close().is_ok());
}

TEST(WalReopenTest, RetainsExclusiveLockForEntireReturnedWriterLifetime) {
  test::TemporaryDirectory temporary{"chronos-wal-reopen-lock"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 0U);
  test::CollectingReplaySink sink;
  common::Result<WalWriter> reopened =
      WalWriter::open_existing({.directory_path = temporary.path().string()}, {}, sink);
  ASSERT_TRUE(reopened.has_value());

  const common::Result<WalRecoveryReport> concurrent = scan_wal(temporary.path().string());
  ASSERT_FALSE(concurrent.has_value());
  EXPECT_EQ(concurrent.error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(reopened->close().is_ok());
  EXPECT_TRUE(scan_wal(temporary.path().string()).has_value());
}

TEST(WalReopenTest, RemovesRecognizedOrphanTemporaryBeforeFutureRotation) {
  test::TemporaryDirectory temporary{"chronos-wal-reopen-orphan"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 1U, .target_segment_size = kSegmentHeaderSize + 64U});
  const common::Result<std::string> orphan_name =
      wal_temporary_segment_file_name(2U, test::make_wal_id());
  ASSERT_TRUE(orphan_name.has_value());
  const std::filesystem::path orphan = temporary.path() / *orphan_name;
  const EncodedSegmentHeader zero_header{};
  test::write_file(orphan, zero_header);
  ASSERT_TRUE(std::filesystem::exists(orphan));
  test::CollectingReplaySink sink;

  common::Result<WalWriter> reopened =
      WalWriter::open_existing({.directory_path = temporary.path().string(),
                                .target_segment_size = kSegmentHeaderSize + 64U,
                                .maximum_application_payload = kApplicationEnvelopeSize},
                               {}, sink);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_FALSE(std::filesystem::exists(orphan));
  const common::Result<WalAppendResult> appended =
      reopened->append_application_entry(test::make_application_payload());
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->record_start.segment_number, 2U);
  EXPECT_TRUE(reopened->close().is_ok());
}

} // namespace
} // namespace chronos::wal
