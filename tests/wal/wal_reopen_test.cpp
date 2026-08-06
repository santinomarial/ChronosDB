#include "chronos/wal/wal_paths.hpp"
#include "chronos/wal/wal_scan.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <array>
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

TEST(WalReopenTest, ReopensRemovedPrefixFromCheckpointAndContinuesGlobalSequence) {
  test::TemporaryDirectory temporary{"chronos-wal-reopen-checkpoint"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 4U, .target_segment_size = kSegmentHeaderSize + 64U});
  ASSERT_TRUE(std::filesystem::remove(temporary.path() / "wal-00000000000000000001.cwal"));
  ASSERT_TRUE(std::filesystem::remove(temporary.path() / "wal-00000000000000000002.cwal"));
  const WalReplayCheckpoint checkpoint{.wal_id = test::make_wal_id(),
                                       .record_sequence = 2U,
                                       .segment_number = 2U,
                                       .byte_offset = kSegmentHeaderSize + 64U};
  test::CollectingReplaySink sink;

  common::Result<WalWriter> reopened = WalWriter::open_existing_from_checkpoint(
      {.directory_path = temporary.path().string(),
       .target_segment_size = kSegmentHeaderSize + 64U,
       .maximum_application_payload = kApplicationEnvelopeSize},
      {}, checkpoint, sink);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(sink.preflight_sequences, (std::vector<std::uint64_t>{3U, 4U}));
  EXPECT_EQ(sink.replay_sequences, sink.preflight_sequences);
  EXPECT_EQ(reopened->wal_id(), checkpoint.wal_id);
  EXPECT_EQ(reopened->written_record_sequence(), 4U);
  EXPECT_EQ(reopened->written_position().segment_number, 4U);
  ASSERT_TRUE(reopened->next_record_sequence().has_value());
  EXPECT_EQ(*reopened->next_record_sequence(), 5U);

  const common::Result<WalAppendResult> appended =
      reopened->append_application_entry(test::make_application_payload());
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(appended->record_sequence, 5U);
  EXPECT_EQ(appended->record_start.segment_number, 5U);
  ASSERT_TRUE(reopened->synchronize().has_value());
  EXPECT_TRUE(reopened->close().is_ok());

  test::CollectingReplaySink verified;
  const common::Result<WalRecoveryReport> report =
      inspect_wal_suffix(temporary.path().string(), checkpoint, verified);
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(verified.replay_sequences, (std::vector<std::uint64_t>{3U, 4U, 5U}));
  EXPECT_EQ(report->last_record_sequence, 5U);
}

TEST(WalReopenTest, CheckpointTailRepairRequiresAuthorizationAndThenConverges) {
  test::TemporaryDirectory temporary{"chronos-wal-reopen-checkpoint-repair"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 3U, .target_segment_size = kSegmentHeaderSize + 64U});
  ASSERT_TRUE(std::filesystem::remove(temporary.path() / "wal-00000000000000000001.cwal"));
  const std::array<std::byte, 5U> incomplete_tail{std::byte{0x55U}};
  test::append_bytes(temporary.path() / "wal-00000000000000000003.cwal", incomplete_tail);
  const WalReplayCheckpoint checkpoint{.wal_id = test::make_wal_id(),
                                       .record_sequence = 1U,
                                       .segment_number = 1U,
                                       .byte_offset = kSegmentHeaderSize + 64U};
  test::CollectingReplaySink rejected_sink;

  common::Result<WalWriter> rejected = WalWriter::open_existing_from_checkpoint(
      {.directory_path = temporary.path().string()}, {}, checkpoint, rejected_sink);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kOutOfRange);
  EXPECT_TRUE(rejected_sink.preflight_sequences.empty());

  test::CollectingReplaySink repaired_sink;
  common::Result<WalWriter> repaired = WalWriter::open_existing_from_checkpoint(
      {.directory_path = temporary.path().string()}, {.repair_incomplete_final_tail = true},
      checkpoint, repaired_sink);
  ASSERT_TRUE(repaired.has_value()) << repaired.error().to_string();
  EXPECT_EQ(repaired_sink.replay_sequences, (std::vector<std::uint64_t>{2U, 3U}));
  EXPECT_EQ(repaired->written_record_sequence(), 3U);
  EXPECT_EQ(repaired->written_position().byte_offset, kSegmentHeaderSize + 64U);
  EXPECT_TRUE(repaired->close().is_ok());
  EXPECT_EQ(std::filesystem::file_size(temporary.path() / "wal-00000000000000000003.cwal"),
            kSegmentHeaderSize + 64U);
}

} // namespace
} // namespace chronos::wal
