#include "chronos/wal/wal_recovery.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace chronos::wal {
namespace {

class TypeAwareSink final : public WalReplaySink {
public:
  common::Status preflight(const WalReplayRecord& record) override {
    preflighted.push_back(record.header.record_sequence);
    if (record.header.record_type != kApplicationEntryRecordType) {
      return common::Status{common::StatusCode::kNotSupported, "unsupported physical record type"};
    }
    return common::Status::ok();
  }

  common::Status replay(const WalReplayRecord& record) override {
    replayed.push_back(record.header.record_sequence);
    return common::Status::ok();
  }

  std::vector<std::uint64_t> preflighted;
  std::vector<std::uint64_t> replayed;
};

TEST(WalReplayTest, ReplaysDeterministicallyInGlobalSequenceOrderAcrossSegments) {
  test::TemporaryDirectory temporary{"chronos-wal-replay-order"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 3U, .target_segment_size = kSegmentHeaderSize + 64U});
  test::CollectingReplaySink sink;

  const common::Result<WalRecoveryReport> report = inspect_wal(temporary.path().string(), sink);
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(sink.preflight_sequences, (std::vector<std::uint64_t>{1U, 2U, 3U}));
  EXPECT_EQ(sink.replay_sequences, sink.preflight_sequences);
  ASSERT_EQ(sink.payloads.size(), 3U);
  EXPECT_EQ(sink.payloads.front(), test::make_application_payload());
}

TEST(WalReplayTest, PhysicalCorruptionAnywherePreventsEverySinkCallback) {
  test::TemporaryDirectory temporary{"chronos-wal-replay-corruption"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 2U);
  const std::filesystem::path segment = temporary.path() / "wal-00000000000000000001.cwal";
  std::vector<std::byte> bytes = test::read_file(segment);
  ASSERT_GT(bytes.size(), kSegmentHeaderSize + 64U + kRecordHeaderSize);
  bytes[kSegmentHeaderSize + 64U + kRecordHeaderSize] ^= std::byte{0x01};
  test::write_file(segment, bytes);
  test::CollectingReplaySink sink;

  const common::Result<WalRecoveryReport> report = inspect_wal(temporary.path().string(), sink);
  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(sink.preflight_sequences.empty());
  EXPECT_TRUE(sink.replay_sequences.empty());
}

TEST(WalReplayTest, UnsupportedSemanticPreflightRejectsBeforeReplay) {
  test::TemporaryDirectory temporary{"chronos-wal-replay-unsupported"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 0U);
  const std::vector<std::byte> first =
      test::encode_physical_record(1U, kApplicationEntryRecordType);
  const std::vector<std::byte> second = test::encode_physical_record(2U, 2U, {});
  const std::filesystem::path segment = temporary.path() / "wal-00000000000000000001.cwal";
  test::append_bytes(segment, first);
  test::append_bytes(segment, second);
  TypeAwareSink sink;

  const common::Result<WalRecoveryReport> report = inspect_wal(temporary.path().string(), sink);
  ASSERT_FALSE(report.has_value());
  EXPECT_EQ(report.error().code(), common::StatusCode::kNotSupported);
  EXPECT_EQ(sink.preflighted, (std::vector<std::uint64_t>{1U, 2U}));
  EXPECT_TRUE(sink.replayed.empty());
}

TEST(WalReplayTest, CheckpointSuffixSkipsCoveredRecordsAndAllowsRemovedPrefixSegments) {
  test::TemporaryDirectory temporary{"chronos-wal-replay-checkpoint"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 3U, .target_segment_size = kSegmentHeaderSize + 64U});
  const WalReplayCheckpoint checkpoint{.wal_id = test::make_wal_id(),
                                       .record_sequence = 2U,
                                       .segment_number = 2U,
                                       .byte_offset = kSegmentHeaderSize + 64U};
  test::CollectingReplaySink complete;
  common::Result<WalRecoveryReport> report =
      inspect_wal_suffix(temporary.path().string(), checkpoint, complete);
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(complete.preflight_sequences, (std::vector<std::uint64_t>{3U}));
  EXPECT_EQ(complete.replay_sequences, complete.preflight_sequences);
  EXPECT_EQ(report->segment_count, 2U);

  ASSERT_TRUE(std::filesystem::remove(temporary.path() / "wal-00000000000000000001.cwal"));
  test::CollectingReplaySink missing_prefix;
  report = inspect_wal_suffix(temporary.path().string(), checkpoint, missing_prefix);
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(missing_prefix.replay_sequences, (std::vector<std::uint64_t>{3U}));

  ASSERT_TRUE(std::filesystem::remove(temporary.path() / "wal-00000000000000000002.cwal"));
  test::CollectingReplaySink missing_coordinate;
  report = inspect_wal_suffix(temporary.path().string(), checkpoint, missing_coordinate);
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(missing_coordinate.replay_sequences, (std::vector<std::uint64_t>{3U}));
  EXPECT_EQ(report->segment_count, 1U);
}

TEST(WalReplayTest, EmptyCheckpointReplaysTheCompleteExistingHistory) {
  test::TemporaryDirectory temporary{"chronos-wal-replay-empty-checkpoint"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 2U);
  test::CollectingReplaySink sink;

  const common::Result<WalRecoveryReport> report =
      inspect_wal_suffix(temporary.path().string(),
                         {.wal_id = test::make_wal_id(),
                          .record_sequence = 0U,
                          .segment_number = kFirstSegmentNumber,
                          .byte_offset = kSegmentHeaderSize},
                         sink);
  ASSERT_TRUE(report.has_value()) << report.error().to_string();
  EXPECT_EQ(sink.preflight_sequences, (std::vector<std::uint64_t>{1U, 2U}));
  EXPECT_EQ(sink.replay_sequences, sink.preflight_sequences);
}

TEST(WalReplayTest, CheckpointSuffixRejectsBadBoundaryMissingSuffixAndCorruptCoveredHeader) {
  test::TemporaryDirectory boundary{"chronos-wal-replay-checkpoint-boundary"};
  ASSERT_TRUE(boundary.valid());
  test::create_wal(boundary.path(),
                   {.record_count = 3U, .target_segment_size = kSegmentHeaderSize + 64U});
  test::CollectingReplaySink bad_boundary;
  EXPECT_EQ(inspect_wal_suffix(boundary.path().string(),
                               {.wal_id = test::make_wal_id(),
                                .record_sequence = 2U,
                                .segment_number = 2U,
                                .byte_offset = kSegmentHeaderSize + 8U},
                               bad_boundary)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(bad_boundary.preflight_sequences.empty());

  WalId wrong_wal_id = test::make_wal_id();
  wrong_wal_id.bytes.front() ^= std::byte{0x01U};
  test::CollectingReplaySink wrong_identity;
  EXPECT_EQ(inspect_wal_suffix(boundary.path().string(),
                               {.wal_id = wrong_wal_id,
                                .record_sequence = 2U,
                                .segment_number = 2U,
                                .byte_offset = kSegmentHeaderSize + 64U},
                               wrong_identity)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(wrong_identity.preflight_sequences.empty());

  ASSERT_TRUE(std::filesystem::remove(boundary.path() / "wal-00000000000000000002.cwal"));
  ASSERT_TRUE(std::filesystem::remove(boundary.path() / "wal-00000000000000000003.cwal"));
  test::CollectingReplaySink missing;
  EXPECT_EQ(inspect_wal_suffix(boundary.path().string(),
                               {.wal_id = test::make_wal_id(),
                                .record_sequence = 2U,
                                .segment_number = 2U,
                                .byte_offset = kSegmentHeaderSize + 64U},
                               missing)
                .error()
                .code(),
            common::StatusCode::kCorruption);

  test::TemporaryDirectory covered{"chronos-wal-replay-checkpoint-covered"};
  ASSERT_TRUE(covered.valid());
  test::create_wal(covered.path(),
                   {.record_count = 3U, .target_segment_size = kSegmentHeaderSize + 64U});
  const std::filesystem::path first = covered.path() / "wal-00000000000000000001.cwal";
  std::vector<std::byte> bytes = test::read_file(first);
  bytes.front() ^= std::byte{0x01U};
  test::write_file(first, bytes);
  test::CollectingReplaySink corrupt;
  EXPECT_EQ(inspect_wal_suffix(covered.path().string(),
                               {.wal_id = test::make_wal_id(),
                                .record_sequence = 2U,
                                .segment_number = 2U,
                                .byte_offset = kSegmentHeaderSize + 64U},
                               corrupt)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(corrupt.preflight_sequences.empty());

  test::TemporaryDirectory gapped{"chronos-wal-replay-checkpoint-gap"};
  ASSERT_TRUE(gapped.valid());
  test::create_wal(gapped.path(),
                   {.record_count = 4U, .target_segment_size = kSegmentHeaderSize + 64U});
  ASSERT_TRUE(std::filesystem::remove(gapped.path() / "wal-00000000000000000003.cwal"));
  test::CollectingReplaySink gap;
  EXPECT_EQ(inspect_wal_suffix(gapped.path().string(),
                               {.wal_id = test::make_wal_id(),
                                .record_sequence = 1U,
                                .segment_number = 1U,
                                .byte_offset = kSegmentHeaderSize + 64U},
                               gap)
                .error()
                .code(),
            common::StatusCode::kCorruption);
  EXPECT_TRUE(gap.preflight_sequences.empty());
}

} // namespace
} // namespace chronos::wal
