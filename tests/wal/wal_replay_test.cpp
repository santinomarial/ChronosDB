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
  test::create_wal(temporary.path(), 3U, kSegmentHeaderSize + 64U);
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

} // namespace
} // namespace chronos::wal
