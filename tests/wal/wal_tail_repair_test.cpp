#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_scan.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_recovery_test_support.hpp"

#include <array>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::wal {
namespace {

TEST(WalTailRepairTest, RequiresExplicitAuthorizationAndLeavesBytesUntouchedOtherwise) {
  test::TemporaryDirectory temporary{"chronos-wal-repair-auth"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 1U);
  const std::filesystem::path segment = temporary.path() / "wal-00000000000000000001.cwal";
  const std::array<std::byte, 11> tail{std::byte{0x44}};
  test::append_bytes(segment, tail);
  const auto original_size = std::filesystem::file_size(segment);
  test::CollectingReplaySink sink;

  const common::Result<WalWriter> opened =
      WalWriter::open_existing({.directory_path = temporary.path().string()}, {}, sink);
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().code(), common::StatusCode::kOutOfRange);
  EXPECT_EQ(std::filesystem::file_size(segment), original_size);
  EXPECT_TRUE(sink.preflight_sequences.empty());
  EXPECT_TRUE(sink.replay_sequences.empty());
}

TEST(WalTailRepairTest, TruncatesOnlyToVerifiedEndSynchronizesAndIsIdempotent) {
  test::TemporaryDirectory temporary{"chronos-wal-repair-idempotent"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 2U);
  const std::filesystem::path segment = temporary.path() / "wal-00000000000000000001.cwal";
  const std::array<std::byte, 17> tail{std::byte{0xaa}, std::byte{0xbb}};
  test::append_bytes(segment, tail);
  const auto original_size = std::filesystem::file_size(segment);
  test::CollectingReplaySink first_sink;

  const common::Result<WalRecoveryReport> repaired =
      recover_wal({.directory_path = temporary.path().string()},
                  {.repair_incomplete_final_tail = true}, first_sink);
  ASSERT_TRUE(repaired.has_value()) << repaired.error().to_string();
  EXPECT_TRUE(repaired->repaired);
  EXPECT_EQ(repaired->repair_original_size, original_size);
  EXPECT_EQ(repaired->repair_new_size, kSegmentHeaderSize + (2U * 64U));
  EXPECT_EQ(std::filesystem::file_size(segment), repaired->repair_new_size);
  EXPECT_EQ(first_sink.replay_sequences, (std::vector<std::uint64_t>{1U, 2U}));

  test::CollectingReplaySink second_sink;
  const common::Result<WalRecoveryReport> repeated =
      recover_wal({.directory_path = temporary.path().string()},
                  {.repair_incomplete_final_tail = true}, second_sink);
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_FALSE(repeated->repaired);
  EXPECT_EQ(*repeated, *scan_wal(temporary.path().string()));
  EXPECT_EQ(second_sink.replay_sequences, first_sink.replay_sequences);
}

TEST(WalTailRepairTest, NeverRepairsACompleteChecksumInvalidRecord) {
  test::TemporaryDirectory temporary{"chronos-wal-no-crc-repair"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(), 1U);
  const std::filesystem::path segment = temporary.path() / "wal-00000000000000000001.cwal";
  std::vector<std::byte> bytes = test::read_file(segment);
  bytes.back() ^= std::byte{0x01};
  test::write_file(segment, bytes);
  const auto original_size = std::filesystem::file_size(segment);
  test::CollectingReplaySink sink;

  const common::Result<WalRecoveryReport> result = recover_wal(
      {.directory_path = temporary.path().string()}, {.repair_incomplete_final_tail = true}, sink);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kCorruption);
  EXPECT_EQ(std::filesystem::file_size(segment), original_size);
  EXPECT_TRUE(sink.replay_sequences.empty());
}

} // namespace
} // namespace chronos::wal
