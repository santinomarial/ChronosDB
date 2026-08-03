#include "wal/wal_crash_test_support.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <utility>

namespace chronos::wal {
namespace {

struct CorruptionPoint {
  std::string_view name;
  std::uint64_t segment;
  std::uint64_t offset;
};

class WalCorruptionMatrixTest : public ::testing::TestWithParam<CorruptionPoint> {};

void create_rotated_crash_fixture(const std::filesystem::path& directory) {
  constexpr std::uint64_t kOneRecordSegmentSize =
      kSegmentHeaderSize + static_cast<std::uint64_t>(72U);
  common::Result<test::CrashChildProcess> spawned = test::CrashChildProcess::spawn({
      .directory = directory,
      .target_segment_size = kOneRecordSegmentSize,
      .maximum_sync_batch_requests = 1U,
  });
  ASSERT_TRUE(spawned.has_value()) << spawned.error().to_string();
  test::CrashChildProcess child = std::move(*spawned);
  ASSERT_TRUE(child.wait_for("READY").has_value());
  for (std::uint64_t id = 701U; id <= 703U; ++id) {
    ASSERT_TRUE(child.send("SUBMIT " + std::to_string(id) + " LOCAL_SYNC").is_ok());
    ASSERT_TRUE(child.wait_for("ADMITTED", id).has_value());
    ASSERT_TRUE(child.wait_for("COMPLETED", id).has_value());
  }
  ASSERT_TRUE(child.send("SHUTDOWN").is_ok());
  ASSERT_TRUE(child.wait_for("SHUTDOWN").has_value());
  ASSERT_TRUE(child.wait_for_exit().has_value());
}

TEST_P(WalCorruptionMatrixTest, StrictRecoverySurfacesDamageAndNeverRepairsIt) {
  test::CrashWalDirectory directory{"chronos-wal-corruption-matrix"};
  ASSERT_TRUE(directory.valid());
  create_rotated_crash_fixture(directory.path());
  const CorruptionPoint point = GetParam();
  const common::Result<std::string> name = wal_segment_file_name(point.segment);
  ASSERT_TRUE(name.has_value()) << name.error().to_string();
  test::flip_crash_file_byte(directory.path() / *name, point.offset);
  const std::vector<test::CrashFileImage> corrupt_image =
      test::snapshot_crash_wal(directory.path());

  const common::Result<WalRecoveryReport> scanned = scan_wal(directory.path().string());
  ASSERT_FALSE(scanned.has_value()) << point.name;
  EXPECT_EQ(scanned.error().code(), common::StatusCode::kCorruption) << point.name;
  const common::Result<test::CrashRecoveryResult> recovered =
      test::recover_crash_wal(directory.path(), true);
  ASSERT_FALSE(recovered.has_value()) << point.name;
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption) << point.name;
  EXPECT_EQ(test::snapshot_crash_wal(directory.path()), corrupt_image) << point.name;
}

INSTANTIATE_TEST_SUITE_P(
    HeaderPayloadAndTrailer, WalCorruptionMatrixTest,
    ::testing::Values(
        CorruptionPoint{.name = "middle segment header CRC", .segment = 2U, .offset = 60U},
        CorruptionPoint{.name = "first record framing", .segment = 1U, .offset = 72U},
        CorruptionPoint{.name = "middle record payload", .segment = 2U, .offset = 124U},
        CorruptionPoint{.name = "complete final record CRC", .segment = 3U, .offset = 135U}));

TEST(WalCorruptionMatrixTest, MissingMiddleSegmentIsNeverSkippedOrRecreated) {
  test::CrashWalDirectory directory{"chronos-wal-corruption-gap"};
  ASSERT_TRUE(directory.valid());
  create_rotated_crash_fixture(directory.path());
  const common::Result<std::string> middle_name = wal_segment_file_name(2U);
  ASSERT_TRUE(middle_name.has_value());
  std::error_code error;
  ASSERT_TRUE(std::filesystem::remove(directory.path() / *middle_name, error));
  ASSERT_FALSE(error);

  const common::Result<WalRecoveryReport> scanned = scan_wal(directory.path().string());
  ASSERT_FALSE(scanned.has_value());
  EXPECT_EQ(scanned.error().code(), common::StatusCode::kCorruption);
  const common::Result<test::CrashRecoveryResult> recovered =
      test::recover_crash_wal(directory.path(), true);
  ASSERT_FALSE(recovered.has_value());
  EXPECT_EQ(recovered.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::wal
