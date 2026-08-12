#include "chronos/runtime/database_bootstrap.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>

namespace chronos::runtime {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-bootstrap-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] DatabaseBootstrapDescriptor descriptor(const std::uint8_t seed = 1U) {
  return {.database_id = uuid(seed),
          .metadata_group_id = uuid(static_cast<std::uint8_t>(seed + 1U)),
          .local_node_id = 7U,
          .mutable_head_rows = 4096U,
          .maximum_sealed_generations = 8U,
          .variable_column_bytes = 4U * 1024U * 1024U,
          .maximum_retry_entries = 65'536U,
          .wal_segment_target_bytes = 16U * 1024U * 1024U,
          .raft_segment_target_bytes = 16U * 1024U * 1024U};
}

TEST(DatabaseBootstrapCodecTest, RoundTripsExactChecksummedImage) {
  const auto expected = descriptor();
  auto encoded = encode_database_bootstrap_v1(expected);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
  ASSERT_EQ(encoded->size(), kDatabaseBootstrapV1Size);
  auto decoded = decode_database_bootstrap_v1(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(*decoded, expected);

  encoded->at(60U) ^= std::byte{1U};
  EXPECT_EQ(decode_database_bootstrap_v1(*encoded).error().code(), common::StatusCode::kCorruption);
  encoded->pop_back();
  EXPECT_EQ(decode_database_bootstrap_v1(*encoded).error().code(), common::StatusCode::kCorruption);
}

TEST(DatabaseBootstrapTest, CreatesReopensAndLocksOneDurableRoot) {
  TemporaryDirectory directory;
  const DatabaseBootstrapConfig config{.database_root = directory.path().string(),
                                       .new_database = descriptor()};
  auto created = DatabaseBootstrap::open_or_create(config);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  EXPECT_EQ(created->descriptor(), descriptor());
  EXPECT_TRUE(std::filesystem::is_regular_file(directory.path() / kDatabaseBootstrapFileName));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / kDatabaseBootstrapTemporaryFileName));
  EXPECT_TRUE(std::filesystem::is_directory(directory.path() / kDatabaseWalDirectoryName));
  EXPECT_TRUE(std::filesystem::is_directory(directory.path() / kDatabaseRaftDirectoryName));

  auto locked = DatabaseBootstrap::open_or_create(config);
  ASSERT_FALSE(locked.has_value());
  EXPECT_EQ(locked.error().code(), common::StatusCode::kUnavailable);
  ASSERT_TRUE(created->close().is_ok());

  auto reopened = DatabaseBootstrap::open_or_create(
      {.database_root = directory.path().string(), .new_database = descriptor(10U)});
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->descriptor(), descriptor());
}

TEST(DatabaseBootstrapTest, ResumesTheExactSynchronizedIntentIdentity) {
  TemporaryDirectory directory;
  const auto expected = descriptor();
  auto encoded = encode_database_bootstrap_v1(expected);
  ASSERT_TRUE(encoded.has_value());
  {
    std::ofstream output(directory.path() / kDatabaseBootstrapTemporaryFileName,
                         std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(encoded->data()),
                 static_cast<std::streamsize>(encoded->size()));
  }
  ASSERT_TRUE(std::filesystem::create_directory(directory.path() / kDatabaseWalDirectoryName));

  auto resumed = DatabaseBootstrap::open_or_create(
      {.database_root = directory.path().string(), .new_database = descriptor(10U)});
  ASSERT_TRUE(resumed.has_value()) << resumed.error().to_string();
  EXPECT_EQ(resumed->descriptor(), expected);
  EXPECT_TRUE(std::filesystem::is_directory(directory.path() / kDatabaseRaftDirectoryName));
  EXPECT_TRUE(std::filesystem::is_regular_file(directory.path() / kDatabaseBootstrapFileName));
  EXPECT_FALSE(std::filesystem::exists(directory.path() / kDatabaseBootstrapTemporaryFileName));
}

TEST(DatabaseBootstrapTest, RejectsUnknownStateDuringIncompleteCreation) {
  TemporaryDirectory directory;
  std::ofstream(directory.path() / "unowned").put('x');
  auto opened = DatabaseBootstrap::open_or_create(
      {.database_root = directory.path().string(), .new_database = descriptor()});
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().code(), common::StatusCode::kUnavailable);
}

} // namespace
} // namespace chronos::runtime
