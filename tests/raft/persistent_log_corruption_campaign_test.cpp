#include "chronos/raft/persistent_log.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

class CorruptionDirectory {
public:
  CorruptionDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-corruption-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }

  ~CorruptionDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  CorruptionDirectory(const CorruptionDirectory&) = delete;
  CorruptionDirectory& operator=(const CorruptionDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

struct FileImage {
  std::string name;
  std::vector<char> bytes;

  [[nodiscard]] bool operator==(const FileImage&) const = default;
};

[[nodiscard]] GroupId campaign_group_id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

[[nodiscard]] GroupPersistentState campaign_state(const GroupId group, const std::uint64_t sequence,
                                                  const std::byte value) {
  PersistentState persistent{};
  persistent.current_term = 1U;
  persistent.log.push_back(LogEntry{1U, 1U, 1U, {value}});
  persistent.commit_index = 1U;
  return GroupPersistentState{group, sequence, std::move(persistent)};
}

[[nodiscard]] std::vector<char> read_file(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  EXPECT_TRUE(input.is_open()) << path;
  return {std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

[[nodiscard]] std::vector<FileImage> snapshot_directory(const std::filesystem::path& directory) {
  std::vector<FileImage> images;
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator(directory)) {
    EXPECT_TRUE(entry.is_regular_file()) << entry.path();
    images.push_back(FileImage{entry.path().filename().string(), read_file(entry.path())});
  }
  std::ranges::sort(images, {}, &FileImage::name);
  return images;
}

void write_file(const std::filesystem::path& path, const std::vector<char>& bytes) {
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(output.is_open()) << path;
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(output.good()) << path;
  output.close();
  ASSERT_TRUE(output.good()) << path;
}

void flip_low_bit(const std::filesystem::path& path, const std::uint64_t offset) {
  std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(file.is_open()) << path;
  file.seekg(static_cast<std::streamoff>(offset));
  char value{};
  file.read(&value, 1);
  ASSERT_EQ(file.gcount(), 1) << path << " offset " << offset;
  value = static_cast<char>(static_cast<unsigned char>(value) ^ 0x01U);
  file.seekp(static_cast<std::streamoff>(offset));
  file.write(&value, 1);
  ASSERT_TRUE(file.good()) << path << " offset " << offset;
}

void create_anchored_fixture(const std::filesystem::path& directory,
                             const RaftPersistentLogConfig& config) {
  auto log = RaftPersistentLog::create_new(config);
  ASSERT_TRUE(log.has_value()) << log.error().to_string();
  const GroupId first = campaign_group_id(std::byte{0x31U});
  const GroupId second = campaign_group_id(std::byte{0x32U});
  ASSERT_TRUE(log->append(campaign_state(first, 1U, std::byte{0xA1U})).has_value());
  ASSERT_TRUE(log->append(campaign_state(second, 2U, std::byte{0xA2U})).has_value());
  ASSERT_TRUE(log->synchronize().has_value());
  const std::vector checkpoint{
      campaign_state(first, 3U, std::byte{0xB1U}),
      campaign_state(second, 4U, std::byte{0xB2U}),
  };
  auto reclaimed = log->checkpoint_and_reclaim(checkpoint);
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->base_segment_number, 3U);
  EXPECT_EQ(reclaimed->checkpoint_first_physical_sequence, 3U);
  EXPECT_EQ(reclaimed->checkpoint_last_physical_sequence, 4U);
  ASSERT_TRUE(log->close().is_ok());

  const std::vector<FileImage> image = snapshot_directory(directory);
  ASSERT_EQ(image.size(), 4U);
  EXPECT_EQ(image[0].name, "LOCK");
  EXPECT_EQ(image[1].name, "raft-00000000000000000003.rlog");
  EXPECT_EQ(image[1].bytes.size(), 277U);
  EXPECT_EQ(image[2].name, "raft-00000000000000000004.rlog");
  EXPECT_EQ(image[2].bytes.size(), 277U);
  EXPECT_EQ(image[3].name, "raft-base-00000000000000000003.rbase");
  EXPECT_EQ(image[3].bytes.size(), 64U);
}

void expect_corruption_without_mutation(const RaftPersistentLogConfig& config,
                                        const std::vector<FileImage>& corrupt_image) {
  for (const bool repair : {false, true}) {
    SCOPED_TRACE(repair ? "repair-authorized open" : "strict open");
    auto opened = RaftPersistentLog::open_existing(
        config, RaftPersistentLogOpenOptions{.repair_incomplete_final_tail = repair});
    ASSERT_FALSE(opened.has_value());
    EXPECT_EQ(opened.error().code(), common::StatusCode::kCorruption);
    EXPECT_EQ(snapshot_directory(config.directory_path), corrupt_image);
  }
}

[[nodiscard]] std::vector<FileImage> authority_files(const std::vector<FileImage>& complete_image) {
  std::vector<FileImage> authority;
  std::ranges::copy_if(complete_image, std::back_inserter(authority), [](const FileImage& file) {
    return file.name.ends_with(".rlog") || file.name.ends_with(".rbase");
  });
  return authority;
}

void expect_clean_recovery(const RaftPersistentLogConfig& config) {
  auto opened = RaftPersistentLog::open_existing(config);
  ASSERT_TRUE(opened.has_value()) << opened.error().to_string();
  EXPECT_EQ(opened->recovery().base_segment_number, 3U);
  EXPECT_EQ(opened->recovery().segment_count, 2U);
  EXPECT_EQ(opened->recovery().record_count, 2U);
  EXPECT_EQ(opened->recovery().durable_physical_sequence, 4U);
  ASSERT_EQ(opened->recovery().latest_group_states.size(), 2U);
  EXPECT_EQ(opened->recovery().latest_group_states[0],
            campaign_state(campaign_group_id(std::byte{0x31U}), 3U, std::byte{0xB1U}));
  EXPECT_EQ(opened->recovery().latest_group_states[1],
            campaign_state(campaign_group_id(std::byte{0x32U}), 4U, std::byte{0xB2U}));
  EXPECT_TRUE(opened->close().is_ok());
}

TEST(RaftPersistentLogCorruptionCampaignTest,
     EveryAuthorityByteRejectsSingleBitCorruptionWithoutRepairOrMutation) {
  CorruptionDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 300U};
  create_anchored_fixture(directory.path(), config);
  const std::vector<FileImage> complete_image = snapshot_directory(directory.path());

  for (const FileImage& file : authority_files(complete_image)) {
    for (std::uint64_t offset = 0U; offset < file.bytes.size(); ++offset) {
      SCOPED_TRACE(file.name);
      SCOPED_TRACE(offset);
      const std::filesystem::path path = directory.path() / file.name;
      flip_low_bit(path, offset);
      const std::vector<FileImage> corrupt_image = snapshot_directory(directory.path());
      expect_corruption_without_mutation(config, corrupt_image);
      flip_low_bit(path, offset);
      ASSERT_EQ(snapshot_directory(directory.path()), complete_image);
    }
  }

  expect_clean_recovery(config);
}

TEST(RaftPersistentLogCorruptionCampaignTest,
     EveryAuthorityTruncationRejectsRepairAndPreservesTheDamagedImage) {
  CorruptionDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 300U};
  create_anchored_fixture(directory.path(), config);
  const std::vector<FileImage> complete_image = snapshot_directory(directory.path());

  for (const FileImage& file : authority_files(complete_image)) {
    for (std::uint64_t size = 0U; size < file.bytes.size(); ++size) {
      SCOPED_TRACE(file.name);
      SCOPED_TRACE(size);
      const std::filesystem::path path = directory.path() / file.name;
      std::filesystem::resize_file(path, size);
      const std::vector<FileImage> corrupt_image = snapshot_directory(directory.path());
      expect_corruption_without_mutation(config, corrupt_image);
      write_file(path, file.bytes);
      ASSERT_EQ(snapshot_directory(directory.path()), complete_image);
    }
  }

  expect_clean_recovery(config);
}

TEST(RaftPersistentLogCorruptionCampaignTest,
     EveryMissingAuthorityArtifactFailsClosedWithoutChangingRemainingFiles) {
  CorruptionDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const RaftPersistentLogConfig config{.directory_path = directory.path().string(),
                                       .target_segment_size = 300U};
  create_anchored_fixture(directory.path(), config);
  const std::vector<FileImage> complete_image = snapshot_directory(directory.path());

  for (const FileImage& file : authority_files(complete_image)) {
    SCOPED_TRACE(file.name);
    const std::filesystem::path path = directory.path() / file.name;
    ASSERT_TRUE(std::filesystem::remove(path));
    const std::vector<FileImage> corrupt_image = snapshot_directory(directory.path());
    expect_corruption_without_mutation(config, corrupt_image);
    write_file(path, file.bytes);
    ASSERT_EQ(snapshot_directory(directory.path()), complete_image);
  }

  expect_clean_recovery(config);
}

} // namespace
} // namespace chronos::raft
