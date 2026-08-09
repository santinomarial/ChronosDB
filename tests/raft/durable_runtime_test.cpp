#include "chronos/common/status.hpp"
#include "chronos/raft/durable_runtime.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-durable-raft-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

[[nodiscard]] GroupId group_id(const std::byte seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return GroupId{bytes};
}

TEST(DurableMultiRaftRuntimeTest, BatchesGroupsBehindOneDurableFrontierAndReopens) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const GroupId first = group_id(std::byte{1U});
  const GroupId second = group_id(std::byte{2U});
  std::vector<RaftGroupConfiguration> groups{{first, {1U}}, {second, {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();

  std::vector<DurableRaftRequest> elections;
  elections.push_back({first, StartElectionOperation{}});
  elections.push_back({second, StartElectionOperation{}});
  auto elected = runtime->execute_batch(std::move(elections));
  ASSERT_TRUE(elected.has_value()) << elected.error().to_string();
  ASSERT_EQ(elected->size(), 2U);
  EXPECT_TRUE((*elected)[0].status.is_ok());
  EXPECT_TRUE((*elected)[1].status.is_ok());
  EXPECT_EQ(runtime->durable_physical_sequence(), 2U);
  EXPECT_EQ(runtime->find_group(first)->role(), Role::kLeader);
  EXPECT_EQ(runtime->find_group(second)->role(), Role::kLeader);

  std::vector<DurableRaftRequest> proposals;
  proposals.push_back({first, ProposeOperation{1U, {std::byte{0x11U}}}});
  proposals.push_back({second, ProposeOperation{1U, {std::byte{0x22U}}}});
  auto proposed = runtime->execute_batch(std::move(proposals));
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  EXPECT_EQ(runtime->durable_physical_sequence(), 4U);
  EXPECT_EQ(runtime->find_group(first)->commit_index(), 1U);
  EXPECT_EQ(runtime->find_group(second)->commit_index(), 1U);

  auto applied = runtime->execute_batch(
      {{first, MarkAppliedOperation{1U}}, {second, MarkAppliedOperation{1U}}});
  ASSERT_TRUE(applied.has_value()) << applied.error().to_string();
  EXPECT_EQ(runtime->durable_physical_sequence(), 6U);
  ASSERT_TRUE(runtime->close().is_ok());

  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(first)->commit_index(), 1U);
  EXPECT_EQ(reopened->find_group(first)->applied_index(), 1U);
  EXPECT_EQ(reopened->find_group(second)->commit_index(), 1U);
  EXPECT_EQ(reopened->find_group(second)->applied_index(), 1U);
  EXPECT_EQ(reopened->durable_physical_sequence(), 6U);
}

TEST(DurableMultiRaftRuntimeTest, ReturnsVoteMessagesOnlyAfterStateIsDurable) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const GroupId group = group_id(std::byte{3U});
  const std::vector<RaftGroupConfiguration> groups{{group, {1U, 2U, 3U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value());

  auto result = runtime->execute_batch({{group, StartElectionOperation{}}});

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 1U);
  ASSERT_TRUE(result->front().transition.has_value());
  EXPECT_EQ(result->front().transition->outbound.size(), 2U);
  EXPECT_EQ(runtime->durable_physical_sequence(), 1U);
  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group)->current_term(), 1U);
  EXPECT_EQ(reopened->find_group(group)->persistent_state().voted_for, 1U);
}

TEST(DurableMultiRaftRuntimeTest, RejectsRecoveredGroupWithoutMembershipConfiguration) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const GroupId group = group_id(std::byte{4U});
  auto runtime = DurableMultiRaftRuntime::create_new(
      1U, log_config, std::vector<RaftGroupConfiguration>{{group, {1U}}});
  ASSERT_TRUE(runtime.has_value());
  ASSERT_TRUE(runtime->execute_batch({{group, StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(runtime->close().is_ok());

  auto missing = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, {});

  ASSERT_FALSE(missing.has_value());
  EXPECT_EQ(missing.error().code(), common::StatusCode::kCorruption);
}

} // namespace
} // namespace chronos::raft
