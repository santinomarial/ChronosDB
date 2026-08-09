#include "chronos/common/status.hpp"
#include "chronos/raft/durable_runtime.hpp"

#include <algorithm>
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

TEST(DurableMultiRaftRuntimeTest, ProvesQuorumSyncOnlyAfterDurableMajorityCommit) {
  TemporaryDirectory leader_directory;
  TemporaryDirectory follower_directory;
  TemporaryDirectory other_directory;
  const GroupId group = group_id(std::byte{8U});
  const std::vector<RaftGroupConfiguration> groups{{group, {1U, 2U, 3U}}};
  auto leader = DurableMultiRaftRuntime::create_new(
      1U, {.directory_path = leader_directory.path().string()}, groups);
  auto follower = DurableMultiRaftRuntime::create_new(
      2U, {.directory_path = follower_directory.path().string()}, groups);
  auto other = DurableMultiRaftRuntime::create_new(
      3U, {.directory_path = other_directory.path().string()}, groups);
  ASSERT_TRUE(leader.has_value());
  ASSERT_TRUE(follower.has_value());
  ASSERT_TRUE(other.has_value());

  auto election = leader->execute_batch({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->front().transition.has_value());
  const auto vote_request = std::ranges::find_if(
      election->front().transition->outbound,
      [](const GroupOutboundMessage& message) { return message.outbound.destination == 2U; });
  ASSERT_NE(vote_request, election->front().transition->outbound.end());
  auto voted = follower->execute_batch(
      {{group, ReceiveOperation{vote_request->source, vote_request->outbound.message}}});
  ASSERT_TRUE(voted.has_value());
  ASSERT_TRUE(voted->front().transition.has_value());
  ASSERT_EQ(voted->front().transition->outbound.size(), 1U);
  const GroupOutboundMessage vote_response = voted->front().transition->outbound.front();
  ASSERT_GT(follower->durable_physical_sequence(), 0U);
  auto elected = leader->execute_batch(
      {{group, ReceiveOperation{vote_response.source, vote_response.outbound.message}}});
  ASSERT_TRUE(elected.has_value());
  ASSERT_EQ(leader->find_group(group)->role(), Role::kLeader);

  auto proposed = leader->execute_batch({{group, ProposeOperation{1U, {std::byte{0x42U}}}}});
  ASSERT_TRUE(proposed.has_value());
  ASSERT_TRUE(proposed->front().transition.has_value());
  EXPECT_EQ(leader->find_group(group)->commit_index(), 0U);
  EXPECT_EQ(leader->prove_quorum_sync(group, 1U).error().code(), common::StatusCode::kUnavailable);
  const auto append_request = std::ranges::find_if(
      proposed->front().transition->outbound,
      [](const GroupOutboundMessage& message) { return message.outbound.destination == 2U; });
  ASSERT_NE(append_request, proposed->front().transition->outbound.end());

  const std::uint64_t follower_before = follower->durable_physical_sequence();
  auto appended = follower->execute_batch(
      {{group, ReceiveOperation{append_request->source, append_request->outbound.message}}});
  ASSERT_TRUE(appended.has_value());
  ASSERT_TRUE(appended->front().transition.has_value());
  ASSERT_EQ(appended->front().transition->outbound.size(), 1U);
  EXPECT_GT(follower->durable_physical_sequence(), follower_before);
  const GroupOutboundMessage append_response = appended->front().transition->outbound.front();

  auto committed = leader->execute_batch(
      {{group, ReceiveOperation{append_response.source, append_response.outbound.message}}});
  ASSERT_TRUE(committed.has_value()) << committed.error().to_string();
  ASSERT_EQ(leader->find_group(group)->commit_index(), 1U);
  auto receipt = leader->prove_quorum_sync(group, 1U);
  ASSERT_TRUE(receipt.has_value()) << receipt.error().to_string();
  EXPECT_EQ(receipt->group_id, group);
  EXPECT_EQ(receipt->leader_node_id, 1U);
  EXPECT_EQ(receipt->leader_term, 1U);
  EXPECT_EQ(receipt->entry_term, 1U);
  EXPECT_EQ(receipt->log_index, 1U);
  EXPECT_EQ(receipt->local_durable_physical_sequence, leader->durable_physical_sequence());
  EXPECT_EQ(follower->prove_quorum_sync(group, 1U).error().code(),
            common::StatusCode::kUnavailable);
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
