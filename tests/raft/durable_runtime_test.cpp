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
  elections.emplace_back(first, StartElectionOperation{});
  elections.emplace_back(second, StartElectionOperation{});
  auto elected = runtime->execute_batch(std::move(elections));
  ASSERT_TRUE(elected.has_value()) << elected.error().to_string();
  ASSERT_EQ(elected->size(), 2U);
  EXPECT_TRUE((*elected)[0].status.is_ok());
  EXPECT_TRUE((*elected)[1].status.is_ok());
  EXPECT_EQ(runtime->durable_physical_sequence(), 2U);
  EXPECT_EQ(runtime->find_group(first)->role(), Role::kLeader);
  EXPECT_EQ(runtime->find_group(second)->role(), Role::kLeader);

  std::vector<DurableRaftRequest> proposals;
  proposals.emplace_back(first, ProposeOperation{1U, {std::byte{0x11U}}});
  proposals.emplace_back(second, ProposeOperation{1U, {std::byte{0x22U}}});
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

TEST(DurableMultiRaftRuntimeTest, NodeWideCheckpointReclaimsAndContinuesGlobalSequence) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string(),
                                           .target_segment_size = 300U};
  const GroupId first = group_id(std::byte{18U});
  const GroupId second = group_id(std::byte{19U});
  const GroupId never_changed = group_id(std::byte{20U});
  const std::vector<RaftGroupConfiguration> groups{
      {first, {1U}}, {second, {1U}}, {never_changed, {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(
      runtime
          ->execute_batch({{first, StartElectionOperation{}}, {second, StartElectionOperation{}}})
          .has_value());
  ASSERT_EQ(runtime->durable_physical_sequence(), 2U);

  auto reclaimed = runtime->checkpoint_and_reclaim();

  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->checkpoint_first_physical_sequence, 3U);
  EXPECT_EQ(reclaimed->checkpoint_last_physical_sequence, 5U);
  EXPECT_EQ(reclaimed->reclaimed_records, 2U);
  EXPECT_EQ(runtime->durable_physical_sequence(), 5U);
  auto proposed = runtime->execute_batch(
      {{first, ProposeOperation{.type = 1U, .payload = {std::byte{0x55U}}}}});
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  EXPECT_EQ(runtime->durable_physical_sequence(), 6U);
  ASSERT_TRUE(runtime->close().is_ok());

  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->durable_physical_sequence(), 6U);
  EXPECT_EQ(reopened->find_group(first)->commit_index(), 1U);
  EXPECT_EQ(reopened->find_group(second)->current_term(), 1U);
  EXPECT_EQ(reopened->find_group(never_changed)->current_term(), 0U);
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
  const auto& transition = result->front().transition;
  if (!transition.has_value()) {
    ADD_FAILURE() << "expected durable election transition";
    return;
  }
  EXPECT_EQ(transition->outbound.size(), 2U);
  EXPECT_EQ(runtime->durable_physical_sequence(), 1U);
  ASSERT_TRUE(runtime->close().is_ok());
  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group)->current_term(), 1U);
  EXPECT_EQ(reopened->find_group(group)->persistent_state().voted_for, 1U);
}

TEST(DurableMultiRaftRuntimeTest, RejectsBatchOutboundExhaustionBeforeGroupMutation) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{21U});
  DurableMultiRaftLimits limits{};
  limits.maximum_batch_outbound = 3U;
  limits.runtime.raft.maximum_voters = 3U;
  auto runtime = DurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U, 2U, 3U}}}, limits);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();

  auto rejected = runtime->execute_batch(
      {{group, StartElectionOperation{}}, {group, StartElectionOperation{}}});

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_FALSE(runtime->failed());
  EXPECT_EQ(runtime->find_group(group)->role(), Role::kFollower);
  EXPECT_EQ(runtime->find_group(group)->current_term(), 0U);
  EXPECT_FALSE(runtime->find_group(group)->persistent_state().voted_for.has_value());
  EXPECT_EQ(runtime->durable_physical_sequence(), 0U);

  auto admitted = runtime->execute_batch({{group, StartElectionOperation{}}});
  ASSERT_TRUE(admitted.has_value()) << admitted.error().to_string();
  EXPECT_EQ(runtime->find_group(group)->role(), Role::kCandidate);
  EXPECT_EQ(runtime->find_group(group)->current_term(), 1U);
  EXPECT_EQ(runtime->durable_physical_sequence(), 1U);
}

TEST(DurableMultiRaftRuntimeTest, SurfacesGroupReadBarrierWithoutInventingPersistence) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{13U});
  const std::vector<RaftGroupConfiguration> groups{{group, {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group, StartElectionOperation{}}}).has_value());
  ASSERT_TRUE(
      runtime->execute_batch({{group, ProposeOperation{.type = 1U, .payload = {std::byte{0x42U}}}}})
          .has_value());
  const std::uint64_t durable_before = runtime->durable_physical_sequence();

  auto barrier = runtime->execute_batch({{group, BeginReadBarrierOperation{}}});

  ASSERT_TRUE(barrier.has_value()) << barrier.error().to_string();
  ASSERT_EQ(barrier->size(), 1U);
  ASSERT_TRUE(barrier->front().status.is_ok());
  const auto& transition = barrier->front().transition;
  if (!transition.has_value()) {
    ADD_FAILURE() << "expected read-barrier transition";
    return;
  }
  EXPECT_FALSE(transition->persistence.has_value());
  const auto& ready = transition->read_barrier_ready;
  if (!ready.has_value()) {
    ADD_FAILURE() << "expected completed single-voter read barrier";
    return;
  }
  EXPECT_EQ(ready->group_id, group);
  EXPECT_EQ(ready->barrier.read_index, 1U);
  EXPECT_EQ(runtime->find_group(group)->applied_index(), 0U);
  EXPECT_EQ(runtime->durable_physical_sequence(), durable_before);
}

TEST(DurableMultiRaftRuntimeTest, ObservesBoundedGroupStateInBatchOrderWithoutPersistence) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{15U});
  auto runtime = DurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();

  auto result = runtime->execute_batch({{group, StartElectionOperation{}},
                                        {group, BeginMembershipChangeOperation{{1U, 2U}}},
                                        {group, ObserveGroupOperation{}}});

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 3U);
  ASSERT_TRUE((*result)[2].status.is_ok());
  EXPECT_FALSE((*result)[2].transition.has_value());
  const auto& observation = (*result)[2].observation;
  if (!observation.has_value()) {
    ADD_FAILURE() << "expected batch-ordered group observation";
    return;
  }
  const RaftGroupObservation& observed = *observation;
  EXPECT_EQ(observed.group_id, group);
  EXPECT_EQ(observed.node_id, 1U);
  EXPECT_EQ(observed.role, Role::kLeader);
  EXPECT_EQ(observed.current_term, 1U);
  EXPECT_EQ(observed.leader_id, 1U);
  EXPECT_EQ(observed.last_log_index, 1U);
  EXPECT_EQ(observed.commit_index, 0U);
  EXPECT_EQ(observed.applied_index, 0U);
  EXPECT_EQ(observed.voters, (std::vector<NodeId>{1U, 2U}));
  EXPECT_EQ(observed.committed_voters, std::vector<NodeId>{1U});
  EXPECT_EQ(observed.joint_old_voters, std::vector<NodeId>{1U});
  EXPECT_EQ(observed.joint_new_voters, (std::vector<NodeId>{1U, 2U}));
  EXPECT_TRUE(observed.joint_membership_active);
  EXPECT_FALSE(observed.joint_membership_can_finalize);
  EXPECT_FALSE(observed.final_membership_pending);
  EXPECT_EQ(runtime->durable_physical_sequence(), 2U);

  auto missing = runtime->execute_batch({{group_id(std::byte{16U}), ObserveGroupOperation{}}});
  ASSERT_TRUE(missing.has_value()) << missing.error().to_string();
  ASSERT_EQ(missing->size(), 1U);
  EXPECT_EQ(missing->front().status.code(), common::StatusCode::kNotFound);
  EXPECT_FALSE(missing->front().observation.has_value());
  EXPECT_FALSE(runtime->failed());
}

TEST(DurableMultiRaftRuntimeTest, ExactRetainedProposalDoesNotAppendOrSynchronizeTwice) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{14U});
  auto runtime = DurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group, StartElectionOperation{}}}).has_value());
  auto first =
      runtime->execute_batch({{group, ProposeExactRetainedOperation{1U, {std::byte{0x42U}}}}});
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(first->front().status.is_ok());
  const std::uint64_t durable_after_first = runtime->durable_physical_sequence();

  auto retry =
      runtime->execute_batch({{group, ProposeExactRetainedOperation{1U, {std::byte{0x42U}}}}});

  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  ASSERT_TRUE(retry->front().status.is_ok());
  const auto& transition = retry->front().transition;
  if (!transition.has_value()) {
    ADD_FAILURE() << "expected retained-proposal retry transition";
    return;
  }
  EXPECT_FALSE(transition->persistence.has_value());
  EXPECT_TRUE(transition->outbound.empty());
  EXPECT_EQ(runtime->durable_physical_sequence(), durable_after_first);
  EXPECT_EQ(runtime->find_group(group)->persistent_state().log.size(), 1U);
}

TEST(DurableMultiRaftRuntimeTest, RequiredLeaderTermRejectsStaleOrFollowerWorkWithoutMutation) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{17U});
  auto runtime = DurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  const std::uint64_t initial_sequence = runtime->durable_physical_sequence();

  auto follower_rejection =
      runtime->execute_batch({{group, ProposeOperation{1U, {std::byte{0x41U}}}, Term{1U}}});

  ASSERT_TRUE(follower_rejection.has_value()) << follower_rejection.error().to_string();
  ASSERT_EQ(follower_rejection->size(), 1U);
  EXPECT_EQ(follower_rejection->front().status.code(), common::StatusCode::kUnavailable);
  EXPECT_FALSE(follower_rejection->front().transition.has_value());
  EXPECT_TRUE(runtime->find_group(group)->persistent_state().log.empty());
  EXPECT_EQ(runtime->durable_physical_sequence(), initial_sequence);

  ASSERT_TRUE(runtime->execute_batch({{group, StartElectionOperation{}}}).has_value());
  ASSERT_EQ(runtime->find_group(group)->role(), Role::kLeader);
  ASSERT_EQ(runtime->find_group(group)->current_term(), 1U);
  const std::uint64_t elected_sequence = runtime->durable_physical_sequence();

  auto stale_rejection =
      runtime->execute_batch({{group, ProposeOperation{1U, {std::byte{0x42U}}}, Term{2U}}});
  auto invalid_rejection =
      runtime->execute_batch({{group, ProposeOperation{1U, {std::byte{0x43U}}}, Term{0U}}});

  ASSERT_TRUE(stale_rejection.has_value()) << stale_rejection.error().to_string();
  ASSERT_TRUE(invalid_rejection.has_value()) << invalid_rejection.error().to_string();
  EXPECT_EQ(stale_rejection->front().status.code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(invalid_rejection->front().status.code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(runtime->find_group(group)->persistent_state().log.empty());
  EXPECT_EQ(runtime->durable_physical_sequence(), elected_sequence);

  auto admitted =
      runtime->execute_batch({{group, ProposeOperation{1U, {std::byte{0x44U}}}, Term{1U}}});

  ASSERT_TRUE(admitted.has_value()) << admitted.error().to_string();
  ASSERT_TRUE(admitted->front().status.is_ok()) << admitted->front().status.to_string();
  const auto& transition = admitted->front().transition;
  if (!transition.has_value()) {
    ADD_FAILURE() << "expected required-term proposal transition";
    return;
  }
  EXPECT_TRUE(transition->persistence.has_value());
  EXPECT_EQ(runtime->find_group(group)->persistent_state().log.size(), 1U);
  EXPECT_GT(runtime->durable_physical_sequence(), elected_sequence);
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
  const auto& election_transition = election->front().transition;
  if (!election_transition.has_value()) {
    ADD_FAILURE() << "expected election transition";
    return;
  }
  const auto vote_request =
      std::ranges::find_if(election_transition->outbound, [](const GroupOutboundMessage& message) {
        return message.outbound.destination == 2U;
      });
  ASSERT_NE(vote_request, election_transition->outbound.end());
  auto voted = follower->execute_batch(
      {{group, ReceiveOperation{vote_request->source, vote_request->outbound.message}}});
  ASSERT_TRUE(voted.has_value());
  const auto& vote_transition = voted->front().transition;
  if (!vote_transition.has_value()) {
    ADD_FAILURE() << "expected vote response transition";
    return;
  }
  ASSERT_EQ(vote_transition->outbound.size(), 1U);
  const GroupOutboundMessage vote_response = vote_transition->outbound.front();
  ASSERT_GT(follower->durable_physical_sequence(), 0U);
  auto elected = leader->execute_batch(
      {{group, ReceiveOperation{vote_response.source, vote_response.outbound.message}}});
  ASSERT_TRUE(elected.has_value());
  ASSERT_EQ(leader->find_group(group)->role(), Role::kLeader);

  auto proposed = leader->execute_batch({{group, ProposeOperation{1U, {std::byte{0x42U}}}}});
  ASSERT_TRUE(proposed.has_value());
  const auto& proposal_transition = proposed->front().transition;
  if (!proposal_transition.has_value()) {
    ADD_FAILURE() << "expected proposal transition";
    return;
  }
  EXPECT_EQ(leader->find_group(group)->commit_index(), 0U);
  EXPECT_EQ(leader->prove_quorum_sync(group, 1U).error().code(), common::StatusCode::kUnavailable);
  const auto append_request =
      std::ranges::find_if(proposal_transition->outbound, [](const GroupOutboundMessage& message) {
        return message.outbound.destination == 2U;
      });
  ASSERT_NE(append_request, proposal_transition->outbound.end());

  const std::uint64_t follower_before = follower->durable_physical_sequence();
  auto appended = follower->execute_batch(
      {{group, ReceiveOperation{append_request->source, append_request->outbound.message}}});
  ASSERT_TRUE(appended.has_value());
  const auto& append_transition = appended->front().transition;
  if (!append_transition.has_value()) {
    ADD_FAILURE() << "expected append response transition";
    return;
  }
  ASSERT_EQ(append_transition->outbound.size(), 1U);
  EXPECT_GT(follower->durable_physical_sequence(), follower_before);
  const GroupOutboundMessage append_response = append_transition->outbound.front();

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

TEST(DurableMultiRaftRuntimeTest, PersistsAndRecoversCompletedMembershipChange) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const GroupId group = group_id(std::byte{9U});
  const std::vector<RaftGroupConfiguration> groups{{group, {1U}}};
  auto runtime = DurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_TRUE(runtime->execute_batch({{group, StartElectionOperation{}}}).has_value());

  auto joint =
      runtime->execute_batch({{group, BeginMembershipChangeOperation{.new_voters = {1U, 2U}}}});
  ASSERT_TRUE(joint.has_value()) << joint.error().to_string();
  EXPECT_TRUE(runtime->find_group(group)->joint_membership_active());
  EXPECT_EQ(runtime->find_group(group)->commit_index(), 0U);
  auto joint_commit = runtime->execute_batch(
      {{group, ReceiveOperation{2U, AppendEntriesResponse{1U, true, 1U, std::nullopt, 0U}}}});
  ASSERT_TRUE(joint_commit.has_value()) << joint_commit.error().to_string();
  EXPECT_EQ(runtime->find_group(group)->commit_index(), 1U);

  auto final = runtime->execute_batch({{group, FinalizeMembershipChangeOperation{}}});
  ASSERT_TRUE(final.has_value()) << final.error().to_string();
  auto final_commit = runtime->execute_batch(
      {{group, ReceiveOperation{2U, AppendEntriesResponse{1U, true, 2U, std::nullopt, 0U}}}});
  ASSERT_TRUE(final_commit.has_value()) << final_commit.error().to_string();
  ASSERT_EQ(runtime->find_group(group)->commit_index(), 2U);
  EXPECT_FALSE(runtime->find_group(group)->joint_membership_active());
  EXPECT_TRUE(
      std::ranges::equal(runtime->find_group(group)->voters(), std::vector<NodeId>{1U, 2U}));
  const std::uint64_t durable_sequence = runtime->durable_physical_sequence();
  ASSERT_TRUE(runtime->close().is_ok());

  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group)->commit_index(), 2U);
  EXPECT_FALSE(reopened->find_group(group)->joint_membership_active());
  EXPECT_TRUE(
      std::ranges::equal(reopened->find_group(group)->voters(), std::vector<NodeId>{1U, 2U}));
  EXPECT_EQ(reopened->durable_physical_sequence(), durable_sequence);
}

TEST(DurableMultiRaftRuntimeTest, ReleasesSnapshotAcknowledgmentOnlyAfterInstalledStateIsDurable) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const GroupId group = group_id(std::byte{10U});
  const std::vector<RaftGroupConfiguration> groups{{group, {1U, 2U}}};
  auto follower = DurableMultiRaftRuntime::create_new(2U, log_config, groups);
  ASSERT_TRUE(follower.has_value()) << follower.error().to_string();
  SnapshotMetadata snapshot{};
  snapshot.last_included_index = 5U;
  snapshot.last_included_term = 2U;
  snapshot.manifest_generation = 11U;
  snapshot.configuration_index = 0U;
  snapshot.voters = {1U, 2U};

  auto requested = follower->execute_batch(
      {{group, ReceiveOperation{1U, InstallSnapshotRequest{2U, 1U, snapshot}}}});
  ASSERT_TRUE(requested.has_value()) << requested.error().to_string();
  const auto& requested_transition = requested->front().transition;
  if (!requested_transition.has_value()) {
    ADD_FAILURE() << "expected snapshot request transition";
    return;
  }
  EXPECT_TRUE(requested_transition->snapshot_install.has_value());
  EXPECT_TRUE(requested_transition->outbound.empty());
  const std::uint64_t term_sequence = follower->durable_physical_sequence();
  ASSERT_GT(term_sequence, 0U);

  auto completed =
      follower->execute_batch({{group, CompleteSnapshotInstallOperation{1U, snapshot, true}}});
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  const auto& completed_transition = completed->front().transition;
  if (!completed_transition.has_value()) {
    ADD_FAILURE() << "expected snapshot completion transition";
    return;
  }
  ASSERT_EQ(completed_transition->outbound.size(), 1U);
  EXPECT_GT(follower->durable_physical_sequence(), term_sequence);
  EXPECT_EQ(follower->find_group(group)->persistent_state().snapshot, snapshot);
  ASSERT_TRUE(follower->close().is_ok());

  auto reopened = DurableMultiRaftRuntime::open_existing(2U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group)->persistent_state().snapshot, snapshot);
  EXPECT_EQ(reopened->find_group(group)->commit_index(), 5U);
  EXPECT_EQ(reopened->find_group(group)->applied_index(), 5U);
}

} // namespace
} // namespace chronos::raft
