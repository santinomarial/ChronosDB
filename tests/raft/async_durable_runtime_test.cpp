#include "chronos/raft/async_durable_runtime.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <poll.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-async-raft-XXXXXX").string();
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

TEST(AsyncDurableMultiRaftRuntimeTest, DrainsAcceptedFifoBatchesObservesAndRecoversAppliedState) {
  TemporaryDirectory directory;
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  const GroupId group = group_id(std::byte{1U});
  const std::vector<RaftGroupConfiguration> groups{{group, {1U}}};
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();

  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  auto proposal =
      runtime->try_submit({{group, ProposeOperation{.type = 1U, .payload = {std::byte{0x42U}}}}});
  auto applied = runtime->try_submit({{group, MarkAppliedOperation{.index = 1U}}});
  auto observed = runtime->try_observe_group(group);
  auto missing = runtime->try_observe_group(group_id(std::byte{9U}));
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(proposal.has_value());
  ASSERT_TRUE(applied.has_value());
  ASSERT_TRUE(observed.has_value());
  ASSERT_TRUE(missing.has_value());

  EXPECT_TRUE(runtime->shutdown().is_ok());
  auto election_result = election->wait();
  auto proposal_result = proposal->wait();
  auto applied_result = applied->wait();
  auto observed_result = observed->wait();
  auto missing_result = missing->wait();
  ASSERT_TRUE(election_result.has_value()) << election_result.error().to_string();
  ASSERT_TRUE(proposal_result.has_value()) << proposal_result.error().to_string();
  ASSERT_TRUE(applied_result.has_value()) << applied_result.error().to_string();
  ASSERT_TRUE(observed_result.has_value()) << observed_result.error().to_string();
  ASSERT_TRUE(missing_result.has_value()) << missing_result.error().to_string();
  EXPECT_EQ(applied->wait().error().code(), common::StatusCode::kInvalidArgument);
  ASSERT_EQ(election_result->size(), 1U);
  ASSERT_EQ(proposal_result->size(), 1U);
  ASSERT_EQ(applied_result->size(), 1U);
  EXPECT_TRUE(election_result->front().status.is_ok());
  EXPECT_TRUE(proposal_result->front().status.is_ok());
  EXPECT_TRUE(applied_result->front().status.is_ok());
  ASSERT_EQ(observed_result->size(), 1U);
  ASSERT_TRUE(observed_result->front().status.is_ok());
  EXPECT_FALSE(observed_result->front().transition.has_value());
  ASSERT_TRUE(observed_result->front().observation.has_value());
  EXPECT_EQ(observed_result->front().observation->group_id, group);
  EXPECT_EQ(observed_result->front().observation->node_id, 1U);
  EXPECT_EQ(observed_result->front().observation->role, Role::kLeader);
  EXPECT_EQ(observed_result->front().observation->current_term, 1U);
  EXPECT_EQ(observed_result->front().observation->leader_id, 1U);
  EXPECT_EQ(observed_result->front().observation->last_log_index, 1U);
  EXPECT_EQ(observed_result->front().observation->commit_index, 1U);
  EXPECT_EQ(observed_result->front().observation->applied_index, 1U);
  EXPECT_EQ(observed_result->front().observation->voters, std::vector<NodeId>{1U});
  EXPECT_EQ(observed_result->front().observation->committed_voters, std::vector<NodeId>{1U});
  EXPECT_FALSE(observed_result->front().observation->joint_membership_active);
  ASSERT_EQ(missing_result->size(), 1U);
  EXPECT_EQ(missing_result->front().status.code(), common::StatusCode::kNotFound);
  EXPECT_FALSE(missing_result->front().transition.has_value());
  EXPECT_FALSE(missing_result->front().observation.has_value());

  const AsyncDurableMultiRaftMetrics metrics = runtime->metrics();
  EXPECT_FALSE(metrics.accepting);
  EXPECT_FALSE(metrics.terminal_failure);
  EXPECT_EQ(metrics.admitted_batches, 5U);
  EXPECT_EQ(metrics.completed_batches, 5U);
  EXPECT_EQ(metrics.pending_batches, 0U);
  EXPECT_EQ(metrics.pending_operations, 0U);
  EXPECT_EQ(runtime->try_submit({{group, HeartbeatOperation{}}}).error().code(),
            common::StatusCode::kUnavailable);

  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group)->commit_index(), 1U);
  EXPECT_EQ(reopened->find_group(group)->applied_index(), 1U);
}

TEST(AsyncDurableMultiRaftRuntimeTest, RejectsInvalidAndOverCapacityBatchesWithoutSideQueue) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{2U});
  AsyncDurableMultiRaftLimits limits{};
  limits.maximum_pending_batches = 1U;
  limits.maximum_pending_operations = 1U;
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, limits);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();

  EXPECT_EQ(runtime->try_submit({}).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(runtime->try_submit({{group, StartElectionOperation{}}, {group, HeartbeatOperation{}}})
                .error()
                .code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(runtime->metrics().rejected_batches, 2U);
  EXPECT_TRUE(runtime->shutdown().is_ok());

  AsyncDurableRaftCompletion invalid_completion;
  EXPECT_FALSE(invalid_completion.is_valid());
  EXPECT_EQ(invalid_completion.wait().error().code(), common::StatusCode::kInvalidArgument);
}

TEST(AsyncDurableMultiRaftRuntimeTest, FailsClosedAfterTerminalDurableRuntimeError) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{3U});
  AsyncDurableMultiRaftLimits limits{};
  limits.durable.runtime.maximum_queued_outbound = 1U;
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U, 2U, 3U}}}, limits);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());

  auto failed = election->wait();

  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_FALSE(runtime->is_accepting());
  EXPECT_EQ(runtime->terminal_status(), failed.error());
  EXPECT_TRUE(runtime->metrics().terminal_failure);
  EXPECT_EQ(runtime->try_submit({{group, HeartbeatOperation{}}}).error(), failed.error());
  EXPECT_EQ(runtime->shutdown(), failed.error());
}

TEST(AsyncDurableMultiRaftRuntimeTest, WakesAndDrainsCompletionDescriptor) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{4U});
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  ASSERT_GE(runtime->completion_descriptor(), 0);
  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());

  pollfd descriptor{.fd = runtime->completion_descriptor(), .events = POLLIN};
  ASSERT_EQ(::poll(&descriptor, 1U, 1000), 1);
  EXPECT_NE(descriptor.revents & POLLIN, 0);
  ASSERT_TRUE(runtime->drain_completion_notifications().is_ok());
  descriptor.revents = 0;
  EXPECT_EQ(::poll(&descriptor, 1U, 0), 0);
  EXPECT_TRUE(election->is_ready());
  auto result = election->wait();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_EQ(result->size(), 1U);
  EXPECT_TRUE(result->front().status.is_ok());
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::raft
