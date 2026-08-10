#include "chronos/raft/async_durable_runtime.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
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

TEST(AsyncDurableMultiRaftRuntimeTest, DrainsAcceptedFifoBatchesAndRecoversAppliedState) {
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
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(proposal.has_value());
  ASSERT_TRUE(applied.has_value());

  EXPECT_TRUE(runtime->shutdown().is_ok());
  auto election_result = election->wait();
  auto proposal_result = proposal->wait();
  auto applied_result = applied->wait();
  ASSERT_TRUE(election_result.has_value()) << election_result.error().to_string();
  ASSERT_TRUE(proposal_result.has_value()) << proposal_result.error().to_string();
  ASSERT_TRUE(applied_result.has_value()) << applied_result.error().to_string();
  EXPECT_EQ(applied->wait().error().code(), common::StatusCode::kInvalidArgument);
  ASSERT_EQ(election_result->size(), 1U);
  ASSERT_EQ(proposal_result->size(), 1U);
  ASSERT_EQ(applied_result->size(), 1U);
  EXPECT_TRUE(election_result->front().status.is_ok());
  EXPECT_TRUE(proposal_result->front().status.is_ok());
  EXPECT_TRUE(applied_result->front().status.is_ok());

  const AsyncDurableMultiRaftMetrics metrics = runtime->metrics();
  EXPECT_FALSE(metrics.accepting);
  EXPECT_FALSE(metrics.terminal_failure);
  EXPECT_EQ(metrics.admitted_batches, 3U);
  EXPECT_EQ(metrics.completed_batches, 3U);
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

} // namespace
} // namespace chronos::raft
