#include "chronos/raft/async_durable_runtime.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <poll.h>
#include <span>
#include <string>
#include <system_error>
#include <thread>
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

class RecordingBatchContext final : public AsyncDurableRaftWorkerBatchContext {
public:
  explicit RecordingBatchContext(const std::size_t count) noexcept : request_count(count) {}
  std::size_t request_count;
};

class ApplyingWorkerExtension final : public AsyncDurableRaftWorkerExtension {
public:
  explicit ApplyingWorkerExtension(GroupId group, const bool fail_initialization = false) noexcept
      : group_(group), fail_initialization_(fail_initialization) {}

  common::Status initialize(DurableMultiRaftRuntime& runtime) override {
    std::lock_guard lock{mutex_};
    worker_thread_ = std::this_thread::get_id();
    if (fail_initialization_)
      return {common::StatusCode::kUnavailable, "worker extension initialization failed"};
    initialized_ = runtime.find_group(group_) != nullptr;
    return initialized_
               ? common::Status::ok()
               : common::Status{common::StatusCode::kNotFound, "worker extension group is absent"};
  }

  common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime&,
                const std::span<const DurableRaftRequest> requests) override {
    std::lock_guard lock{mutex_};
    if (std::this_thread::get_id() != worker_thread_)
      return common::make_unexpected(
          common::Status{common::StatusCode::kInternal, "worker extension thread changed"});
    ++prepared_;
    return std::unique_ptr<AsyncDurableRaftWorkerBatchContext>{
        std::make_unique<RecordingBatchContext>(requests.size())};
  }

  common::Status complete_batch(DurableMultiRaftRuntime& runtime,
                                std::unique_ptr<AsyncDurableRaftWorkerBatchContext> context,
                                const std::span<const DurableRaftResult> results) override {
    std::lock_guard lock{mutex_};
    if (std::this_thread::get_id() != worker_thread_)
      return {common::StatusCode::kInternal, "worker extension thread changed"};
    const auto* const recorded = dynamic_cast<const RecordingBatchContext*>(context.get());
    if (recorded == nullptr || recorded->request_count != results.size())
      return {common::StatusCode::kCorruption, "worker extension batch context changed"};
    ++completed_;
    for (const DurableRaftResult& result : results) {
      if (!result.status.is_ok() || !result.transition.has_value() ||
          !result.transition->advanced_commit_index.has_value())
        continue;
      const LogIndex committed = *result.transition->advanced_commit_index;
      auto applied = runtime.execute_batch({{group_, MarkAppliedOperation{committed}}});
      if (!applied.has_value())
        return applied.error();
      if (applied->size() != 1U || !applied->front().status.is_ok())
        return {common::StatusCode::kInternal, "worker extension could not mark applied"};
      auto receipt = runtime.prove_quorum_sync(group_, committed);
      if (!receipt.has_value())
        return receipt.error();
      receipt_ = *receipt;
    }
    return common::Status::ok();
  }

  common::Status shutdown(DurableMultiRaftRuntime&) override {
    std::lock_guard lock{mutex_};
    shutdown_on_worker_ = std::this_thread::get_id() == worker_thread_;
    return common::Status::ok();
  }

  [[nodiscard]] bool initialized() const {
    std::lock_guard lock{mutex_};
    return initialized_;
  }
  [[nodiscard]] bool shutdown_on_worker() const {
    std::lock_guard lock{mutex_};
    return shutdown_on_worker_;
  }
  [[nodiscard]] std::size_t prepared() const {
    std::lock_guard lock{mutex_};
    return prepared_;
  }
  [[nodiscard]] std::size_t completed() const {
    std::lock_guard lock{mutex_};
    return completed_;
  }
  [[nodiscard]] std::optional<QuorumSyncReceipt> receipt() const {
    std::lock_guard lock{mutex_};
    return receipt_;
  }

private:
  GroupId group_;
  bool fail_initialization_{};
  mutable std::mutex mutex_;
  std::thread::id worker_thread_;
  bool initialized_{};
  bool shutdown_on_worker_{};
  std::size_t prepared_{};
  std::size_t completed_{};
  std::optional<QuorumSyncReceipt> receipt_;
};

TEST(AsyncDurableMultiRaftRuntimeTest, RunsApplicationExtensionBeforePublishingCompletion) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{8U});
  auto extension = std::make_shared<ApplyingWorkerExtension>(group);
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, {}, extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_TRUE(extension->initialized());

  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto proposal =
      runtime->try_submit({{group, ProposeOperation{.type = 1U, .payload = {std::byte{0x5aU}}}}});
  ASSERT_TRUE(proposal.has_value());
  const auto proposed = proposal->wait();
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();

  const auto receipt = extension->receipt();
  ASSERT_TRUE(receipt.has_value());
  EXPECT_EQ(receipt->group_id, group);
  EXPECT_EQ(receipt->log_index, 1U);
  EXPECT_EQ(extension->prepared(), 2U);
  EXPECT_EQ(extension->completed(), 2U);
  EXPECT_TRUE(runtime->shutdown().is_ok());
  EXPECT_TRUE(extension->shutdown_on_worker());

  auto reopened = DurableMultiRaftRuntime::open_existing(
      1U, {.directory_path = directory.path().string()}, {}, {{group, {1U}}});
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group)->applied_index(), 1U);
}

TEST(AsyncDurableMultiRaftRuntimeTest, FailsCreationClosedWhenExtensionCannotInitialize) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{9U});
  auto extension = std::make_shared<ApplyingWorkerExtension>(group, true);
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, {}, extension);
  ASSERT_FALSE(runtime.has_value());
  EXPECT_EQ(runtime.error().code(), common::StatusCode::kUnavailable);
  EXPECT_FALSE(extension->initialized());
  EXPECT_TRUE(extension->shutdown_on_worker());
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
  EXPECT_EQ(election->submission_sequence(), 1U);
  EXPECT_EQ(proposal->submission_sequence(), 2U);
  EXPECT_EQ(applied->submission_sequence(), 3U);
  EXPECT_EQ(observed->submission_sequence(), 4U);
  EXPECT_EQ(missing->submission_sequence(), 5U);

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
  EXPECT_EQ(invalid_completion.submission_sequence(), 0U);
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
