#include "chronos/raft/async_durable_runtime.hpp"

#include <algorithm>
#include <array>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <optional>
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

class BlockingWorkerExtension final : public AsyncDurableRaftWorkerExtension {
public:
  common::Status initialize(DurableMultiRaftRuntime&) override {
    return common::Status::ok();
  }

  common::Result<std::unique_ptr<AsyncDurableRaftWorkerBatchContext>>
  prepare_batch(DurableMultiRaftRuntime&,
                const std::span<const DurableRaftRequest> requests) override {
    std::unique_lock lock{mutex_};
    if (!blocked_once_) {
      blocked_once_ = true;
      worker_blocked_ = true;
      condition_.notify_all();
      condition_.wait(lock, [this] { return released_; });
    }
    return std::unique_ptr<AsyncDurableRaftWorkerBatchContext>{
        std::make_unique<RecordingBatchContext>(requests.size())};
  }

  common::Status complete_batch(DurableMultiRaftRuntime&,
                                std::unique_ptr<AsyncDurableRaftWorkerBatchContext>,
                                std::span<const DurableRaftResult>) override {
    return common::Status::ok();
  }

  common::Status shutdown(DurableMultiRaftRuntime&) override {
    return common::Status::ok();
  }

  [[nodiscard]] bool wait_until_blocked() {
    std::unique_lock lock{mutex_};
    return condition_.wait_for(lock, std::chrono::seconds{1}, [this] { return worker_blocked_; });
  }

  void release() {
    {
      const std::lock_guard lock{mutex_};
      released_ = true;
    }
    condition_.notify_all();
  }

private:
  std::mutex mutex_;
  std::condition_variable condition_;
  bool blocked_once_{};
  bool worker_blocked_{};
  bool released_{};
};

class BlockingWorkerReleaseGuard {
public:
  explicit BlockingWorkerReleaseGuard(BlockingWorkerExtension* extension) noexcept
      : extension_(extension) {}
  ~BlockingWorkerReleaseGuard() {
    extension_->release();
  }
  BlockingWorkerReleaseGuard(const BlockingWorkerReleaseGuard&) = delete;
  BlockingWorkerReleaseGuard& operator=(const BlockingWorkerReleaseGuard&) = delete;

private:
  BlockingWorkerExtension* extension_;
};

TEST(AsyncDurableMultiRaftRuntimeTest, RunsApplicationExtensionBeforePublishingCompletion) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{8U});
  auto extension = std::make_shared<ApplyingWorkerExtension>(group);
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, {}, extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  EXPECT_TRUE(extension->initialized());
  EXPECT_TRUE(runtime->owns_worker_extension(*extension));
  const auto different_extension = std::make_shared<ApplyingWorkerExtension>(group);
  EXPECT_FALSE(runtime->owns_worker_extension(*different_extension));

  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto proposal =
      runtime->try_submit({{group, ProposeOperation{.type = 1U, .payload = {std::byte{0x5aU}}}}});
  ASSERT_TRUE(proposal.has_value());
  const auto proposed = proposal->wait();
  ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();

  const auto receipt = extension->receipt();
  if (!receipt.has_value()) {
    ADD_FAILURE() << "expected application extension quorum receipt";
    return;
  }
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
  const auto& observation = observed_result->front().observation;
  if (!observation.has_value()) {
    ADD_FAILURE() << "expected FIFO-ordered group observation";
    return;
  }
  EXPECT_EQ(observation->group_id, group);
  EXPECT_EQ(observation->node_id, 1U);
  EXPECT_EQ(observation->role, Role::kLeader);
  EXPECT_EQ(observation->current_term, 1U);
  EXPECT_EQ(observation->leader_id, 1U);
  EXPECT_EQ(observation->last_log_index, 1U);
  EXPECT_EQ(observation->commit_index, 1U);
  EXPECT_EQ(observation->applied_index, 1U);
  EXPECT_EQ(observation->voters, std::vector<NodeId>{1U});
  EXPECT_EQ(observation->committed_voters, std::vector<NodeId>{1U});
  EXPECT_FALSE(observation->joint_membership_active);
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
  AsyncRaftLogReclamationCompletion invalid_reclamation;
  EXPECT_FALSE(invalid_reclamation.is_valid());
  EXPECT_EQ(invalid_reclamation.submission_sequence(), 0U);
  EXPECT_EQ(invalid_reclamation.wait().error().code(), common::StatusCode::kInvalidArgument);
}

TEST(AsyncDurableMultiRaftRuntimeTest, RejectsOutboundReservationBeforeSideQueue) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{0x26U});
  AsyncDurableMultiRaftLimits limits{};
  limits.durable.maximum_batch_outbound = 3U;
  limits.durable.runtime.raft.maximum_voters = 3U;
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U, 2U, 3U}}}, limits);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();

  auto rejected =
      runtime->try_submit({{group, StartElectionOperation{}}, {group, StartElectionOperation{}}});

  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(runtime->is_accepting());
  EXPECT_FALSE(runtime->metrics().terminal_failure);
  EXPECT_EQ(runtime->metrics().rejected_batches, 1U);
  EXPECT_EQ(runtime->metrics().admitted_batches, 0U);

  auto admitted = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(admitted.has_value()) << admitted.error().to_string();
  auto result = admitted->wait();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_TRUE(runtime->shutdown().is_ok());
}

TEST(AsyncDurableMultiRaftRuntimeTest, CheckpointsAndReclaimsSharedLogOnTheOwningWorker) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{0x25U});
  const std::vector<RaftGroupConfiguration> groups{{group, {1U}}};
  const RaftPersistentLogConfig log_config{.directory_path = directory.path().string(),
                                           .target_segment_size = 2048U};
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(1U, log_config, groups);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  for (std::size_t index = 0U; index < 8U; ++index) {
    auto proposal = runtime->try_submit(
        {{group, ProposeOperation{.type = 1U, .payload = std::vector<std::byte>(128U)}}});
    ASSERT_TRUE(proposal.has_value());
    auto proposed = proposal->wait();
    ASSERT_TRUE(proposed.has_value()) << proposed.error().to_string();
  }

  auto reclamation = runtime->try_checkpoint_and_reclaim();
  ASSERT_TRUE(reclamation.has_value()) << reclamation.error().to_string();
  EXPECT_TRUE(reclamation->is_valid());
  const auto reclaimed = reclamation->wait();
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_GT(reclaimed->base_segment_number, 1U);
  EXPECT_GT(reclaimed->reclaimed_segments, 0U);
  EXPECT_EQ(runtime->metrics().admitted_reclamations, 1U);
  EXPECT_EQ(runtime->metrics().completed_reclamations, 1U);
  EXPECT_EQ(runtime->metrics().admitted_batches, 9U);
  EXPECT_EQ(runtime->metrics().completed_batches, 9U);
  EXPECT_FALSE(reclamation->is_ready());
  EXPECT_EQ(reclamation->wait().error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(runtime->shutdown().is_ok());

  auto reopened = DurableMultiRaftRuntime::open_existing(1U, log_config, {}, groups);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(reopened->find_group(group)->commit_index(), 8U);
}

TEST(AsyncDurableMultiRaftRuntimeTest, FailsClosedAfterTerminalDurableRuntimeError) {
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{3U});
  RaftPersistentLogConfig log_config{.directory_path = directory.path().string()};
  log_config.maximum_records = 1U;
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(1U, log_config, {{group, {1U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto election = runtime->try_submit({{group, StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  auto proposal =
      runtime->try_submit({{group, ProposeOperation{.type = 1U, .payload = {std::byte{0x42U}}}}});
  ASSERT_TRUE(proposal.has_value());

  auto failed = proposal->wait();

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

TEST(AsyncDurableMultiRaftRuntimeTest, ConcurrentShutdownDrainsTheExactAcceptedBound) {
  constexpr std::size_t kMaximumPending = 64U;
  constexpr std::size_t kProducerCount = 8U;
  constexpr std::size_t kShutdownCallerCount = 2U;
  TemporaryDirectory directory;
  const GroupId group = group_id(std::byte{5U});
  AsyncDurableMultiRaftLimits limits{};
  limits.maximum_pending_batches = kMaximumPending;
  limits.maximum_pending_operations = kMaximumPending;
  auto extension = std::make_shared<BlockingWorkerExtension>();
  auto runtime = AsyncDurableMultiRaftRuntime::create_new(
      1U, {.directory_path = directory.path().string()}, {{group, {1U}}}, limits, extension);
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  [[maybe_unused]] BlockingWorkerReleaseGuard release_on_exit{extension.get()};

  auto first = runtime->try_observe_group(group);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  if (!extension->wait_until_blocked()) {
    extension->release();
    EXPECT_TRUE(runtime->shutdown().is_ok());
    FAIL() << "durable worker did not reach the controlled shutdown boundary";
  }

  std::vector<AsyncDurableRaftCompletion> completions;
  completions.reserve(kMaximumPending);
  completions.push_back(std::move(*first));
  std::size_t resource_exhausted{};
  std::size_t unavailable{};
  std::size_t unexpected_rejections{};
  std::mutex outcomes_mutex;
  auto record = [&](common::Result<AsyncDurableRaftCompletion> outcome) {
    const std::lock_guard lock{outcomes_mutex};
    if (outcome.has_value()) {
      completions.push_back(std::move(*outcome));
    } else if (outcome.error().code() == common::StatusCode::kResourceExhausted) {
      ++resource_exhausted;
    } else if (outcome.error().code() == common::StatusCode::kUnavailable) {
      ++unavailable;
    } else {
      ++unexpected_rejections;
    }
  };

  std::barrier race_start{static_cast<std::ptrdiff_t>(kProducerCount + kShutdownCallerCount)};
  std::vector<std::thread> producers;
  producers.reserve(kProducerCount);
  for (std::size_t producer = 0U; producer < kProducerCount; ++producer) {
    producers.emplace_back([&] {
      while (true) {
        auto outcome = runtime->try_observe_group(group);
        const bool reached_bound = !outcome.has_value() &&
                                   outcome.error().code() == common::StatusCode::kResourceExhausted;
        const bool unexpected = !outcome.has_value() &&
                                outcome.error().code() != common::StatusCode::kResourceExhausted;
        record(std::move(outcome));
        if (reached_bound || unexpected)
          break;
      }
      race_start.arrive_and_wait();
      record(runtime->try_observe_group(group));
    });
  }

  std::array<common::Status, kShutdownCallerCount> shutdown_statuses;
  std::array<std::thread, kShutdownCallerCount> shutdown_callers;
  for (std::size_t caller = 0U; caller < kShutdownCallerCount; ++caller) {
    shutdown_callers[caller] = std::thread{[&, caller] {
      race_start.arrive_and_wait();
      shutdown_statuses[caller] = runtime->shutdown();
    }};
  }
  for (std::thread& producer : producers)
    producer.join();
  extension->release();
  for (std::thread& caller : shutdown_callers)
    caller.join();

  ASSERT_EQ(completions.size(), kMaximumPending);
  EXPECT_EQ(resource_exhausted + unavailable, kProducerCount * 2U);
  EXPECT_GE(resource_exhausted, kProducerCount);
  EXPECT_EQ(unexpected_rejections, 0U);
  for (const common::Status& status : shutdown_statuses)
    EXPECT_TRUE(status.is_ok()) << status.to_string();

  std::sort(completions.begin(), completions.end(),
            [](const AsyncDurableRaftCompletion& left, const AsyncDurableRaftCompletion& right) {
              return left.submission_sequence() < right.submission_sequence();
            });
  for (std::size_t index = 0U; index < completions.size(); ++index) {
    AsyncDurableRaftCompletion& completion = completions[index];
    EXPECT_EQ(completion.submission_sequence(), index + 1U);
    EXPECT_TRUE(completion.is_ready());
    auto result = completion.wait();
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    ASSERT_EQ(result->size(), 1U);
    EXPECT_TRUE(result->front().status.is_ok());
    EXPECT_TRUE(result->front().observation.has_value());
  }

  const AsyncDurableMultiRaftMetrics metrics = runtime->metrics();
  EXPECT_FALSE(metrics.accepting);
  EXPECT_FALSE(metrics.terminal_failure);
  EXPECT_EQ(metrics.admitted_batches, kMaximumPending);
  EXPECT_EQ(metrics.completed_batches, kMaximumPending);
  EXPECT_EQ(metrics.rejected_batches, resource_exhausted + unavailable);
  EXPECT_EQ(metrics.pending_batches, 0U);
  EXPECT_EQ(metrics.pending_operations, 0U);
  EXPECT_EQ(metrics.high_water_pending_batches, kMaximumPending);
  EXPECT_EQ(metrics.high_water_pending_operations, kMaximumPending);

  pollfd descriptor{.fd = runtime->completion_descriptor(), .events = POLLIN};
  ASSERT_EQ(::poll(&descriptor, 1U, 0), 1);
  EXPECT_NE(descriptor.revents & POLLIN, 0);
  ASSERT_TRUE(runtime->drain_completion_notifications().is_ok());
  descriptor.revents = 0;
  EXPECT_EQ(::poll(&descriptor, 1U, 0), 0);
}

} // namespace
} // namespace chronos::raft
