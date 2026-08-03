#include "chronos/wal/wal_commit_coordinator.hpp"
#include "wal/wal_commit_coordinator_internal.hpp"
#include "wal/wal_writer_internal.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::wal {
namespace {

class Gate {
public:
  static void enter(void* const context) {
    static_cast<Gate*>(context)->enter();
  }

  void enter() {
    std::unique_lock lock{mutex_};
    entered_ = true;
    condition_.notify_all();
    condition_.wait(lock, [this] { return released_; });
  }

  void wait_until_entered() {
    std::unique_lock lock{mutex_};
    condition_.wait(lock, [this] { return entered_; });
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
  bool entered_{false};
  bool released_{false};
};

[[nodiscard]] WalWriter make_injected_writer(test::ScriptedWalSyscalls& syscalls) {
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  common::Result<WalWriter> created = detail::WalWriterTestAccess::create_new(
      {.directory_path = "/database/wal"}, generator, syscalls);
  EXPECT_TRUE(created.has_value())
      << (created.has_value() ? std::string{} : created.error().to_string());
  return created.has_value() ? std::move(*created) : WalWriter{};
}

[[nodiscard]] common::Result<WalCommitCoordinator>
start_gated(WalWriter writer, const WalCommitCoordinatorConfig& config, Gate& gate) {
  common::Result<WalCommitCoordinator> coordinator = detail::WalCommitCoordinatorTestAccess::start(
      std::move(writer), config, &Gate::enter, &gate);
  if (coordinator.has_value()) {
    gate.wait_until_entered();
  }
  return coordinator;
}

TEST(WalCommitCoordinatorConfigTest, RejectsInvalidBoundsAndClosedWriters) {
  const std::vector<WalCommitCoordinatorConfig> invalid{
      {.maximum_pending_requests = 0U},
      {.maximum_pending_encoded_bytes = kMinimumRecordLength - 1U},
      {.maximum_sync_batch_requests = 0U},
      {.maximum_sync_batch_encoded_bytes = kMinimumRecordLength - 1U},
      {.maximum_sync_batch_delay = std::chrono::microseconds{-1}},
      {.maximum_sync_batch_delay = std::chrono::hours{25}},
  };
  for (const WalCommitCoordinatorConfig& config : invalid) {
    const common::Result<WalCommitCoordinator> result =
        WalCommitCoordinator::start(WalWriter{}, config);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kInvalidArgument);
  }
  const common::Result<WalCommitCoordinator> closed =
      WalCommitCoordinator::start(WalWriter{});
  ASSERT_FALSE(closed.has_value());
  EXPECT_EQ(closed.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(WalCommitCoordinatorAdmissionTest, BoundsQueuedAndInFlightRequestsAndEncodedBytes) {
  test::ScriptedWalSyscalls syscalls;
  Gate gate;
  common::Result<WalCommitCoordinator> started = start_gated(
      make_injected_writer(syscalls),
      {.maximum_pending_requests = 2U, .maximum_pending_encoded_bytes = 128U}, gate);
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  WalCommitCoordinator coordinator = std::move(*started);
  const std::vector<std::byte> payload = test::make_application_payload();

  common::Result<WalCommitCompletion> first =
      coordinator.try_submit(payload, WalDurabilityMode::kAsync);
  common::Result<WalCommitCompletion> second =
      coordinator.try_submit(payload, WalDurabilityMode::kAsync);
  const common::Result<WalCommitCompletion> full =
      coordinator.try_submit(payload, WalDurabilityMode::kAsync);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_FALSE(full.has_value());
  EXPECT_EQ(full.error().code(), common::StatusCode::kResourceExhausted);
  const WalCommitMetrics full_metrics = coordinator.metrics();
  EXPECT_EQ(full_metrics.pending_requests, 2U);
  EXPECT_EQ(full_metrics.pending_encoded_bytes, 128U);
  EXPECT_EQ(full_metrics.rejected_requests, 1U);

  gate.release();
  EXPECT_TRUE(first->wait().has_value());
  EXPECT_TRUE(second->wait().has_value());
  EXPECT_TRUE(coordinator.shutdown().is_ok());
  const WalCommitMetrics drained = coordinator.metrics();
  EXPECT_EQ(drained.pending_requests, 0U);
  EXPECT_EQ(drained.pending_encoded_bytes, 0U);
  EXPECT_EQ(drained.high_water_pending_requests, 2U);
  EXPECT_EQ(drained.high_water_pending_encoded_bytes, 128U);
  EXPECT_EQ(drained.admitted_requests, 2U);
  EXPECT_EQ(drained.acknowledged_async_requests, 2U);
}

TEST(WalCommitCoordinatorBatchTest, MixedModesPreserveIndividualAcknowledgmentBoundaries) {
  test::ScriptedWalSyscalls syscalls;
  Gate worker_gate;
  Gate sync_gate;
  common::Result<WalCommitCoordinator> started = start_gated(
      make_injected_writer(syscalls),
      {.maximum_pending_requests = 3U,
       .maximum_pending_encoded_bytes = 192U,
       .maximum_sync_batch_requests = 3U,
       .maximum_sync_batch_encoded_bytes = 192U,
       .maximum_sync_batch_delay = std::chrono::hours{1}},
      worker_gate);
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  WalCommitCoordinator coordinator = std::move(*started);
  syscalls.fdatasync_hook = [&sync_gate] { sync_gate.enter(); };
  const std::vector<std::byte> payload = test::make_application_payload();

  common::Result<WalCommitCompletion> first_local =
      coordinator.try_submit(payload, WalDurabilityMode::kLocalSync);
  common::Result<WalCommitCompletion> async =
      coordinator.try_submit(payload, WalDurabilityMode::kAsync);
  common::Result<WalCommitCompletion> second_local =
      coordinator.try_submit(payload, WalDurabilityMode::kLocalSync);
  ASSERT_TRUE(first_local.has_value());
  ASSERT_TRUE(async.has_value());
  ASSERT_TRUE(second_local.has_value());

  worker_gate.release();
  sync_gate.wait_until_entered();
  EXPECT_FALSE(first_local->is_ready());
  EXPECT_TRUE(async->is_ready());
  EXPECT_FALSE(second_local->is_ready());
  const common::Result<WalCommitResult> async_result = async->wait();
  ASSERT_TRUE(async_result.has_value());
  EXPECT_EQ(async_result->requested_durability, WalDurabilityMode::kAsync);
  EXPECT_EQ(async_result->effective_durability, WalDurabilityMode::kAsync);
  EXPECT_FALSE(async_result->synchronization_position.has_value());

  sync_gate.release();
  const common::Result<WalCommitResult> first_result = first_local->wait();
  const common::Result<WalCommitResult> second_result = second_local->wait();
  ASSERT_TRUE(first_result.has_value());
  ASSERT_TRUE(second_result.has_value());
  EXPECT_TRUE(first_result->synchronization_position.has_value());
  EXPECT_TRUE(second_result->synchronization_position.has_value());
  EXPECT_EQ(first_result->requested_durability, WalDurabilityMode::kLocalSync);
  EXPECT_EQ(first_result->effective_durability, WalDurabilityMode::kLocalSync);
  EXPECT_EQ(first_result->admission_sequence, 1U);
  EXPECT_EQ(async_result->admission_sequence, 2U);
  EXPECT_EQ(second_result->admission_sequence, 3U);
  EXPECT_EQ(first_result->append.record_sequence, 1U);
  EXPECT_EQ(async_result->append.record_sequence, 2U);
  EXPECT_EQ(second_result->append.record_sequence, 3U);
  EXPECT_TRUE(coordinator.shutdown().is_ok());

  const WalCommitMetrics metrics = coordinator.metrics();
  EXPECT_EQ(metrics.synchronization_attempts, 1U);
  EXPECT_EQ(metrics.successful_synchronizations, 1U);
  EXPECT_EQ(metrics.local_sync_batches, 1U);
  EXPECT_EQ(metrics.local_sync_requests_in_batches, 2U);
  EXPECT_EQ(metrics.maximum_observed_local_sync_batch_requests, 2U);
  EXPECT_EQ(metrics.acknowledged_async_requests, 1U);
  EXPECT_EQ(metrics.acknowledged_local_sync_requests, 2U);
}

TEST(WalCommitCoordinatorBatchTest, EncodedByteLimitAndZeroDelayTriggerSynchronization) {
  {
    test::ScriptedWalSyscalls syscalls;
    Gate gate;
    common::Result<WalCommitCoordinator> started = start_gated(
        make_injected_writer(syscalls),
        {.maximum_pending_requests = 2U,
         .maximum_pending_encoded_bytes = 128U,
         .maximum_sync_batch_requests = 10U,
         .maximum_sync_batch_encoded_bytes = 128U,
         .maximum_sync_batch_delay = std::chrono::hours{1}},
        gate);
    ASSERT_TRUE(started.has_value());
    WalCommitCoordinator coordinator = std::move(*started);
    const std::vector<std::byte> payload = test::make_application_payload();
    common::Result<WalCommitCompletion> first =
        coordinator.try_submit(payload, WalDurabilityMode::kLocalSync);
    common::Result<WalCommitCompletion> second =
        coordinator.try_submit(payload, WalDurabilityMode::kLocalSync);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    gate.release();
    EXPECT_TRUE(first->wait().has_value());
    EXPECT_TRUE(second->wait().has_value());
    EXPECT_TRUE(coordinator.shutdown().is_ok());
    EXPECT_EQ(coordinator.metrics().synchronization_attempts, 1U);
    EXPECT_EQ(coordinator.metrics().maximum_observed_local_sync_batch_encoded_bytes, 128U);
  }
  {
    test::ScriptedWalSyscalls syscalls;
    common::Result<WalCommitCoordinator> started = WalCommitCoordinator::start(
        make_injected_writer(syscalls),
        {.maximum_sync_batch_delay = std::chrono::microseconds{0}});
    ASSERT_TRUE(started.has_value());
    WalCommitCoordinator coordinator = std::move(*started);
    common::Result<WalCommitCompletion> completion = coordinator.try_submit(
        test::make_application_payload(), WalDurabilityMode::kLocalSync);
    ASSERT_TRUE(completion.has_value());
    EXPECT_TRUE(completion->wait().has_value());
    EXPECT_TRUE(coordinator.shutdown().is_ok());
    EXPECT_EQ(coordinator.metrics().synchronization_attempts, 1U);
  }
}

TEST(WalCommitCoordinatorBatchTest, RotationSyncReleasesPriorLocalRequestWithoutAnExtraSync) {
  test::ScriptedWalSyscalls syscalls;
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  common::Result<WalWriter> writer = detail::WalWriterTestAccess::create_new(
      {.directory_path = "/database/wal",
       .target_segment_size = kSegmentHeaderSize + 64U,
       .maximum_application_payload = kApplicationEnvelopeSize},
      generator, syscalls);
  ASSERT_TRUE(writer.has_value());
  Gate gate;
  common::Result<WalCommitCoordinator> started = start_gated(
      std::move(*writer),
      {.maximum_pending_requests = 2U,
       .maximum_pending_encoded_bytes = 128U,
       .maximum_sync_batch_requests = 2U,
       .maximum_sync_batch_encoded_bytes = 128U,
       .maximum_sync_batch_delay = std::chrono::hours{1}},
      gate);
  ASSERT_TRUE(started.has_value());
  WalCommitCoordinator coordinator = std::move(*started);
  common::Result<WalCommitCompletion> local = coordinator.try_submit(
      test::make_application_payload(), WalDurabilityMode::kLocalSync);
  common::Result<WalCommitCompletion> async = coordinator.try_submit(
      test::make_application_payload(), WalDurabilityMode::kAsync);
  ASSERT_TRUE(local.has_value());
  ASSERT_TRUE(async.has_value());
  gate.release();

  const common::Result<WalCommitResult> local_result = local->wait();
  const common::Result<WalCommitResult> async_result = async->wait();
  ASSERT_TRUE(local_result.has_value());
  ASSERT_TRUE(async_result.has_value());
  ASSERT_TRUE(local_result->synchronization_position.has_value());
  EXPECT_EQ(local_result->append.record_start.segment_number, 1U);
  EXPECT_EQ(local_result->synchronization_position->segment_number, 2U);
  EXPECT_EQ(async_result->append.record_start.segment_number, 2U);
  EXPECT_TRUE(coordinator.shutdown().is_ok());
  const WalCommitMetrics metrics = coordinator.metrics();
  EXPECT_EQ(metrics.synchronization_attempts, 0U);
  EXPECT_EQ(metrics.local_sync_batches, 1U);
  EXPECT_EQ(metrics.acknowledged_local_sync_requests, 1U);
}

TEST(WalCommitCoordinatorConcurrencyTest, PhysicalSequenceMatchesLinearizedAdmissionOrder) {
  constexpr std::size_t kProducerCount = 16U;
  test::ScriptedWalSyscalls syscalls;
  Gate gate;
  common::Result<WalCommitCoordinator> started = start_gated(
      make_injected_writer(syscalls),
      {.maximum_pending_requests = kProducerCount,
       .maximum_pending_encoded_bytes = kProducerCount * 64U},
      gate);
  ASSERT_TRUE(started.has_value());
  WalCommitCoordinator coordinator = std::move(*started);

  std::atomic<bool> begin{false};
  std::mutex completions_mutex;
  std::vector<WalCommitCompletion> completions;
  completions.reserve(kProducerCount);
  std::vector<std::thread> producers;
  producers.reserve(kProducerCount);
  for (std::size_t index = 0; index < kProducerCount; ++index) {
    producers.emplace_back([&] {
      while (!begin.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      common::Result<WalCommitCompletion> submitted = coordinator.try_submit(
          test::make_application_payload(), WalDurabilityMode::kAsync);
      ASSERT_TRUE(submitted.has_value());
      const std::lock_guard lock{completions_mutex};
      completions.push_back(std::move(*submitted));
    });
  }
  begin.store(true, std::memory_order_release);
  for (std::thread& producer : producers) {
    producer.join();
  }
  ASSERT_EQ(completions.size(), kProducerCount);
  gate.release();

  std::vector<WalCommitResult> results;
  results.reserve(kProducerCount);
  for (const WalCommitCompletion& completion : completions) {
    common::Result<WalCommitResult> result = completion.wait();
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    results.push_back(*result);
  }
  EXPECT_TRUE(coordinator.shutdown().is_ok());
  std::sort(results.begin(), results.end(), [](const auto& left, const auto& right) {
    return left.admission_sequence < right.admission_sequence;
  });
  for (std::size_t index = 0; index < results.size(); ++index) {
    const std::uint64_t expected = static_cast<std::uint64_t>(index) + 1U;
    EXPECT_EQ(results[index].admission_sequence, expected);
    EXPECT_EQ(results[index].append.record_sequence, expected);
  }
}

TEST(WalCommitCoordinatorShutdownTest, StopsAdmissionAndDrainsAPartialSyncGroup) {
  test::ScriptedWalSyscalls syscalls;
  Gate gate;
  common::Result<WalCommitCoordinator> started = start_gated(
      make_injected_writer(syscalls),
      {.maximum_sync_batch_requests = 10U,
       .maximum_sync_batch_delay = std::chrono::hours{1}},
      gate);
  ASSERT_TRUE(started.has_value());
  WalCommitCoordinator coordinator = std::move(*started);
  common::Result<WalCommitCompletion> completion = coordinator.try_submit(
      test::make_application_payload(), WalDurabilityMode::kLocalSync);
  ASSERT_TRUE(completion.has_value());

  gate.release();
  EXPECT_TRUE(coordinator.shutdown().is_ok());
  EXPECT_TRUE(completion->wait().has_value());
  const common::Result<WalCommitCompletion> rejected = coordinator.try_submit(
      test::make_application_payload(), WalDurabilityMode::kAsync);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kUnavailable);
  EXPECT_TRUE(coordinator.shutdown().is_ok());
  const WalCommitMetrics metrics = coordinator.metrics();
  EXPECT_FALSE(metrics.accepting);
  EXPECT_EQ(metrics.pending_requests, 0U);
  EXPECT_EQ(metrics.acknowledged_local_sync_requests, 1U);
  EXPECT_EQ(metrics.synchronization_attempts, 1U);
}

TEST(WalCommitCoordinatorFailureTest, RequestValidationFailureDoesNotPoisonLaterAdmission) {
  test::ScriptedWalSyscalls syscalls;
  Gate gate;
  common::Result<WalCommitCoordinator> started = start_gated(
      make_injected_writer(syscalls),
      {.maximum_pending_requests = 2U, .maximum_pending_encoded_bytes = 128U}, gate);
  ASSERT_TRUE(started.has_value());
  WalCommitCoordinator coordinator = std::move(*started);
  const std::vector<std::byte> invalid_payload(15U);
  common::Result<WalCommitCompletion> invalid =
      coordinator.try_submit(invalid_payload, WalDurabilityMode::kAsync);
  common::Result<WalCommitCompletion> valid = coordinator.try_submit(
      test::make_application_payload(), WalDurabilityMode::kAsync);
  ASSERT_TRUE(invalid.has_value());
  ASSERT_TRUE(valid.has_value());
  gate.release();

  const common::Result<WalCommitResult> invalid_result = invalid->wait();
  const common::Result<WalCommitResult> valid_result = valid->wait();
  ASSERT_FALSE(invalid_result.has_value());
  EXPECT_EQ(invalid_result.error().code(), common::StatusCode::kInvalidArgument);
  ASSERT_TRUE(valid_result.has_value());
  EXPECT_EQ(valid_result->append.record_sequence, 1U);
  EXPECT_TRUE(coordinator.shutdown().is_ok());
  const WalCommitMetrics metrics = coordinator.metrics();
  EXPECT_FALSE(metrics.terminal_failure);
  EXPECT_EQ(metrics.failed_requests, 1U);
  EXPECT_EQ(metrics.appended_requests, 1U);
  EXPECT_EQ(metrics.acknowledged_async_requests, 1U);
  EXPECT_EQ(metrics.pending_requests, 0U);
  EXPECT_EQ(metrics.admitted_requests,
            metrics.acknowledged_async_requests + metrics.acknowledged_local_sync_requests +
                metrics.failed_requests);
}

TEST(WalCommitCoordinatorFailureTest, AppendFailurePoisonsAndFailsAllAcceptedRequests) {
  test::ScriptedWalSyscalls syscalls;
  Gate gate;
  common::Result<WalCommitCoordinator> started = start_gated(
      make_injected_writer(syscalls),
      {.maximum_pending_requests = 3U, .maximum_pending_encoded_bytes = 192U}, gate);
  ASSERT_TRUE(started.has_value());
  WalCommitCoordinator coordinator = std::move(*started);
  syscalls.pwrite_outcomes = {{-1, EIO}};
  std::vector<WalCommitCompletion> completions;
  for (std::size_t index = 0; index < 3U; ++index) {
    common::Result<WalCommitCompletion> submitted = coordinator.try_submit(
        test::make_application_payload(), WalDurabilityMode::kAsync);
    ASSERT_TRUE(submitted.has_value());
    completions.push_back(std::move(*submitted));
  }
  gate.release();

  common::Status first_failure;
  for (const WalCommitCompletion& completion : completions) {
    const common::Result<WalCommitResult> result = completion.wait();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), common::StatusCode::kIoError);
    if (first_failure.is_ok()) {
      first_failure = result.error();
    } else {
      EXPECT_EQ(result.error(), first_failure);
    }
  }
  EXPECT_EQ(coordinator.shutdown(), first_failure);
  const WalCommitMetrics metrics = coordinator.metrics();
  EXPECT_TRUE(metrics.terminal_failure);
  EXPECT_FALSE(metrics.accepting);
  EXPECT_EQ(metrics.pending_requests, 0U);
  EXPECT_EQ(metrics.failed_requests, 3U);
  EXPECT_EQ(metrics.appended_requests, 0U);
}

TEST(WalCommitCoordinatorFailureTest, SyncFailureKeepsAsyncSuccessAndFailsLocalWaiters) {
  test::ScriptedWalSyscalls syscalls;
  Gate gate;
  common::Result<WalCommitCoordinator> started = start_gated(
      make_injected_writer(syscalls),
      {.maximum_pending_requests = 3U,
       .maximum_pending_encoded_bytes = 192U,
       .maximum_sync_batch_requests = 3U,
       .maximum_sync_batch_encoded_bytes = 192U,
       .maximum_sync_batch_delay = std::chrono::hours{1}},
      gate);
  ASSERT_TRUE(started.has_value());
  WalCommitCoordinator coordinator = std::move(*started);
  syscalls.fdatasync_outcomes = {{-1, EIO}};
  common::Result<WalCommitCompletion> local_one = coordinator.try_submit(
      test::make_application_payload(), WalDurabilityMode::kLocalSync);
  common::Result<WalCommitCompletion> async = coordinator.try_submit(
      test::make_application_payload(), WalDurabilityMode::kAsync);
  common::Result<WalCommitCompletion> local_two = coordinator.try_submit(
      test::make_application_payload(), WalDurabilityMode::kLocalSync);
  ASSERT_TRUE(local_one.has_value());
  ASSERT_TRUE(async.has_value());
  ASSERT_TRUE(local_two.has_value());
  gate.release();

  EXPECT_TRUE(async->wait().has_value());
  const common::Result<WalCommitResult> first_result = local_one->wait();
  const common::Result<WalCommitResult> second_result = local_two->wait();
  ASSERT_FALSE(first_result.has_value());
  ASSERT_FALSE(second_result.has_value());
  EXPECT_EQ(first_result.error(), second_result.error());
  EXPECT_EQ(first_result.error().code(), common::StatusCode::kIoError);
  EXPECT_EQ(coordinator.shutdown(), first_result.error());
  const WalCommitMetrics metrics = coordinator.metrics();
  EXPECT_EQ(metrics.appended_requests, 3U);
  EXPECT_EQ(metrics.acknowledged_async_requests, 1U);
  EXPECT_EQ(metrics.acknowledged_local_sync_requests, 0U);
  EXPECT_EQ(metrics.failed_requests, 2U);
  EXPECT_EQ(metrics.synchronization_attempts, 1U);
  EXPECT_EQ(metrics.failed_synchronizations, 1U);
  EXPECT_EQ(metrics.pending_requests, 0U);
}

} // namespace
} // namespace chronos::wal
