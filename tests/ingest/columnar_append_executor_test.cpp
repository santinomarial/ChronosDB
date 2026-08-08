#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"
#include "wal/wal_commit_coordinator_internal.hpp"
#include "wal/wal_writer_internal.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] RetryIdentity retry_identity(const std::uint8_t seed) {
  return RetryIdentity{.client_id = test::request_id<ClientId>(seed),
                       .client_batch_id =
                           test::request_id<ClientBatchId>(static_cast<std::uint8_t>(seed + 32U))};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(const std::byte timestamp_tail = std::byte{0U}) {
  auto schema = columnar::test::batch_schema();
  std::vector<columnar::OwnedColumnVector> columns = columnar::test::batch_columns();
  std::vector<std::byte> timestamps(16U);
  timestamps.back() = timestamp_tail;
  columns[0] =
      columnar::test::fixed_vector(1U, columnar::test::type(schema::LogicalTypeKind::kTimestampNs),
                                   false, 2U, {}, 0U, std::move(timestamps));
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(std::move(schema), std::move(columns)).value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> successor_batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::successor_batch_schema(),
                                           columnar::test::successor_batch_columns())
          .value());
}

[[nodiscard]] TabletState tablet(const std::size_t schema_versions = 1U) {
  return TabletState::create(
             columnar::test::batch_schema(), tablet_id(),
             TabletStateConfig{.head_capacity =
                                   head::MutableHeadCapacity{.row_capacity = 8U,
                                                             .variable_value_bytes = {0U, 8U, 0U}},
                               .maximum_schema_versions = schema_versions,
                               .maximum_sealed_generations = 2U,
                               .maximum_retry_entries = 8U,
                               .flush_queue = nullptr})
      .value();
}

[[nodiscard]] ColumnarAppendExecutionInput
execution_input(const std::uint8_t seed,
                std::shared_ptr<const columnar::OwnedColumnarBatch> input_batch,
                const wal::WalDurabilityMode durability = wal::WalDurabilityMode::kAsync) {
  const RetryIdentity identity = retry_identity(seed);
  return ColumnarAppendExecutionInput{.client_id = identity.client_id,
                                      .client_batch_id = identity.client_batch_id,
                                      .batch = std::move(input_batch),
                                      .durability = durability};
}

TEST(ColumnarAppendExecutorTest, AppliesRegisteredSuccessorAndRetainsAncestorRetryOutcome) {
  wal::test::TemporaryDirectory wal_directory{"chronos-ingest-executor-schema-switch"};
  ASSERT_TRUE(wal_directory.valid());
  auto writer = wal::WalWriter::create_new({.directory_path = wal_directory.path().string()});
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  auto started = wal::WalCommitCoordinator::start(
      std::move(*writer), {.maximum_sync_batch_delay = std::chrono::microseconds{0}});
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  wal::WalCommitCoordinator coordinator = std::move(*started);
  RetryDirectory retry_directory = RetryDirectory::create({.maximum_entries = 8U}).value();
  TabletState target = tablet(2U);
  const auto ancestor = batch();
  const auto successor = successor_batch();
  ASSERT_TRUE(
      target
          .register_schema(successor->schema_ptr(),
                           head::MutableHeadCapacity{.row_capacity = 8U,
                                                     .variable_value_bytes = {0U, 8U, 0U, 8U}})
          .is_ok());

  const auto first =
      execute_columnar_append(execution_input(1U, ancestor), retry_directory, target, coordinator);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  const auto second =
      execute_columnar_append(execution_input(2U, successor), retry_directory, target, coordinator);
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  const TabletSnapshot switched = target.snapshot().value();
  ASSERT_EQ(switched.sealed_generations().size(), 1U);
  EXPECT_EQ(switched.sealed_generations()[0].schema_ptr()->version().value(), 1U);
  EXPECT_EQ(switched.schema_ptr()->version().value(), 2U);
  EXPECT_EQ(switched.visible_row_count(), 4U);

  const auto retry =
      execute_columnar_append(execution_input(1U, ancestor), retry_directory, target, coordinator);
  ASSERT_TRUE(retry.has_value()) << retry.error().to_string();
  EXPECT_EQ(retry->kind, ColumnarAppendExecutionKind::kMatchingRetry);
  EXPECT_EQ(retry->outcome.get(), first->outcome.get());
  EXPECT_FALSE(retry->wal_commit.has_value());
  EXPECT_EQ(coordinator.metrics().appended_requests, 2U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 4U);
  EXPECT_TRUE(coordinator.shutdown().is_ok());
}

class DecodingReplaySink final : public wal::WalReplaySink {
public:
  [[nodiscard]] common::Status preflight(const wal::WalReplayRecord& record) override {
    const auto decoded = decode_columnar_append_v1_record(
        wal::DecodedRecord{.header = record.header, .payload = record.payload});
    return decoded.has_value() ? common::Status::ok() : decoded.error().status();
  }

  [[nodiscard]] common::Status replay(const wal::WalReplayRecord& record) override {
    const auto decoded = decode_columnar_append_v1_record(
        wal::DecodedRecord{.header = record.header, .payload = record.payload});
    if (!decoded.has_value()) {
      return decoded.error().status();
    }
    client_ids.push_back(decoded->client_id());
    record_sequences.push_back(record.header.record_sequence);
    return common::Status::ok();
  }

  std::vector<ClientId> client_ids;
  std::vector<std::uint64_t> record_sequences;
};

class WorkerGate {
public:
  static void enter(void* const context) {
    auto& gate = *static_cast<WorkerGate*>(context);
    std::unique_lock lock{gate.mutex_};
    gate.entered_ = true;
    gate.condition_.notify_all();
    gate.condition_.wait(lock, [&gate] { return gate.released_; });
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

[[nodiscard]] wal::WalWriter make_injected_writer(wal::test::ScriptedWalSyscalls& syscalls) {
  wal::test::FixedWalIdGenerator generator{wal::test::make_wal_id()};
  auto created = wal::detail::WalWriterTestAccess::create_new({.directory_path = "/database/wal"},
                                                              generator, syscalls);
  EXPECT_TRUE(created.has_value())
      << (created.has_value() ? std::string{} : created.error().to_string());
  return created.has_value() ? std::move(*created) : wal::WalWriter{};
}

TEST(ColumnarAppendExecutorTest, AppliesAsyncAndLocalSyncAndReturnsMatchingRetriesWithoutWal) {
  wal::test::TemporaryDirectory wal_directory{"chronos-ingest-executor"};
  ASSERT_TRUE(wal_directory.valid());
  const wal::WalWriterConfig writer_config{.directory_path = wal_directory.path().string()};
  auto writer = wal::WalWriter::create_new(writer_config);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  const wal::WalId expected_wal_id = writer->wal_id();
  auto started = wal::WalCommitCoordinator::start(
      std::move(*writer), {.maximum_sync_batch_delay = std::chrono::microseconds{0}});
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  wal::WalCommitCoordinator coordinator = std::move(*started);
  RetryDirectory retry_directory = RetryDirectory::create({.maximum_entries = 8U}).value();
  TabletState target = tablet();
  const auto input_batch = batch();

  const auto first = execute_columnar_append(execution_input(1U, input_batch), retry_directory,
                                             target, coordinator);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  EXPECT_EQ(first->kind, ColumnarAppendExecutionKind::kApplied);
  EXPECT_EQ(first->requested_durability, wal::WalDurabilityMode::kAsync);
  ASSERT_TRUE(first->wal_commit.has_value());
  const wal::WalCommitResult first_commit = first->wal_commit.value_or(wal::WalCommitResult{});
  EXPECT_EQ(first_commit.append.record_sequence, 1U);
  EXPECT_EQ(first->outcome->wal_id, expected_wal_id);
  EXPECT_EQ(first->outcome->record_sequence, 1U);
  EXPECT_EQ(first->outcome->applied_row_count, 2U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 2U);
  EXPECT_EQ(target.snapshot()->retry_outcome(retry_identity(1U)).get(), first->outcome.get());

  const auto matching = execute_columnar_append(execution_input(1U, input_batch), retry_directory,
                                                target, coordinator);
  ASSERT_TRUE(matching.has_value()) << matching.error().to_string();
  EXPECT_EQ(matching->kind, ColumnarAppendExecutionKind::kMatchingRetry);
  EXPECT_EQ(matching->requested_durability, wal::WalDurabilityMode::kAsync);
  EXPECT_FALSE(matching->wal_commit.has_value());
  EXPECT_EQ(matching->outcome.get(), first->outcome.get());
  EXPECT_EQ(coordinator.metrics().appended_requests, 1U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 2U);

  const auto second =
      execute_columnar_append(execution_input(2U, input_batch, wal::WalDurabilityMode::kLocalSync),
                              retry_directory, target, coordinator);
  ASSERT_TRUE(second.has_value()) << second.error().to_string();
  EXPECT_EQ(second->kind, ColumnarAppendExecutionKind::kApplied);
  EXPECT_EQ(second->requested_durability, wal::WalDurabilityMode::kLocalSync);
  ASSERT_TRUE(second->wal_commit.has_value());
  const wal::WalCommitResult second_commit = second->wal_commit.value_or(wal::WalCommitResult{});
  EXPECT_EQ(second_commit.append.record_sequence, 2U);
  EXPECT_EQ(second_commit.effective_durability, wal::WalDurabilityMode::kLocalSync);
  EXPECT_GE(second_commit.durable_record_sequence.value_or(0U), 2U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 4U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), 2U);

  const auto conflicting = execute_columnar_append(execution_input(1U, batch(std::byte{1U})),
                                                   retry_directory, target, coordinator);
  ASSERT_FALSE(conflicting.has_value());
  EXPECT_EQ(conflicting.error().code(), common::StatusCode::kAlreadyExists);
  EXPECT_EQ(coordinator.metrics().appended_requests, 2U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 4U);

  EXPECT_TRUE(coordinator.shutdown().is_ok());
  DecodingReplaySink sink;
  const auto inspected = wal::inspect_wal(wal_directory.path().string(), sink);
  ASSERT_TRUE(inspected.has_value()) << inspected.error().to_string();
  EXPECT_EQ(sink.record_sequences, (std::vector<std::uint64_t>{1U, 2U}));
  EXPECT_EQ(sink.client_ids,
            (std::vector<ClientId>{retry_identity(1U).client_id, retry_identity(2U).client_id}));
}

TEST(ColumnarAppendExecutorTest, ReportsInFlightWithoutWalOrTabletMutation) {
  wal::test::ScriptedWalSyscalls syscalls;
  auto coordinator = wal::WalCommitCoordinator::start(make_injected_writer(syscalls)).value();
  RetryDirectory retry_directory = RetryDirectory::create({.maximum_entries = 2U}).value();
  TabletState target = tablet();
  const auto input_batch = batch();
  auto encoded_result = columnar::encode_columnar_batch_v1(*input_batch);
  ASSERT_TRUE(encoded_result.has_value()) << encoded_result.error().to_string();
  columnar::EncodedColumnarBatch encoded = std::move(*encoded_result);
  const Sha256Digest request_digest =
      compute_columnar_append_v1_request_digest(
          ColumnarAppendDigestInput{.table_id = input_batch->schema().table_id(),
                                    .tablet_id = tablet_id(),
                                    .schema_id = input_batch->schema().schema_id(),
                                    .schema_version = input_batch->schema().version(),
                                    .encoded_batch = encoded.bytes()})
          .value();
  auto held = retry_directory.try_reserve(
      retry_identity(1U),
      ColumnarAppendMutationIdentity{.table_id = input_batch->schema().table_id(),
                                     .tablet_id = tablet_id(),
                                     .request_digest = request_digest});
  ASSERT_TRUE(held.has_value());
  ASSERT_EQ(held->kind(), RetryDecisionKind::kReserved);

  const auto result = execute_columnar_append(execution_input(1U, input_batch), retry_directory,
                                              target, coordinator);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(coordinator.metrics().appended_requests, 0U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 0U);
  EXPECT_FALSE(target.metrics().failed);
  EXPECT_TRUE(coordinator.shutdown().is_ok());
}

TEST(ColumnarAppendExecutorTest, WalAdmissionRejectionCancelsEveryPreWalReservation) {
  wal::test::ScriptedWalSyscalls syscalls;
  WorkerGate gate;
  auto started = wal::detail::WalCommitCoordinatorTestAccess::start(
      make_injected_writer(syscalls), {.maximum_pending_requests = 1U}, &WorkerGate::enter, &gate);
  ASSERT_TRUE(started.has_value()) << started.error().to_string();
  wal::WalCommitCoordinator coordinator = std::move(*started);
  gate.wait_until_entered();
  auto occupying =
      coordinator.try_submit(wal::test::make_application_payload(), wal::WalDurabilityMode::kAsync);
  ASSERT_TRUE(occupying.has_value()) << occupying.error().to_string();
  RetryDirectory retry_directory = RetryDirectory::create({.maximum_entries = 2U}).value();
  TabletState target = tablet();

  const auto rejected =
      execute_columnar_append(execution_input(1U, batch()), retry_directory, target, coordinator);
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(retry_directory.metrics().entries, 0U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 0U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), 0U);
  EXPECT_FALSE(target.metrics().failed);

  gate.release();
  EXPECT_TRUE(occupying->wait().has_value());
  EXPECT_TRUE(coordinator.shutdown().is_ok());
}

TEST(ColumnarAppendExecutorTest, AcceptedWalFailureLeavesIdentityAndTabletFailedClosed) {
  wal::test::ScriptedWalSyscalls syscalls;
  wal::WalWriter writer = make_injected_writer(syscalls);
  syscalls.pwrite_outcomes.push_back({.result = -1, .error_number = EIO});
  auto coordinator = wal::WalCommitCoordinator::start(std::move(writer)).value();
  RetryDirectory retry_directory = RetryDirectory::create({.maximum_entries = 2U}).value();
  TabletState target = tablet();

  const auto failed =
      execute_columnar_append(execution_input(1U, batch()), retry_directory, target, coordinator);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  const RetryDirectoryMetrics retry_metrics = retry_directory.metrics();
  EXPECT_EQ(retry_metrics.entries, 1U);
  EXPECT_EQ(retry_metrics.in_flight_entries, 1U);
  EXPECT_EQ(retry_metrics.committed_entries, 0U);
  EXPECT_TRUE(target.metrics().failed);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 0U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), 0U);
  EXPECT_TRUE(coordinator.metrics().terminal_failure);
  EXPECT_EQ(coordinator.shutdown().code(), common::StatusCode::kIoError);
}

TEST(ColumnarAppendExecutorTest, RejectsUnknownDurabilityBeforeRetryLookup) {
  wal::test::ScriptedWalSyscalls syscalls;
  auto coordinator = wal::WalCommitCoordinator::start(make_injected_writer(syscalls)).value();
  RetryDirectory retry_directory = RetryDirectory::create({.maximum_entries = 2U}).value();
  TabletState target = tablet();

  // Deliberately exercises the defensive validation for an enum value received across an unsafe
  // integration boundary.
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  const auto invalid_durability = static_cast<wal::WalDurabilityMode>(255U);
  const auto result = execute_columnar_append(execution_input(1U, batch(), invalid_durability),
                                              retry_directory, target, coordinator);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(retry_directory.metrics().entries, 0U);
  EXPECT_EQ(coordinator.metrics().appended_requests, 0U);
  EXPECT_FALSE(target.metrics().failed);
  EXPECT_TRUE(coordinator.shutdown().is_ok());
}

} // namespace
} // namespace chronos::ingest
