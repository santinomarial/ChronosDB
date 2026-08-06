#include "chronos/ingest/tablet_state.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"
#include "ingest/tablet_state_internal.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <latch>
#include <limits>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] schema::TabletId tablet_id() {
  return columnar::test::id<schema::TabletId>(70U);
}

[[nodiscard]] wal::WalId wal_id(const std::uint8_t seed = 1U) {
  wal::WalId id;
  id.bytes.back() = static_cast<std::byte>(seed);
  return id;
}

[[nodiscard]] head::HeadCommitPosition position(const std::uint64_t sequence,
                                                const std::uint8_t wal_seed = 1U) {
  return head::HeadCommitPosition{.wal_id = wal_id(wal_seed), .record_sequence = sequence};
}

[[nodiscard]] RetryIdentity retry_identity(const std::uint8_t seed) {
  return RetryIdentity{.client_id = test::request_id<ClientId>(seed),
                       .client_batch_id =
                           test::request_id<ClientBatchId>(static_cast<std::uint8_t>(seed + 32U))};
}

[[nodiscard]] Sha256Digest digest(const std::uint8_t seed) {
  Sha256Digest::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(seed + index);
  }
  return Sha256Digest{bytes};
}

[[nodiscard]] ColumnarAppendMutationIdentity mutation(
    const std::uint8_t seed,
    const std::shared_ptr<const schema::TableSchema>& schema = columnar::test::batch_schema()) {
  return ColumnarAppendMutationIdentity{
      .table_id = schema->table_id(), .tablet_id = tablet_id(), .request_digest = digest(seed)};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch>
batch(std::shared_ptr<const schema::TableSchema> schema = columnar::test::batch_schema()) {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(std::move(schema), columnar::test::batch_columns())
          .value());
}

[[nodiscard]] TabletStateConfig config(const std::uint32_t rows = 4U,
                                       const std::size_t string_bytes = 2U,
                                       const std::size_t sealed = 2U,
                                       const std::size_t retries = 8U) {
  return TabletStateConfig{
      .head_capacity = head::MutableHeadCapacity{.row_capacity = rows,
                                                 .variable_value_bytes = {0U, string_bytes, 0U}},
      .maximum_sealed_generations = sealed,
      .maximum_retry_entries = retries};
}

[[nodiscard]] TabletState tablet(const TabletStateConfig& limits = config()) {
  return TabletState::create(columnar::test::batch_schema(), tablet_id(), limits).value();
}

[[nodiscard]] PreparedTabletAppend
prepare(TabletState& target, const std::uint8_t seed,
        const std::shared_ptr<const columnar::OwnedColumnarBatch>& input) {
  auto prepared = target.prepare_append(retry_identity(seed), mutation(seed), input);
  EXPECT_TRUE(prepared.has_value()) << prepared.error().to_string();
  return std::move(*prepared);
}

[[nodiscard]] TabletAppendResult publish(PreparedTabletAppend& prepared,
                                         const std::uint64_t sequence) {
  EXPECT_TRUE(prepared.mark_wal_started().is_ok());
  auto published = prepared.publish(position(sequence));
  EXPECT_TRUE(published.has_value()) << published.error().to_string();
  return std::move(*published);
}

TEST(TabletStateTest, RequiresExplicitBoundsAndStartsAtOneExactEmptyEpoch) {
  const auto schema = columnar::test::batch_schema();
  EXPECT_EQ(TabletState::create(nullptr, tablet_id(), config()).error().code(),
            common::StatusCode::kInvalidArgument);
  TabletStateConfig zero_sealed = config();
  zero_sealed.maximum_sealed_generations = 0U;
  EXPECT_EQ(TabletState::create(schema, tablet_id(), zero_sealed).error().code(),
            common::StatusCode::kInvalidArgument);
  TabletStateConfig zero_retries = config();
  zero_retries.maximum_retry_entries = 0U;
  EXPECT_EQ(TabletState::create(schema, tablet_id(), zero_retries).error().code(),
            common::StatusCode::kInvalidArgument);

  TabletState target = TabletState::create(schema, tablet_id(), config()).value();
  const TabletSnapshot empty = target.snapshot().value();
  EXPECT_EQ(empty.table_id(), schema->table_id());
  EXPECT_EQ(empty.tablet_id(), tablet_id());
  EXPECT_EQ(empty.schema_ptr().get(), schema.get());
  EXPECT_FALSE(empty.applied_position().has_value());
  EXPECT_TRUE(empty.sealed_generations().empty());
  EXPECT_EQ(empty.active_generation().generation(), 1U);
  EXPECT_EQ(empty.active_generation().row_count(), 0U);
  EXPECT_EQ(empty.visible_row_count(), 0U);
  EXPECT_EQ(empty.retry_entry_count(), 0U);
  EXPECT_EQ(empty.retry_outcome(retry_identity(1U)), nullptr);

  const TabletStateMetrics metrics = target.metrics();
  EXPECT_EQ(metrics.maximum_sealed_generations, 2U);
  EXPECT_EQ(metrics.maximum_retry_entries, 8U);
  EXPECT_EQ(metrics.active_generation, 1U);
  EXPECT_EQ(metrics.visible_rows, 0U);
  EXPECT_FALSE(metrics.failed);
}

TEST(TabletStateTest, PreparesWithoutVisibilityAndCancelsBeforeWal) {
  TabletState target = tablet();
  const auto input = batch();
  PreparedTabletAppend prepared = prepare(target, 1U, input);
  EXPECT_FALSE(prepared.wal_started());
  EXPECT_EQ(target.snapshot()->visible_row_count(), 0U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), 0U);
  EXPECT_EQ(target.prepare_append(retry_identity(2U), mutation(2U), input).error().code(),
            common::StatusCode::kUnavailable);
  EXPECT_EQ(prepared.publish(position(1U)).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(prepared.cancel_before_wal().is_ok());
  EXPECT_FALSE(prepared.is_valid());
  EXPECT_EQ(target.snapshot()->visible_row_count(), 0U);
}

TEST(TabletStateTest, PublishesRowsPositionAndExactRetryOutcomeTogether) {
  TabletState target = tablet();
  const auto input = batch();
  PreparedTabletAppend prepared = prepare(target, 1U, input);
  const TabletAppendResult result = publish(prepared, 7U);
  ASSERT_TRUE(result.snapshot.applied_position().has_value());
  EXPECT_EQ(result.snapshot.applied_position().value_or(head::HeadCommitPosition{}), position(7U));
  EXPECT_EQ(result.snapshot.active_generation().row_count(), 2U);
  EXPECT_EQ(result.snapshot.visible_row_count(), 2U);
  EXPECT_EQ(result.snapshot.retry_entry_count(), 1U);
  EXPECT_EQ(result.snapshot.retry_outcome(retry_identity(1U)).get(), result.outcome.get());
  EXPECT_EQ(result.outcome->mutation, mutation(1U));
  EXPECT_EQ(result.outcome->wal_id, wal_id());
  EXPECT_EQ(result.outcome->record_sequence, 7U);
  EXPECT_EQ(result.outcome->applied_row_count, input->row_count());
  EXPECT_EQ(result.snapshot.active_generation().row_metadata(1U)->commit_position, position(7U));

  const TabletSnapshot reacquired = target.snapshot().value();
  EXPECT_EQ(reacquired.retry_outcome(retry_identity(1U)).get(), result.outcome.get());
  EXPECT_EQ(reacquired.active_generation().row_count(), 2U);
}

TEST(TabletStateTest, HandsTheExactPublishedOutcomeToTheGlobalRetryDirectory) {
  TabletState target = tablet();
  RetryDirectory directory = RetryDirectory::create({.maximum_entries = 2U}).value();
  const RetryIdentity identity = retry_identity(1U);
  const ColumnarAppendMutationIdentity request = mutation(1U);
  auto decision = directory.try_reserve(identity, request);
  ASSERT_TRUE(decision.has_value());
  ASSERT_EQ(decision->kind(), RetryDecisionKind::kReserved);
  RetryReservation reservation = std::move(*decision->reservation());
  PreparedTabletAppend prepared = target.prepare_append(identity, request, batch()).value();

  EXPECT_TRUE(reservation.mark_wal_started().is_ok());
  EXPECT_TRUE(prepared.mark_wal_started().is_ok());
  const TabletAppendResult published = prepared.publish(position(9U)).value();
  const auto committed = reservation.commit_published(published.outcome);
  ASSERT_TRUE(committed.has_value()) << committed.error().to_string();
  EXPECT_EQ(committed->get(), published.outcome.get());

  auto retry = directory.try_reserve(identity, request);
  ASSERT_TRUE(retry.has_value());
  EXPECT_EQ(retry->kind(), RetryDecisionKind::kMatchingCommitted);
  EXPECT_EQ(retry->committed_outcome().get(), published.outcome.get());
}

TEST(TabletStateTest, RotatesWholeBatchesAndBackpressuresAtTheSealedBound) {
  TabletState target = tablet(config(2U, 1U, 1U, 8U));
  const auto input = batch();
  PreparedTabletAppend first = prepare(target, 1U, input);
  const TabletSnapshot first_snapshot = publish(first, 1U).snapshot;

  PreparedTabletAppend cancelled_rotation = prepare(target, 2U, input);
  const TabletSnapshot topology = target.snapshot().value();
  ASSERT_EQ(topology.sealed_generations().size(), 1U);
  EXPECT_EQ(topology.sealed_generations()[0].generation(), 1U);
  EXPECT_EQ(topology.sealed_generations()[0].row_count(), 2U);
  EXPECT_EQ(topology.active_generation().generation(), 2U);
  EXPECT_EQ(topology.active_generation().row_count(), 0U);
  EXPECT_EQ(topology.applied_position(), first_snapshot.applied_position());
  EXPECT_EQ(topology.retry_entry_count(), 1U);
  EXPECT_TRUE(cancelled_rotation.cancel_before_wal().is_ok());

  PreparedTabletAppend second = prepare(target, 3U, input);
  const TabletSnapshot second_snapshot = publish(second, 3U).snapshot;
  EXPECT_EQ(second_snapshot.visible_row_count(), 4U);
  EXPECT_EQ(second_snapshot.active_generation().generation(), 2U);
  EXPECT_EQ(second_snapshot.active_generation().row_count(), 2U);
  EXPECT_EQ(second_snapshot.retry_entry_count(), 2U);

  const auto blocked = target.prepare_append(retry_identity(4U), mutation(4U), input);
  ASSERT_FALSE(blocked.has_value());
  EXPECT_EQ(blocked.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 4U);
  EXPECT_EQ(target.snapshot()->sealed_generations().size(), 1U);

  EXPECT_EQ(first_snapshot.visible_row_count(), 2U);
  EXPECT_TRUE(first_snapshot.sealed_generations().empty());
  EXPECT_EQ(first_snapshot.active_generation().generation(), 1U);
}

TEST(TabletStateTest, RejectsOversizedBatchAndRetryBoundBeforeWal) {
  const auto input = batch();
  TabletState oversized = tablet(config(1U, 1U, 1U, 4U));
  EXPECT_EQ(oversized.prepare_append(retry_identity(1U), mutation(1U), input).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_TRUE(oversized.snapshot()->sealed_generations().empty());
  EXPECT_EQ(oversized.snapshot()->active_generation().generation(), 1U);

  TabletState retry_limited = tablet(config(4U, 2U, 1U, 1U));
  PreparedTabletAppend first = prepare(retry_limited, 1U, input);
  static_cast<void>(publish(first, 1U));
  EXPECT_EQ(retry_limited.prepare_append(retry_identity(2U), mutation(2U), input).error().code(),
            common::StatusCode::kResourceExhausted);
  EXPECT_EQ(retry_limited.snapshot()->retry_entry_count(), 1U);
  EXPECT_EQ(retry_limited.prepare_append(retry_identity(1U), mutation(1U), input).error().code(),
            common::StatusCode::kAlreadyExists);
  EXPECT_EQ(retry_limited.prepare_append(retry_identity(1U), mutation(9U), input).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(TabletStateTest, InvalidPostWalPositionFailsClosedAtTheOldOuterEpoch) {
  TabletState target = tablet(config(6U, 3U));
  const auto input = batch();
  PreparedTabletAppend first = prepare(target, 1U, input);
  static_cast<void>(publish(first, 2U));
  PreparedTabletAppend invalid_append = prepare(target, 2U, input);
  EXPECT_TRUE(invalid_append.mark_wal_started().is_ok());
  EXPECT_EQ(invalid_append.publish(position(2U)).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(target.metrics().failed);
  EXPECT_EQ(target.snapshot()->visible_row_count(), 2U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), 1U);
  EXPECT_EQ(target.prepare_append(retry_identity(3U), mutation(3U), input).error().code(),
            common::StatusCode::kUnavailable);
}

class OuterPublicationGate {
public:
  static void pause(void* const context) noexcept {
    auto& gate = *static_cast<OuterPublicationGate*>(context);
    gate.reached_.store(true, std::memory_order_release);
    while (!gate.released_.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }

  [[nodiscard]] bool wait_until_reached() const {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (!reached_.load(std::memory_order_acquire)) {
      if (std::chrono::steady_clock::now() >= deadline) {
        return false;
      }
      std::this_thread::yield();
    }
    return true;
  }

  void release() noexcept {
    released_.store(true, std::memory_order_release);
  }

private:
  std::atomic<bool> reached_{false};
  std::atomic<bool> released_{false};
};

TEST(TabletStateConcurrencyTest, InnerHeadPublicationRemainsHiddenUntilOuterPublication) {
  const auto input = batch();
  OuterPublicationGate gate;
  TabletState target =
      detail::TabletStateTestAccess::create(columnar::test::batch_schema(), tablet_id(), config(),
                                            &OuterPublicationGate::pause, &gate)
          .value();
  std::atomic<bool> failed{false};
  std::jthread writer{[&] {
    auto prepared = target.prepare_append(retry_identity(1U), mutation(1U), input);
    if (!prepared.has_value() || !prepared->mark_wal_started().is_ok() ||
        !prepared->publish(position(1U)).has_value()) {
      failed.store(true, std::memory_order_release);
      gate.release();
    }
  }};

  ASSERT_TRUE(gate.wait_until_reached());
  const TabletSnapshot old_epoch = target.snapshot().value();
  EXPECT_FALSE(old_epoch.applied_position().has_value());
  EXPECT_EQ(old_epoch.visible_row_count(), 0U);
  EXPECT_EQ(old_epoch.retry_entry_count(), 0U);
  EXPECT_EQ(old_epoch.active_generation().row_count(), 0U);
  gate.release();
  writer.join();

  EXPECT_FALSE(failed.load(std::memory_order_acquire));
  const TabletSnapshot new_epoch = target.snapshot().value();
  EXPECT_EQ(new_epoch.visible_row_count(), 2U);
  EXPECT_EQ(new_epoch.retry_entry_count(), 1U);
  ASSERT_TRUE(new_epoch.applied_position().has_value());
  EXPECT_EQ(new_epoch.applied_position().value_or(head::HeadCommitPosition{}), position(1U));
}

TEST(TabletStateConcurrencyTest, AcquireReadersObserveOnlyCompleteOuterEpochs) {
  constexpr std::size_t kReaders = 4U;
  constexpr std::uint64_t kBatches = 48U;
  TabletState target = tablet(config(static_cast<std::uint32_t>(kBatches * 2U), kBatches, 1U,
                                     static_cast<std::size_t>(kBatches)));
  const auto input = batch();
  std::latch start{static_cast<std::ptrdiff_t>(kReaders + 1U)};
  std::atomic<bool> done{false};
  std::atomic<std::size_t> failures{0U};
  std::atomic<std::size_t> observations{0U};
  std::vector<std::jthread> readers;
  readers.reserve(kReaders);
  for (std::size_t index = 0U; index < kReaders; ++index) {
    static_cast<void>(index);
    readers.emplace_back([&] {
      start.arrive_and_wait();
      while (!done.load(std::memory_order_acquire)) {
        const TabletSnapshot observed = target.snapshot().value();
        const std::size_t rows = observed.visible_row_count();
        ++observations;
        if (rows % 2U != 0U || observed.retry_entry_count() != rows / 2U ||
            (rows == 0U) != !observed.applied_position().has_value()) {
          ++failures;
          continue;
        }
        if (rows != 0U &&
            observed.applied_position()->record_sequence != observed.retry_entry_count()) {
          ++failures;
        }
      }
    });
  }

  start.arrive_and_wait();
  for (std::uint64_t sequence = 1U; sequence <= kBatches; ++sequence) {
    const auto seed = static_cast<std::uint8_t>(sequence);
    PreparedTabletAppend prepared = prepare(target, seed, input);
    static_cast<void>(publish(prepared, sequence));
  }
  done.store(true, std::memory_order_release);
  readers.clear();

  EXPECT_EQ(failures.load(), 0U);
  EXPECT_GT(observations.load(), 0U);
  EXPECT_EQ(target.snapshot()->visible_row_count(), kBatches * 2U);
  EXPECT_EQ(target.snapshot()->retry_entry_count(), kBatches);
}

} // namespace
} // namespace chronos::ingest
