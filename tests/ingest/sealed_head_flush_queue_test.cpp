#include "chronos/ingest/sealed_head_flush_queue.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"
#include "ingest/sealed_head_flush_queue_internal.hpp"
#include "ingest/tablet_state_internal.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] schema::TabletId tablet_id(const std::uint16_t seed = 70U) {
  return columnar::test::id<schema::TabletId>(seed);
}

[[nodiscard]] wal::WalId wal_id() {
  wal::WalId value;
  value.bytes.back() = std::byte{1U};
  return value;
}

[[nodiscard]] head::HeadCommitPosition position(const std::uint64_t sequence) {
  return head::HeadCommitPosition{.wal_id = wal_id(), .record_sequence = sequence};
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value());
}

[[nodiscard]] std::shared_ptr<const columnar::OwnedColumnarBatch> successor_batch() {
  return std::make_shared<const columnar::OwnedColumnarBatch>(
      columnar::OwnedColumnarBatch::create(columnar::test::successor_batch_schema(),
                                           columnar::test::successor_batch_columns())
          .value());
}

[[nodiscard]] RetryIdentity retry_identity(const std::uint8_t seed) {
  return RetryIdentity{.client_id = test::request_id<ClientId>(seed),
                       .client_batch_id =
                           test::request_id<ClientBatchId>(static_cast<std::uint8_t>(seed + 32U))};
}

[[nodiscard]] ColumnarAppendMutationIdentity mutation(const schema::TabletId& target,
                                                      const std::uint8_t seed) {
  Sha256Digest::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return ColumnarAppendMutationIdentity{.table_id = columnar::test::batch_schema()->table_id(),
                                        .tablet_id = target,
                                        .request_digest = Sha256Digest{bytes}};
}

struct SnapshotSpec {
  std::uint16_t tablet_seed;
  std::uint64_t generation;
  std::uint64_t record_sequence;
};

[[nodiscard]] head::HeadSnapshot sealed_snapshot(const SnapshotSpec spec) {
  head::MutableHead mutable_head =
      head::MutableHead::create(columnar::test::batch_schema(), tablet_id(spec.tablet_seed),
                                spec.generation,
                                {.row_capacity = 2U, .variable_value_bytes = {0U, 2U, 0U}})
          .value();
  head::PreparedHeadAppend prepared = mutable_head.prepare_append(batch()).value();
  EXPECT_TRUE(prepared.mark_wal_started().is_ok());
  static_cast<void>(prepared.publish(position(spec.record_sequence)).value());
  return mutable_head.seal().value();
}

void publish(detail::SealedHeadFlushReservation& reservation, head::HeadSnapshot snapshot) {
  reservation.stage(std::move(snapshot));
  reservation.publish();
}

[[nodiscard]] SealedHeadFlushWork acquire(SealedHeadFlushQueue& queue) {
  auto result = queue.try_acquire();
  auto work = std::move(result).value();
  // This test helper intentionally turns an unexpected empty queue into bad_optional_access.
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  return std::move(work).value();
}

[[nodiscard]] SealedGenerationRetirementReceipt
retirement_receipt(const head::HeadSnapshot& snapshot) {
  std::uint64_t minimum_sequence = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t maximum_sequence = 0U;
  wal::WalId snapshot_wal_id;
  for (std::uint32_t row = 0U; row < snapshot.row_count(); ++row) {
    const head::HeadRowMetadata metadata = snapshot.row_metadata(row).value();
    snapshot_wal_id = metadata.commit_position.wal_id;
    minimum_sequence = std::min(minimum_sequence, metadata.commit_position.record_sequence);
    maximum_sequence = std::max(maximum_sequence, metadata.commit_position.record_sequence);
  }
  return detail::TabletStateTestAccess::retirement_receipt(
      snapshot.table_id(), snapshot.tablet_id(), snapshot.schema_ptr()->schema_id(),
      snapshot.schema_ptr()->version(), snapshot.generation(), snapshot.row_count(),
      snapshot_wal_id, minimum_sequence, maximum_sequence);
}

struct ManualClock {
  std::atomic<std::int64_t> nanoseconds;

  [[nodiscard]] static std::chrono::steady_clock::time_point now(void* context) noexcept {
    const auto* const clock = static_cast<ManualClock*>(context);
    return std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{clock->nanoseconds.load(std::memory_order_acquire)}};
  }
};

TEST(SealedHeadFlushQueueTest, RejectsZeroCapacity) {
  EXPECT_EQ(SealedHeadFlushQueue::create({.capacity = 0U}).error().code(),
            common::StatusCode::kInvalidArgument);
}

TEST(SealedHeadFlushQueueTest, ReservationOrderPreventsLaterProducerFromOvertaking) {
  const auto queue = SealedHeadFlushQueue::create({.capacity = 2U}).value();
  auto first = detail::SealedHeadFlushQueueTestAccess::reserve(*queue).value();
  auto second = detail::SealedHeadFlushQueueTestAccess::reserve(*queue).value();
  publish(second, sealed_snapshot({.tablet_seed = 72U, .generation = 2U, .record_sequence = 2U}));

  EXPECT_FALSE(queue->try_acquire().value().has_value());
  EXPECT_EQ(queue->metrics().reserved, 1U);
  EXPECT_EQ(queue->metrics().ready, 1U);

  publish(first, sealed_snapshot({.tablet_seed = 71U, .generation = 1U, .record_sequence = 1U}));
  SealedHeadFlushWork first_work = acquire(*queue);
  EXPECT_EQ(first_work.sequence(), 1U);
  ASSERT_NE(first_work.snapshot(), nullptr);
  EXPECT_EQ(first_work.snapshot()->tablet_id(), tablet_id(71U));
  EXPECT_FALSE(queue->try_acquire().value().has_value());
  EXPECT_TRUE(first_work.release_for_retry().is_ok());

  first_work = acquire(*queue);
  EXPECT_EQ(first_work.sequence(), 1U);
  EXPECT_TRUE(first_work.complete(retirement_receipt(*first_work.snapshot())).is_ok());
  SealedHeadFlushWork second_work = acquire(*queue);
  EXPECT_EQ(second_work.sequence(), 2U);
  ASSERT_NE(second_work.snapshot(), nullptr);
  EXPECT_EQ(second_work.snapshot()->tablet_id(), tablet_id(72U));
  EXPECT_TRUE(second_work.complete(retirement_receipt(*second_work.snapshot())).is_ok());
  EXPECT_EQ(queue->metrics().completed, 2U);
  EXPECT_EQ(queue->metrics().retries, 1U);
  EXPECT_EQ(queue->metrics().occupied, 0U);
}

TEST(SealedHeadFlushQueueTest, MetricsTrackOldestAgeAcrossRetryAndCompletion) {
  ManualClock clock;
  const auto queue =
      detail::SealedHeadFlushQueueTestAccess::create({.capacity = 2U}, &ManualClock::now, &clock)
          .value();
  auto first = detail::SealedHeadFlushQueueTestAccess::reserve(*queue).value();
  clock.nanoseconds.store(10, std::memory_order_release);
  publish(first, sealed_snapshot({.tablet_seed = 71U, .generation = 1U, .record_sequence = 1U}));
  auto second = detail::SealedHeadFlushQueueTestAccess::reserve(*queue).value();
  clock.nanoseconds.store(20, std::memory_order_release);
  publish(second, sealed_snapshot({.tablet_seed = 72U, .generation = 2U, .record_sequence = 2U}));
  clock.nanoseconds.store(30, std::memory_order_release);

  EXPECT_EQ(queue->metrics().oldest_age, std::chrono::nanoseconds{20});
  SealedHeadFlushWork work = acquire(*queue);
  EXPECT_TRUE(work.release_for_retry().is_ok());
  EXPECT_EQ(queue->metrics().oldest_age, std::chrono::nanoseconds{20});
  work = acquire(*queue);
  EXPECT_TRUE(work.complete(retirement_receipt(*work.snapshot())).is_ok());
  clock.nanoseconds.store(40, std::memory_order_release);
  EXPECT_EQ(queue->metrics().oldest_age, std::chrono::nanoseconds{20});
}

TEST(SealedHeadFlushQueueTest, CompletionRequiresTheExactPublishedReplacementReceipt) {
  const auto queue = SealedHeadFlushQueue::create({.capacity = 1U}).value();
  auto reservation = detail::SealedHeadFlushQueueTestAccess::reserve(*queue).value();
  publish(reservation,
          sealed_snapshot({.tablet_seed = 71U, .generation = 1U, .record_sequence = 1U}));
  SealedHeadFlushWork work = acquire(*queue);
  ASSERT_NE(work.snapshot(), nullptr);
  const head::HeadSnapshot& snapshot = *work.snapshot();
  const auto hostile = detail::TabletStateTestAccess::retirement_receipt(
      snapshot.table_id(), snapshot.tablet_id(), snapshot.schema_ptr()->schema_id(),
      snapshot.schema_ptr()->version(), snapshot.generation(), snapshot.row_count(), wal_id(), 2U,
      2U);

  EXPECT_EQ(work.complete(hostile).code(), common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(work.is_valid());
  EXPECT_EQ(queue->metrics().occupied, 1U);
  EXPECT_EQ(queue->metrics().in_flight, 1U);
  EXPECT_TRUE(work.complete(retirement_receipt(snapshot)).is_ok());
  EXPECT_FALSE(work.is_valid());
  EXPECT_EQ(work.snapshot(), nullptr);
  EXPECT_EQ(queue->metrics().occupied, 0U);
}

TEST(SealedHeadFlushQueueTest, TabletRotationPublishesTheExactSealedPin) {
  const auto queue = SealedHeadFlushQueue::create({.capacity = 2U}).value();
  const schema::TabletId target_id = tablet_id();
  TabletState tablet = TabletState::create(columnar::test::batch_schema(), target_id,
                                           {.head_capacity = {.row_capacity = 2U,
                                                              .variable_value_bytes = {0U, 2U, 0U}},
                                            .maximum_schema_versions = 1U,
                                            .maximum_sealed_generations = 2U,
                                            .maximum_retry_entries = 8U,
                                            .flush_queue = queue})
                           .value();
  auto first = tablet.prepare_append(retry_identity(1U), mutation(target_id, 1U), batch()).value();
  EXPECT_TRUE(first.mark_wal_started().is_ok());
  static_cast<void>(first.publish(position(1U)).value());
  auto second = tablet.prepare_append(retry_identity(2U), mutation(target_id, 2U), batch()).value();
  EXPECT_TRUE(second.cancel_before_wal().is_ok());

  const TabletSnapshot tablet_snapshot = tablet.snapshot().value();
  ASSERT_EQ(tablet_snapshot.sealed_generations().size(), 1U);
  SealedHeadFlushWork work = acquire(*queue);
  ASSERT_NE(work.snapshot(), nullptr);
  EXPECT_TRUE(work.snapshot()->is_sealed());
  EXPECT_EQ(work.snapshot()->generation(), 1U);
  EXPECT_EQ(work.snapshot()->row_count(), 2U);
  EXPECT_EQ(work.snapshot()->tablet_id(), target_id);
  EXPECT_EQ(work.snapshot()->applied_position(), position(1U));
  EXPECT_EQ(work.snapshot()->generation(),
            tablet_snapshot.sealed_generations().front().generation());
  EXPECT_TRUE(work.complete(retirement_receipt(*work.snapshot())).is_ok());
}

TEST(SealedHeadFlushQueueTest, QueuedPinOutlivesTabletAndWorkDestructionRetriesIt) {
  const auto queue = SealedHeadFlushQueue::create({.capacity = 1U}).value();
  const schema::TabletId target_id = tablet_id();
  {
    TabletState tablet =
        TabletState::create(
            columnar::test::batch_schema(), target_id,
            {.head_capacity = {.row_capacity = 2U, .variable_value_bytes = {0U, 2U, 0U}},
             .maximum_schema_versions = 1U,
             .maximum_sealed_generations = 1U,
             .maximum_retry_entries = 8U,
             .flush_queue = queue})
            .value();
    auto first =
        tablet.prepare_append(retry_identity(1U), mutation(target_id, 1U), batch()).value();
    EXPECT_TRUE(first.mark_wal_started().is_ok());
    static_cast<void>(first.publish(position(1U)).value());
    auto second =
        tablet.prepare_append(retry_identity(2U), mutation(target_id, 2U), batch()).value();
    EXPECT_TRUE(second.cancel_before_wal().is_ok());
  }

  {
    SealedHeadFlushWork abandoned = acquire(*queue);
    ASSERT_NE(abandoned.snapshot(), nullptr);
    EXPECT_EQ(abandoned.snapshot()->row_metadata(1U).value().commit_position.record_sequence, 1U);
  }
  EXPECT_EQ(queue->metrics().retries, 1U);
  EXPECT_EQ(queue->metrics().ready, 1U);
  SealedHeadFlushWork retried = acquire(*queue);
  ASSERT_NE(retried.snapshot(), nullptr);
  EXPECT_TRUE(retried.complete(retirement_receipt(*retried.snapshot())).is_ok());
}

TEST(SealedHeadFlushQueueTest, FullQueueRejectsRotationBeforeWalOrTopologyChange) {
  const auto queue = SealedHeadFlushQueue::create({.capacity = 1U}).value();
  const schema::TabletId target_id = tablet_id();
  TabletState tablet = TabletState::create(columnar::test::batch_schema(), target_id,
                                           {.head_capacity = {.row_capacity = 2U,
                                                              .variable_value_bytes = {0U, 2U, 0U}},
                                            .maximum_schema_versions = 1U,
                                            .maximum_sealed_generations = 3U,
                                            .maximum_retry_entries = 8U,
                                            .flush_queue = queue})
                           .value();
  auto first = tablet.prepare_append(retry_identity(1U), mutation(target_id, 1U), batch()).value();
  EXPECT_TRUE(first.mark_wal_started().is_ok());
  static_cast<void>(first.publish(position(1U)).value());
  auto second = tablet.prepare_append(retry_identity(2U), mutation(target_id, 2U), batch()).value();
  EXPECT_TRUE(second.mark_wal_started().is_ok());
  static_cast<void>(second.publish(position(2U)).value());
  const TabletSnapshot before = tablet.snapshot().value();
  ASSERT_EQ(before.active_generation().generation(), 2U);
  ASSERT_FALSE(before.active_generation().is_sealed());

  const auto rejected = tablet.prepare_append(retry_identity(3U), mutation(target_id, 3U), batch());
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  const TabletSnapshot after = tablet.snapshot().value();
  EXPECT_EQ(after.active_generation().generation(), 2U);
  EXPECT_FALSE(after.active_generation().is_sealed());
  EXPECT_EQ(after.visible_row_count(), before.visible_row_count());
  EXPECT_EQ(queue->metrics().capacity_rejections, 1U);

  SealedHeadFlushWork work = acquire(*queue);
  EXPECT_TRUE(work.complete(retirement_receipt(*work.snapshot())).is_ok());
  auto unblocked = tablet.prepare_append(retry_identity(3U), mutation(target_id, 3U), batch());
  ASSERT_TRUE(unblocked.has_value()) << unblocked.error().to_string();
  EXPECT_TRUE(unblocked->cancel_before_wal().is_ok());
  EXPECT_EQ(queue->metrics().accepted, 2U);
}

TEST(SealedHeadFlushQueueTest, FailedRotationAfterReservationReleasesCapacity) {
  const auto queue = SealedHeadFlushQueue::create({.capacity = 1U}).value();
  const schema::TabletId target_id = tablet_id();
  TabletState tablet = TabletState::create(columnar::test::batch_schema(), target_id,
                                           {.head_capacity = {.row_capacity = 2U,
                                                              .variable_value_bytes = {0U, 2U, 0U}},
                                            .maximum_schema_versions = 2U,
                                            .maximum_sealed_generations = 2U,
                                            .maximum_retry_entries = 8U,
                                            .flush_queue = queue})
                           .value();
  auto first = tablet.prepare_append(retry_identity(1U), mutation(target_id, 1U), batch()).value();
  EXPECT_TRUE(first.mark_wal_started().is_ok());
  static_cast<void>(first.publish(position(1U)).value());
  ASSERT_TRUE(tablet
                  .register_schema(columnar::test::successor_batch_schema(),
                                   {.row_capacity = 1U, .variable_value_bytes = {0U, 2U, 0U, 2U}})
                  .is_ok());

  const auto rejected =
      tablet.prepare_append(retry_identity(2U), mutation(target_id, 2U), successor_batch());
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(queue->metrics().occupied, 0U);
  EXPECT_EQ(queue->metrics().reserved, 0U);
  EXPECT_EQ(queue->metrics().accepted, 0U);
  EXPECT_EQ(tablet.snapshot()->active_generation().generation(), 1U);
  EXPECT_FALSE(tablet.snapshot()->active_generation().is_sealed());
}

TEST(SealedHeadFlushQueueConcurrencyTest, ConcurrentProducersPublishEveryPinExactlyOnce) {
  constexpr std::size_t kProducers = 8U;
  const auto queue = SealedHeadFlushQueue::create({.capacity = kProducers}).value();
  std::array<head::HeadSnapshot, kProducers> snapshots{
      sealed_snapshot({.tablet_seed = 80U, .generation = 1U, .record_sequence = 1U}),
      sealed_snapshot({.tablet_seed = 81U, .generation = 2U, .record_sequence = 2U}),
      sealed_snapshot({.tablet_seed = 82U, .generation = 3U, .record_sequence = 3U}),
      sealed_snapshot({.tablet_seed = 83U, .generation = 4U, .record_sequence = 4U}),
      sealed_snapshot({.tablet_seed = 84U, .generation = 5U, .record_sequence = 5U}),
      sealed_snapshot({.tablet_seed = 85U, .generation = 6U, .record_sequence = 6U}),
      sealed_snapshot({.tablet_seed = 86U, .generation = 7U, .record_sequence = 7U}),
      sealed_snapshot({.tablet_seed = 87U, .generation = 8U, .record_sequence = 8U})};
  std::atomic<bool> failed{false};
  std::vector<std::thread> producers;
  producers.reserve(kProducers);
  for (std::size_t index = 0U; index < kProducers; ++index) {
    producers.emplace_back([&, index] {
      auto reservation = detail::SealedHeadFlushQueueTestAccess::reserve(*queue);
      if (!reservation.has_value()) {
        failed.store(true, std::memory_order_release);
        return;
      }
      publish(*reservation, snapshots[index]);
    });
  }
  for (std::thread& producer : producers) {
    producer.join();
  }
  ASSERT_FALSE(failed.load(std::memory_order_acquire));

  std::vector<schema::TabletId> observed;
  observed.reserve(kProducers);
  for (std::uint64_t expected_sequence = 1U; expected_sequence <= kProducers; ++expected_sequence) {
    SealedHeadFlushWork work = acquire(*queue);
    EXPECT_EQ(work.sequence(), expected_sequence);
    ASSERT_NE(work.snapshot(), nullptr);
    observed.push_back(work.snapshot()->tablet_id());
    EXPECT_TRUE(work.complete(retirement_receipt(*work.snapshot())).is_ok());
  }
  std::ranges::sort(observed);
  for (std::size_t index = 0U; index < kProducers; ++index) {
    EXPECT_EQ(observed[index], tablet_id(static_cast<std::uint16_t>(80U + index)));
  }
  EXPECT_EQ(queue->metrics().accepted, kProducers);
  EXPECT_EQ(queue->metrics().completed, kProducers);
}

} // namespace
} // namespace chronos::ingest
