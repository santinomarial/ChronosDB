#include "chronos/ingest/retry_directory.hpp"
#include "ingest/ingest_test_support.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <latch>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] RetryIdentity retry_identity(const std::uint8_t seed) {
  return RetryIdentity{.client_id = test::request_id<ClientId>(seed),
                       .client_batch_id = test::request_id<ClientBatchId>(
                           static_cast<std::uint8_t>(seed + 16U))};
}

[[nodiscard]] Sha256Digest digest(const std::uint8_t seed) {
  Sha256Digest::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(seed + index);
  }
  return Sha256Digest{bytes};
}

[[nodiscard]] ColumnarAppendMutationIdentity mutation(const std::uint8_t seed) {
  return ColumnarAppendMutationIdentity{
      .table_id = columnar::test::id<schema::TableId>(static_cast<std::uint16_t>(100U + seed)),
      .tablet_id =
          columnar::test::id<schema::TabletId>(static_cast<std::uint16_t>(200U + seed)),
      .request_digest = digest(seed)};
}

[[nodiscard]] wal::WalId wal_id(const std::uint8_t seed) {
  wal::WalId id;
  id.bytes.back() = static_cast<std::byte>(seed);
  return id;
}

[[nodiscard]] std::shared_ptr<const ColumnarAppendRetryOutcome>
outcome(const ColumnarAppendMutationIdentity& identity, const std::uint64_t sequence = 7U,
        const std::uint32_t rows = 3U) {
  return std::make_shared<const ColumnarAppendRetryOutcome>(ColumnarAppendRetryOutcome{
      .mutation = identity,
      .wal_id = wal_id(9U),
      .record_sequence = sequence,
      .applied_row_count = rows});
}

[[nodiscard]] RetryReservation take_reservation(RetryDecision& decision) {
  EXPECT_EQ(decision.kind(), RetryDecisionKind::kReserved);
  EXPECT_NE(decision.reservation(), nullptr);
  return std::move(*decision.reservation());
}

TEST(RetryDirectoryTest, RequiresAnExplicitBoundAndReportsExactMetrics) {
  const auto invalid = RetryDirectory::create({.maximum_entries = 0U});
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);

  RetryDirectory directory = RetryDirectory::create({.maximum_entries = 2U}).value();
  EXPECT_EQ(directory.metrics(), (RetryDirectoryMetrics{.maximum_entries = 2U,
                                                        .entries = 0U,
                                                        .in_flight_entries = 0U,
                                                        .committed_entries = 0U,
                                                        .high_water_entries = 0U}));

  auto first = directory.try_reserve(retry_identity(1U), mutation(1U));
  auto second = directory.try_reserve(retry_identity(2U), mutation(2U));
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(directory.metrics(), (RetryDirectoryMetrics{.maximum_entries = 2U,
                                                        .entries = 2U,
                                                        .in_flight_entries = 2U,
                                                        .committed_entries = 0U,
                                                        .high_water_entries = 2U}));
  const auto full = directory.try_reserve(retry_identity(3U), mutation(3U));
  ASSERT_FALSE(full.has_value());
  EXPECT_EQ(full.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(directory.metrics().entries, 2U);
}

TEST(RetryDirectoryTest, PreWalRejectionAndDroppedDecisionsReleaseTheIdentity) {
  RetryDirectory directory = RetryDirectory::create({.maximum_entries = 1U}).value();
  {
    const auto ignored = directory.try_reserve(retry_identity(1U), mutation(1U));
    ASSERT_TRUE(ignored.has_value());
    EXPECT_EQ(ignored->kind(), RetryDecisionKind::kReserved);
  }
  EXPECT_EQ(directory.metrics().entries, 0U);

  auto decision = directory.try_reserve(retry_identity(1U), mutation(1U));
  ASSERT_TRUE(decision.has_value());
  RetryReservation reservation = take_reservation(*decision);
  EXPECT_TRUE(reservation.is_valid());
  EXPECT_FALSE(reservation.wal_started());
  EXPECT_TRUE(reservation.cancel_before_wal().is_ok());
  EXPECT_FALSE(reservation.is_valid());
  EXPECT_EQ(directory.metrics().entries, 0U);
  ASSERT_TRUE(directory.try_reserve(retry_identity(1U), mutation(1U)).has_value());
}

TEST(RetryDirectoryTest, WalStartedReservationRemainsBlockingWhenItsHandleIsDropped) {
  RetryDirectory directory = RetryDirectory::create({.maximum_entries = 1U}).value();
  {
    auto decision = directory.try_reserve(retry_identity(1U), mutation(1U));
    ASSERT_TRUE(decision.has_value());
    RetryReservation reservation = take_reservation(*decision);
    EXPECT_TRUE(reservation.mark_wal_started().is_ok());
    EXPECT_TRUE(reservation.wal_started());
    EXPECT_EQ(reservation.cancel_before_wal().code(), common::StatusCode::kInvalidArgument);

    const auto contender = directory.try_reserve(retry_identity(1U), mutation(1U));
    ASSERT_TRUE(contender.has_value());
    EXPECT_EQ(contender->kind(), RetryDecisionKind::kInFlight);
  }
  EXPECT_EQ(directory.metrics().in_flight_entries, 1U);
  EXPECT_EQ(directory.try_reserve(retry_identity(1U), mutation(9U))->kind(),
            RetryDecisionKind::kInFlight);
  EXPECT_EQ(directory.try_reserve(retry_identity(2U), mutation(2U)).error().code(),
            common::StatusCode::kResourceExhausted);
}

TEST(RetryDirectoryTest, CommitsOnlyAValidPublishedOutcomeAfterWalStarts) {
  RetryDirectory directory = RetryDirectory::create({.maximum_entries = 2U}).value();
  const RetryIdentity key = retry_identity(1U);
  const ColumnarAppendMutationIdentity request = mutation(1U);
  auto decision = directory.try_reserve(key, request);
  ASSERT_TRUE(decision.has_value());
  RetryReservation reservation = take_reservation(*decision);

  EXPECT_EQ(reservation.commit_published(outcome(request)).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_TRUE(reservation.mark_wal_started().is_ok());
  EXPECT_EQ(reservation.mark_wal_started().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(reservation.commit_published(nullptr).error().code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(reservation.commit_published(outcome(mutation(2U))).error().code(),
            common::StatusCode::kInvalidArgument);

  const auto bad_position =
      std::make_shared<const ColumnarAppendRetryOutcome>(ColumnarAppendRetryOutcome{
          .mutation = request, .wal_id = {}, .record_sequence = 0U, .applied_row_count = 0U});
  EXPECT_EQ(reservation.commit_published(bad_position).error().code(),
            common::StatusCode::kInvalidArgument);

  const std::shared_ptr<const ColumnarAppendRetryOutcome> published = outcome(request);
  const auto committed = reservation.commit_published(published);
  ASSERT_TRUE(committed.has_value()) << committed.error().to_string();
  EXPECT_EQ(*committed, published);
  EXPECT_FALSE(reservation.is_valid());
  EXPECT_EQ(directory.metrics(), (RetryDirectoryMetrics{.maximum_entries = 2U,
                                                        .entries = 1U,
                                                        .in_flight_entries = 0U,
                                                        .committed_entries = 1U,
                                                        .high_water_entries = 1U}));

  const auto matching = directory.try_reserve(key, request);
  ASSERT_TRUE(matching.has_value());
  EXPECT_EQ(matching->kind(), RetryDecisionKind::kMatchingCommitted);
  EXPECT_EQ(matching->committed_outcome(), published);
  EXPECT_EQ(matching->committed_outcome().get(), published.get());
  EXPECT_EQ(directory.try_reserve(key, mutation(2U))->kind(), RetryDecisionKind::kConflict);

  ColumnarAppendMutationIdentity other_target = request;
  other_target.tablet_id = columnar::test::id<schema::TabletId>(999U);
  EXPECT_EQ(directory.try_reserve(key, other_target)->kind(), RetryDecisionKind::kConflict);
}

TEST(RetryDirectoryTest, IdentityScopeIncludesBothNominalComponents) {
  RetryDirectory directory = RetryDirectory::create({.maximum_entries = 3U}).value();
  const RetryIdentity first = retry_identity(1U);
  const RetryIdentity second{.client_id = first.client_id,
                             .client_batch_id = test::request_id<ClientBatchId>(99U)};
  const RetryIdentity third{.client_id = test::request_id<ClientId>(99U),
                            .client_batch_id = first.client_batch_id};
  auto first_decision = directory.try_reserve(first, mutation(1U));
  auto second_decision = directory.try_reserve(second, mutation(1U));
  auto third_decision = directory.try_reserve(third, mutation(1U));
  ASSERT_TRUE(first_decision.has_value());
  ASSERT_TRUE(second_decision.has_value());
  ASSERT_TRUE(third_decision.has_value());
  EXPECT_EQ(first_decision->kind(), RetryDecisionKind::kReserved);
  EXPECT_EQ(second_decision->kind(), RetryDecisionKind::kReserved);
  EXPECT_EQ(third_decision->kind(), RetryDecisionKind::kReserved);
  EXPECT_EQ(directory.metrics().entries, 3U);
  EXPECT_EQ(directory.metrics().high_water_entries, 3U);
}

TEST(RetryDirectoryConcurrencyTest, ExactlyOneConcurrentContenderOwnsTheReservation) {
  constexpr std::size_t kContenders = 16U;
  RetryDirectory directory = RetryDirectory::create({.maximum_entries = 1U}).value();
  const RetryIdentity key = retry_identity(1U);
  const ColumnarAppendMutationIdentity request = mutation(1U);
  std::latch start{static_cast<std::ptrdiff_t>(kContenders)};
  std::atomic<std::size_t> reserved{0U};
  std::atomic<std::size_t> in_flight{0U};
  std::atomic<std::size_t> failures{0U};
  std::vector<std::jthread> threads;
  threads.reserve(kContenders);
  for (std::size_t index = 0U; index < kContenders; ++index) {
    static_cast<void>(index);
    threads.emplace_back([&] {
      start.arrive_and_wait();
      auto decision = directory.try_reserve(key, request);
      if (!decision.has_value()) {
        ++failures;
        return;
      }
      if (decision->kind() == RetryDecisionKind::kReserved) {
        ++reserved;
        RetryReservation reservation = take_reservation(*decision);
        if (!reservation.mark_wal_started().is_ok()) {
          ++failures;
        }
      } else if (decision->kind() == RetryDecisionKind::kInFlight) {
        ++in_flight;
      } else {
        ++failures;
      }
    });
  }
  threads.clear();

  EXPECT_EQ(reserved.load(), 1U);
  EXPECT_EQ(in_flight.load(), kContenders - 1U);
  EXPECT_EQ(failures.load(), 0U);
  EXPECT_EQ(directory.metrics().in_flight_entries, 1U);
}

} // namespace
} // namespace chronos::ingest
