#include "chronos/common/status.hpp"
#include "chronos/query/resource_context.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <latch>
#include <limits>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

TEST(QueryResourceContextTest, RejectsZeroLimitsAndReservations) {
  EXPECT_EQ(QueryResourceContext::create(0U).error().code(), common::StatusCode::kInvalidArgument);
  const auto context = QueryResourceContext::create(64U).value();
  EXPECT_EQ(context.reserve(0U).error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(context.maximum_memory_bytes(), 64U);
  EXPECT_EQ(context.reserved_memory_bytes(), 0U);
  EXPECT_EQ(context.available_memory_bytes(), 64U);
  EXPECT_EQ(context.peak_reserved_memory_bytes(), 0U);
}

TEST(QueryResourceContextTest, ReservationsReleaseExactlyAndMovesTransferTheObligation) {
  const auto context = QueryResourceContext::create(100U).value();
  QueryMemoryReservation first = context.reserve(30U).value();
  EXPECT_TRUE(first.is_valid());
  EXPECT_EQ(first.bytes(), 30U);
  EXPECT_EQ(context.reserved_memory_bytes(), 30U);

  QueryMemoryReservation second = context.reserve(20U).value();
  EXPECT_EQ(context.reserved_memory_bytes(), 50U);
  QueryMemoryReservation moved = std::move(first);
  EXPECT_EQ(moved.bytes(), 30U);
  EXPECT_EQ(context.reserved_memory_bytes(), 50U);

  moved = std::move(second);
  EXPECT_EQ(moved.bytes(), 20U);
  EXPECT_EQ(context.reserved_memory_bytes(), 20U);
  EXPECT_EQ(context.peak_reserved_memory_bytes(), 50U);
  moved.release();
  EXPECT_FALSE(moved.is_valid());
  EXPECT_EQ(context.reserved_memory_bytes(), 0U);
  moved.release();
  EXPECT_EQ(context.reserved_memory_bytes(), 0U);

  {
    QueryMemoryReservation scoped = context.reserve(10U).value();
    EXPECT_EQ(context.reserved_memory_bytes(), 10U);
  }
  EXPECT_EQ(context.reserved_memory_bytes(), 0U);

  QueryMemoryReservation surviving;
  {
    const auto short_lived_context = QueryResourceContext::create(7U).value();
    surviving = short_lived_context.reserve(7U).value();
  }
  EXPECT_TRUE(surviving.is_valid());
  surviving.release();
}

TEST(QueryResourceContextTest, SharedCopiesEnforceOneLimitWithoutOverflow) {
  const auto context = QueryResourceContext::create(64U).value();
  // NOLINTNEXTLINE(performance-unnecessary-copy-initialization) -- exercises shared-copy state.
  const QueryResourceContext worker = context;
  auto exact = worker.reserve(64U);
  ASSERT_TRUE(exact.has_value());
  EXPECT_TRUE(context.owns(*exact));
  const auto unrelated = QueryResourceContext::create(64U).value();
  EXPECT_FALSE(unrelated.owns(*exact));
  EXPECT_EQ(context.available_memory_bytes(), 0U);
  EXPECT_EQ(context.reserve(1U).error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(context.reserve(std::numeric_limits<std::size_t>::max()).error().code(),
            common::StatusCode::kResourceExhausted);
  exact->release();
  EXPECT_EQ(worker.available_memory_bytes(), 64U);
  EXPECT_EQ(worker.peak_reserved_memory_bytes(), 64U);
}

TEST(QueryResourceContextTest, CancellationIsSharedIdempotentAndCooperative) {
  const auto context = QueryResourceContext::create(64U).value();
  // NOLINTNEXTLINE(performance-unnecessary-copy-initialization) -- exercises shared-copy state.
  const QueryResourceContext worker = context;
  QueryMemoryReservation retained = worker.reserve(32U).value();
  EXPECT_TRUE(context.check_cancelled().has_value());
  EXPECT_TRUE(worker.request_cancel());
  EXPECT_FALSE(context.request_cancel());
  EXPECT_TRUE(context.is_cancelled());
  EXPECT_EQ(context.check_cancelled().error().code(), common::StatusCode::kCancelled);
  EXPECT_EQ(context.reserve(1U).error().code(), common::StatusCode::kCancelled);

  // Cancellation does not revoke an owner from another thread. Ordinary unwinding releases it.
  EXPECT_EQ(context.reserved_memory_bytes(), 32U);
  retained.release();
  EXPECT_EQ(context.reserved_memory_bytes(), 0U);
}

TEST(QueryResourceContextTest, ConcurrentReservationsNeverExceedTheSharedLimit) {
  constexpr std::size_t kThreads = 32U;
  constexpr std::size_t kReservationBytes = 16U;
  constexpr std::size_t kSuccessfulReservations = 8U;
  const auto context =
      QueryResourceContext::create(kSuccessfulReservations * kReservationBytes).value();
  std::vector<std::optional<QueryMemoryReservation>> held(kThreads);
  std::atomic<std::size_t> successes{};
  std::atomic<std::size_t> failures{};
  std::latch start{static_cast<std::ptrdiff_t>(kThreads)};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (std::size_t index = 0U; index < kThreads; ++index) {
    threads.emplace_back([&, index] {
      start.arrive_and_wait();
      auto reservation = context.reserve(kReservationBytes);
      if (reservation.has_value()) {
        held[index].emplace(std::move(*reservation));
        successes.fetch_add(1U, std::memory_order_relaxed);
      } else {
        EXPECT_EQ(reservation.error().code(), common::StatusCode::kResourceExhausted);
        failures.fetch_add(1U, std::memory_order_relaxed);
      }
    });
  }
  for (std::thread& thread : threads)
    thread.join();

  EXPECT_EQ(successes.load(std::memory_order_relaxed), kSuccessfulReservations);
  EXPECT_EQ(failures.load(std::memory_order_relaxed), kThreads - kSuccessfulReservations);
  EXPECT_EQ(context.reserved_memory_bytes(), context.maximum_memory_bytes());
  EXPECT_EQ(context.peak_reserved_memory_bytes(), context.maximum_memory_bytes());
  held.clear();
  EXPECT_EQ(context.reserved_memory_bytes(), 0U);
}

TEST(QueryResourceContextTest, ConcurrentWorkersObserveCancellation) {
  constexpr std::size_t kWorkers = 8U;
  const auto context = QueryResourceContext::create(1'024U).value();
  std::latch ready{static_cast<std::ptrdiff_t>(kWorkers + 1U)};
  std::atomic<std::size_t> observations{};
  std::vector<std::thread> workers;
  workers.reserve(kWorkers);
  for (std::size_t index = 0U; index < kWorkers; ++index) {
    workers.emplace_back([&, worker = context] {
      ready.arrive_and_wait();
      while (!worker.is_cancelled())
        std::this_thread::yield();
      if (worker.check_cancelled().error().code() == common::StatusCode::kCancelled)
        observations.fetch_add(1U, std::memory_order_relaxed);
    });
  }
  ready.arrive_and_wait();
  EXPECT_TRUE(context.request_cancel());
  for (std::thread& worker : workers)
    worker.join();
  EXPECT_EQ(observations.load(std::memory_order_relaxed), kWorkers);
}

TEST(QueryResourceContextPropertyTest, DeterministicReserveReleaseTraceMatchesAReferenceModel) {
  constexpr std::size_t kLimit = 257U;
  const auto context = QueryResourceContext::create(kLimit).value();
  std::vector<std::optional<QueryMemoryReservation>> held(19U);
  std::vector<std::size_t> held_bytes(held.size());
  std::size_t expected_reserved = 0U;
  std::size_t expected_peak = 0U;
  std::uint32_t state = 0x4d59'5df4U;
  for (std::size_t step = 0U; step < 2'000U; ++step) {
    state = state * 1'664'525U + 1'013'904'223U;
    const std::size_t slot = state % held.size();
    if (held[slot].has_value()) {
      expected_reserved -= held_bytes[slot];
      held_bytes[slot] = 0U;
      held[slot].reset();
    } else {
      const std::size_t bytes = ((state >> 8U) % 37U) + 1U;
      auto reservation = context.reserve(bytes);
      if (bytes <= kLimit - expected_reserved) {
        ASSERT_TRUE(reservation.has_value());
        expected_reserved += bytes;
        expected_peak = std::max(expected_peak, expected_reserved);
        held_bytes[slot] = bytes;
        held[slot].emplace(std::move(*reservation));
      } else {
        ASSERT_FALSE(reservation.has_value());
        EXPECT_EQ(reservation.error().code(), common::StatusCode::kResourceExhausted);
      }
    }
    EXPECT_EQ(context.reserved_memory_bytes(), expected_reserved);
    EXPECT_EQ(context.peak_reserved_memory_bytes(), expected_peak);
  }
  held.clear();
  EXPECT_EQ(context.reserved_memory_bytes(), 0U);
}

} // namespace
} // namespace chronos::query
