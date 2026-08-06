#include "chronos/ingest/retry_directory.hpp"
#include "support/counting_allocator.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

namespace {

template <typename Identifier>
// Value and domain deliberately construct one nominal test identity without a production generator.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] Identifier nominal_id(const std::uint64_t value, const std::uint8_t domain) {
  chronos::common::Uuid::Bytes bytes{};
  bytes.front() = static_cast<std::byte>(domain);
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[bytes.size() - 1U - index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] chronos::ingest::RetryIdentity retry_identity(const std::uint64_t index) {
  const std::uint64_t value = index + 1U;
  return chronos::ingest::RetryIdentity{
      .client_id = nominal_id<chronos::ingest::ClientId>(value, 1U),
      .client_batch_id = nominal_id<chronos::ingest::ClientBatchId>(value, 2U)};
}

[[nodiscard]] chronos::ingest::ColumnarAppendMutationIdentity mutation(const std::uint64_t index) {
  chronos::ingest::Sha256Digest::Bytes digest{};
  const std::uint64_t value = index + 1U;
  for (std::size_t byte = 0U; byte < sizeof(value); ++byte) {
    digest[digest.size() - 1U - byte] = static_cast<std::byte>((value >> (byte * 8U)) & 0xffU);
  }
  return chronos::ingest::ColumnarAppendMutationIdentity{
      .table_id = nominal_id<chronos::schema::TableId>(1U, 3U),
      .tablet_id = nominal_id<chronos::schema::TabletId>(1U, 4U),
      .request_digest = chronos::ingest::Sha256Digest{digest}};
}

[[nodiscard]] std::shared_ptr<const chronos::ingest::ColumnarAppendRetryOutcome>
outcome(const std::uint64_t index) {
  chronos::wal::WalId wal_id;
  wal_id.bytes.back() = std::byte{1U};
  return std::make_shared<const chronos::ingest::ColumnarAppendRetryOutcome>(
      chronos::ingest::ColumnarAppendRetryOutcome{.mutation = mutation(index),
                                                  .wal_id = wal_id,
                                                  .record_sequence = index + 1U,
                                                  .applied_row_count = 1U});
}

struct PopulatedDirectory {
  chronos::ingest::RetryDirectory directory;
  chronos::benchmark_support::AllocationCounts allocations;
};

[[nodiscard]] std::optional<PopulatedDirectory> populate_directory(const std::size_t entries) {
  chronos::benchmark_support::ScopedAllocationCounting counting;
  auto created = chronos::ingest::RetryDirectory::create({.maximum_entries = entries});
  if (!created.has_value()) {
    return std::nullopt;
  }
  for (std::size_t index = 0U; index < entries; ++index) {
    auto decision = created->try_reserve(retry_identity(index), mutation(index));
    if (!decision.has_value() ||
        decision->kind() != chronos::ingest::RetryDecisionKind::kReserved ||
        decision->reservation() == nullptr ||
        !decision->reservation()->mark_wal_started().is_ok()) {
      return std::nullopt;
    }
    auto committed = decision->reservation()->commit_published(outcome(index));
    if (!committed.has_value()) {
      return std::nullopt;
    }
  }
  const chronos::benchmark_support::AllocationCounts allocations = counting.stop();
  return PopulatedDirectory{.directory = std::move(*created), .allocations = allocations};
}

[[nodiscard]] bool matching_lookup(chronos::ingest::RetryDirectory& directory,
                                   const std::size_t index) {
  auto decision = directory.try_reserve(retry_identity(index), mutation(index));
  return decision.has_value() &&
         decision->kind() == chronos::ingest::RetryDecisionKind::kMatchingCommitted &&
         decision->committed_outcome() != nullptr &&
         decision->committed_outcome()->record_sequence == index + 1U;
}

void benchmark_retry_lookup(benchmark::State& state) {
  const auto entries = static_cast<std::size_t>(state.range(0));
  auto populated = populate_directory(entries);
  if (!populated.has_value()) {
    state.SkipWithError("could not construct the committed retry population");
    return;
  }
  std::size_t cursor = 0U;
  for ([[maybe_unused]] auto iteration : state) {
    if (!matching_lookup(populated->directory, cursor)) {
      state.SkipWithError("retry population lookup did not return the committed identity");
      return;
    }
    cursor = cursor + 1U == entries ? 0U : cursor + 1U;
    benchmark::DoNotOptimize(cursor);
  }
  const chronos::ingest::RetryDirectoryMetrics metrics = populated->directory.metrics();
  if (metrics.entries != entries || metrics.committed_entries != entries ||
      metrics.in_flight_entries != 0U) {
    state.SkipWithError("retry lookup changed the committed population");
    return;
  }
  state.SetItemsProcessed(state.iterations());
  state.counters["population_allocations"] =
      static_cast<double>(populated->allocations.allocations);
  state.counters["population_requested_bytes"] =
      static_cast<double>(populated->allocations.allocated_bytes);
  state.SetLabel("matching lookup over one bounded committed std::map population");
}

struct SharedRetryFixture {
  std::mutex mutex;
  std::shared_ptr<chronos::ingest::RetryDirectory> directory;
  std::size_t users{};
  std::size_t entries{};
  chronos::benchmark_support::AllocationCounts allocations;
};

SharedRetryFixture shared_retry_fixture;

struct SharedRetryLease {
  std::shared_ptr<chronos::ingest::RetryDirectory> directory;
  chronos::benchmark_support::AllocationCounts allocations;
};

[[nodiscard]] std::optional<SharedRetryLease> acquire_shared_retry(const std::size_t entries) {
  std::lock_guard lock{shared_retry_fixture.mutex};
  if (shared_retry_fixture.users == 0U) {
    auto populated = populate_directory(entries);
    if (!populated.has_value()) {
      return std::nullopt;
    }
    shared_retry_fixture.directory =
        std::make_shared<chronos::ingest::RetryDirectory>(std::move(populated->directory));
    shared_retry_fixture.entries = entries;
    shared_retry_fixture.allocations = populated->allocations;
  }
  if (shared_retry_fixture.entries != entries) {
    return std::nullopt;
  }
  ++shared_retry_fixture.users;
  return SharedRetryLease{.directory = shared_retry_fixture.directory,
                          .allocations = shared_retry_fixture.allocations};
}

void release_shared_retry() {
  std::lock_guard lock{shared_retry_fixture.mutex};
  --shared_retry_fixture.users;
  if (shared_retry_fixture.users == 0U) {
    shared_retry_fixture.directory.reset();
  }
}

void benchmark_retry_lookup_contention(benchmark::State& state) {
  const auto entries = static_cast<std::size_t>(state.range(0));
  auto lease = acquire_shared_retry(entries);
  if (!lease.has_value()) {
    state.SkipWithError("could not construct the shared committed retry population");
    return;
  }
  std::size_t cursor = static_cast<std::size_t>(state.thread_index()) % entries;
  bool valid = true;
  for ([[maybe_unused]] auto iteration : state) {
    if (!matching_lookup(*lease->directory, cursor)) {
      valid = false;
      break;
    }
    cursor = (cursor + 97U) % entries;
    benchmark::DoNotOptimize(cursor);
  }
  const chronos::ingest::RetryDirectoryMetrics metrics = lease->directory->metrics();
  valid = valid && metrics.entries == entries && metrics.committed_entries == entries &&
          metrics.in_flight_entries == 0U;
  if (!valid) {
    state.SkipWithError("contended retry lookup changed or missed the committed population");
  }
  state.SetItemsProcessed(state.iterations());
  state.counters["population_allocations"] = benchmark::Counter(
      static_cast<double>(lease->allocations.allocations), benchmark::Counter::kAvgThreads);
  state.counters["population_requested_bytes"] = benchmark::Counter(
      static_cast<double>(lease->allocations.allocated_bytes), benchmark::Counter::kAvgThreads);
  state.SetLabel("threads contend on matching lookups in one shared retry directory");
  release_shared_retry();
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_retry_lookup)->Arg(64)->Arg(4096)->Arg(65536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_retry_lookup_contention)
    ->Arg(4096)
    ->Arg(65536)
    ->Threads(1)
    ->Threads(2)
    ->Threads(4);

} // namespace
