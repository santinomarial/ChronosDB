#include "chronos/query/resource_context.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::query {
namespace {

constexpr std::size_t kBenchmarkMemoryLimit = std::size_t{64U} * 1024U * 1024U;

void reserve_and_release(benchmark::State& state) {
  const auto bytes = static_cast<std::size_t>(state.range(0));
  const QueryResourceContext context = QueryResourceContext::create(kBenchmarkMemoryLimit).value();
  for (auto _ : state) {
    static_cast<void>(_);
    auto reservation = context.reserve(bytes);
    benchmark::DoNotOptimize(reservation);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()));
  state.counters["reservation_bytes"] = static_cast<double>(bytes);
}

void check_not_cancelled(benchmark::State& state) {
  const QueryResourceContext context = QueryResourceContext::create(1U).value();
  for (auto _ : state) {
    static_cast<void>(_);
    bool cancelled = context.is_cancelled();
    benchmark::DoNotOptimize(cancelled);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()));
}

void publish_shared_reservation(benchmark::State& state) {
  const auto copies = static_cast<std::size_t>(state.range(0));
  const QueryResourceContext context = QueryResourceContext::create(kBenchmarkMemoryLimit).value();
  std::vector<QuerySharedMemoryReservation> published;
  published.reserve(copies);
  for (auto _ : state) {
    static_cast<void>(_);
    QuerySharedMemoryReservation reservation = context.reserve_shared(4'096U).value();
    for (std::size_t copy = 0U; copy < copies; ++copy)
      published.push_back(reservation);
    benchmark::DoNotOptimize(published);
    published.clear();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(copies));
  state.counters["published_copies"] = static_cast<double>(copies);
  state.counters["query_credit_bytes"] = 4'096.0;
}

void reserve_independent_pins(benchmark::State& state) {
  const auto pins = static_cast<std::size_t>(state.range(0));
  const QueryResourceContext context = QueryResourceContext::create(kBenchmarkMemoryLimit).value();
  std::vector<QueryMemoryReservation> reservations;
  reservations.reserve(pins);
  for (auto _ : state) {
    static_cast<void>(_);
    for (std::size_t pin = 0U; pin < pins; ++pin)
      reservations.push_back(context.reserve(4'096U).value());
    benchmark::DoNotOptimize(reservations);
    reservations.clear();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(pins));
  state.counters["independent_pins"] = static_cast<double>(pins);
  state.counters["query_credit_bytes"] = static_cast<double>(pins * 4'096U);
}

BENCHMARK(reserve_and_release)->Arg(64)->Arg(4'096)->Arg(1U << 20U);
BENCHMARK(check_not_cancelled);
BENCHMARK(publish_shared_reservation)->Arg(1)->Arg(8)->Arg(64);
BENCHMARK(reserve_independent_pins)->Arg(1)->Arg(8)->Arg(64);

} // namespace
} // namespace chronos::query
