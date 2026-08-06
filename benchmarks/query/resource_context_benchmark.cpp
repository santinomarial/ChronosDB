#include "chronos/query/resource_context.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>

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

BENCHMARK(reserve_and_release)->Arg(64)->Arg(4'096)->Arg(1U << 20U);
BENCHMARK(check_not_cancelled);

} // namespace
} // namespace chronos::query
