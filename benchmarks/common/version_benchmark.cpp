#include "chronos/common/version.hpp"

#include <benchmark/benchmark.h>
#include <string>

namespace {

void benchmark_version_json(benchmark::State& state) {
  constexpr chronos::common::VersionInfo kInfo{
      .semantic_version = "0.1.0-pre-alpha",
      .git_commit = "0123456789ab\"\\\n",
      .git_metadata_available = true,
      .git_dirty = true,
      .build_type = "Release",
      .compiler = "Clang 18.1.0",
      .target_architecture = "x86_64",
      .operating_system = "Linux",
  };

  for ([[maybe_unused]] auto _ : state) {
    std::string json = chronos::common::version_json(kInfo);
    benchmark::DoNotOptimize(json);
  }
  state.SetLabel("local measurement only; version JSON serialization");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_version_json);

} // namespace
