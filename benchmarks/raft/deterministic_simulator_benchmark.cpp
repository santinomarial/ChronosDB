#include "chronos/raft/deterministic_simulator.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::uint64_t kScheduleSeed = 20'260'821U;

struct WorkloadShape {
  std::size_t node_count{};
  std::size_t action_count{};
};

[[nodiscard]] RaftSimulationConfig workload_config(const WorkloadShape shape) {
  RaftSimulationConfig config;
  config.node_ids.reserve(shape.node_count);
  config.initial_voters.reserve(shape.node_count);
  for (std::size_t index = 0U; index < shape.node_count; ++index) {
    const NodeId node_id = static_cast<NodeId>(index) + 1U;
    config.node_ids.push_back(node_id);
    config.initial_voters.push_back(node_id);
  }
  config.limits.maximum_trace_actions = shape.action_count;
  return config;
}

void seeded_simulation(benchmark::State& state) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  const auto action_count = static_cast<std::size_t>(state.range(1));
  const RaftSimulationConfig config =
      workload_config({.node_count = node_count, .action_count = action_count});
  std::uint64_t safety_checks = 0U;

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    {
      auto simulation = DeterministicRaftSimulator::create(config);
      if (!simulation.has_value()) {
        const std::string error = simulation.error().to_string();
        state.ResumeTiming();
        state.SkipWithError(error);
        return;
      }
      state.ResumeTiming();
      const common::Status status =
          simulation->run_seeded({.seed = kScheduleSeed, .actions = action_count});
      state.PauseTiming();
      if (!status.is_ok() || simulation->stats().actions != action_count) {
        const std::string error =
            status.is_ok() ? "seeded schedule retained the wrong action count" : status.to_string();
        state.ResumeTiming();
        state.SkipWithError(error);
        return;
      }
      safety_checks = simulation->stats().safety_checks;
      benchmark::DoNotOptimize(safety_checks);
    }
    state.ResumeTiming();
  }

  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(action_count));
  state.counters["actions_per_iteration"] = static_cast<double>(action_count);
  state.counters["nodes"] = static_cast<double>(node_count);
  state.counters["safety_checks_per_iteration"] = static_cast<double>(safety_checks);
  state.counters["seed"] = static_cast<double>(kScheduleSeed);
  state.SetLabel("local measurement only; generation and safety oracle timed; simulator lifetime "
                 "excluded");
}

void replay_simulation(benchmark::State& state) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  const auto action_count = static_cast<std::size_t>(state.range(1));
  const RaftSimulationConfig config =
      workload_config({.node_count = node_count, .action_count = action_count});
  auto generator = DeterministicRaftSimulator::create(config);
  if (!generator.has_value()) {
    state.SkipWithError(generator.error().to_string());
    return;
  }
  const common::Status generation_status =
      generator->run_seeded({.seed = kScheduleSeed, .actions = action_count});
  if (!generation_status.is_ok() || generator->trace().size() != action_count) {
    state.SkipWithError(generation_status.is_ok() ? "trace generation retained the wrong size"
                                                  : generation_status.to_string());
    return;
  }
  const std::vector<RaftSimulationAction> trace(generator->trace().begin(),
                                                generator->trace().end());
  std::uint64_t safety_checks = 0U;

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    {
      auto simulation = DeterministicRaftSimulator::create(config);
      if (!simulation.has_value()) {
        const std::string error = simulation.error().to_string();
        state.ResumeTiming();
        state.SkipWithError(error);
        return;
      }
      state.ResumeTiming();
      const common::Status status = simulation->replay(trace);
      state.PauseTiming();
      if (!status.is_ok() || simulation->stats().actions != action_count) {
        const std::string error =
            status.is_ok() ? "replay retained the wrong action count" : status.to_string();
        state.ResumeTiming();
        state.SkipWithError(error);
        return;
      }
      safety_checks = simulation->stats().safety_checks;
      benchmark::DoNotOptimize(safety_checks);
    }
    state.ResumeTiming();
  }

  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(action_count));
  state.counters["actions_per_iteration"] = static_cast<double>(action_count);
  state.counters["nodes"] = static_cast<double>(node_count);
  state.counters["safety_checks_per_iteration"] = static_cast<double>(safety_checks);
  state.counters["seed"] = static_cast<double>(kScheduleSeed);
  state.SetLabel("local measurement only; fixed-trace copy and safety oracle timed; trace "
                 "generation and simulator lifetime excluded");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(seeded_simulation)->Args({3, 1'024})->Args({5, 1'024});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(replay_simulation)->Args({3, 1'024})->Args({5, 1'024});

} // namespace
} // namespace chronos::raft
