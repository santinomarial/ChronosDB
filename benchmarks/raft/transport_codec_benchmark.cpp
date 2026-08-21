#include "chronos/raft/transport_codec.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

struct AppendShape {
  std::size_t entry_count{};
  std::size_t payload_bytes{};
};

[[nodiscard]] GroupId benchmark_group_id() {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{0x42U};
  return GroupId{bytes};
}

[[nodiscard]] RaftTransportEnvelope route(Message message) {
  return {benchmark_group_id(), 1U, 2U, std::move(message)};
}

[[nodiscard]] RaftTransportEnvelope append_envelope(const AppendShape shape) {
  std::vector<LogEntry> entries;
  entries.reserve(shape.entry_count);
  for (std::size_t ordinal = 0U; ordinal < shape.entry_count; ++ordinal) {
    std::vector<std::byte> payload(shape.payload_bytes);
    for (std::size_t offset = 0U; offset < payload.size(); ++offset)
      payload[offset] = static_cast<std::byte>((ordinal * 131U + offset * 17U) & 0xffU);
    entries.push_back({.index = static_cast<LogIndex>(ordinal) + 8U,
                       .term = 4U,
                       .type = static_cast<std::uint8_t>(ordinal % 253U + 1U),
                       .payload = std::move(payload)});
  }
  return route(AppendEntriesRequest{.term = 4U,
                                    .leader_id = 1U,
                                    .previous_log_index = 7U,
                                    .previous_log_term = 3U,
                                    .entries = std::move(entries),
                                    .leader_commit = 7U});
}

[[nodiscard]] RaftTransportEnvelope snapshot_envelope(const std::size_t voter_count) {
  SnapshotMetadata snapshot;
  snapshot.last_included_index = 8U;
  snapshot.last_included_term = 3U;
  snapshot.manifest_generation = 11U;
  snapshot.part_set_checksum.fill(std::byte{0x5aU});
  snapshot.configuration_index = 7U;
  snapshot.voters.reserve(voter_count);
  for (std::size_t ordinal = 0U; ordinal < voter_count; ++ordinal)
    snapshot.voters.push_back(static_cast<NodeId>(ordinal) + 1U);
  return route(
      InstallSnapshotRequest{.term = 4U, .leader_id = 1U, .snapshot = std::move(snapshot)});
}

[[nodiscard]] bool validate_workload(benchmark::State& state, const RaftTransportEnvelope& envelope,
                                     std::vector<std::byte>& encoded) {
  auto encoded_result = encode_raft_transport_envelope_v1(envelope);
  if (!encoded_result.has_value()) {
    state.SkipWithError(encoded_result.error().to_string());
    return false;
  }
  encoded = std::move(*encoded_result);
  auto decoded = decode_raft_transport_envelope_v1(encoded);
  if (!decoded.has_value()) {
    state.SkipWithError(decoded.error().to_string());
    return false;
  }
  if (*decoded != envelope) {
    state.SkipWithError("transport workload did not round-trip exactly");
    return false;
  }
  return true;
}

void measure_encode(benchmark::State& state, const RaftTransportEnvelope& envelope,
                    const std::string_view label) {
  std::vector<std::byte> example;
  if (!validate_workload(state, envelope, example))
    return;
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = encode_raft_transport_envelope_v1(envelope);
    if (!encoded.has_value() || encoded->size() != example.size()) {
      state.SkipWithError(encoded.has_value() ? "transport encode changed frame size"
                                              : encoded.error().to_string());
      return;
    }
    benchmark::DoNotOptimize(encoded->data());
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(example.size()));
  state.counters["frame_bytes"] = static_cast<double>(example.size());
  state.SetLabel(std::string{label});
}

void measure_decode(benchmark::State& state, const RaftTransportEnvelope& envelope,
                    const std::string_view label) {
  std::vector<std::byte> encoded;
  if (!validate_workload(state, envelope, encoded))
    return;
  for ([[maybe_unused]] auto iteration : state) {
    auto decoded = decode_raft_transport_envelope_v1(encoded);
    if (!decoded.has_value()) {
      state.SkipWithError(decoded.error().to_string());
      return;
    }
    benchmark::DoNotOptimize(decoded->message.index());
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(encoded.size()));
  state.counters["frame_bytes"] = static_cast<double>(encoded.size());
  state.SetLabel(std::string{label});
}

void vote_response_encode(benchmark::State& state) {
  measure_encode(state, route(RequestVoteResponse{.term = 4U, .granted = true}),
                 "local measurement only; canonical vote-response encode with CRC32C");
}

void vote_response_decode(benchmark::State& state) {
  measure_decode(state, route(RequestVoteResponse{.term = 4U, .granted = true}),
                 "local measurement only; checked owned vote-response decode");
}

void append_entries_encode(benchmark::State& state) {
  const AppendShape shape{.entry_count = static_cast<std::size_t>(state.range(0)),
                          .payload_bytes = static_cast<std::size_t>(state.range(1))};
  measure_encode(state, append_envelope(shape),
                 "local measurement only; canonical AppendEntries encode with CRC32C");
  state.counters["entries"] = static_cast<double>(shape.entry_count);
  state.counters["payload_bytes_per_entry"] = static_cast<double>(shape.payload_bytes);
}

void append_entries_decode(benchmark::State& state) {
  const AppendShape shape{.entry_count = static_cast<std::size_t>(state.range(0)),
                          .payload_bytes = static_cast<std::size_t>(state.range(1))};
  measure_decode(state, append_envelope(shape),
                 "local measurement only; checked owned AppendEntries decode");
  state.counters["entries"] = static_cast<double>(shape.entry_count);
  state.counters["payload_bytes_per_entry"] = static_cast<double>(shape.payload_bytes);
}

void snapshot_request_encode(benchmark::State& state) {
  const auto voters = static_cast<std::size_t>(state.range(0));
  measure_encode(state, snapshot_envelope(voters),
                 "local measurement only; canonical snapshot-metadata encode with CRC32C");
  state.counters["voters"] = static_cast<double>(voters);
}

void snapshot_request_decode(benchmark::State& state) {
  const auto voters = static_cast<std::size_t>(state.range(0));
  measure_decode(state, snapshot_envelope(voters),
                 "local measurement only; checked owned snapshot-metadata decode");
  state.counters["voters"] = static_cast<double>(voters);
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(vote_response_encode);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(vote_response_decode);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(append_entries_encode)->Args({0, 0})->Args({1, 128})->Args({32, 4'096});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(append_entries_decode)->Args({0, 0})->Args({1, 128})->Args({32, 4'096});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(snapshot_request_encode)->Arg(3)->Arg(5);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(snapshot_request_decode)->Arg(3)->Arg(5);

} // namespace
} // namespace chronos::raft
