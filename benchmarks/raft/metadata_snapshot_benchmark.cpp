#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] MetadataApplicationSnapshot snapshot(const std::size_t entry_count) {
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{7U};
  SnapshotMetadata metadata{.last_included_index = entry_count,
                            .last_included_term = 3U,
                            .manifest_generation = entry_count,
                            .part_set_checksum = {},
                            .configuration_index = entry_count,
                            .voters = {1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U}};
  metadata.part_set_checksum.fill(std::byte{0x5aU});
  MetadataApplicationSnapshot value{
      .group_id = GroupId{group_bytes}, .raft_snapshot = std::move(metadata), .entries = {}};
  const std::vector<std::byte> payload =
      encode_metadata_command_v1(ClusterNodeMetadata{7U, "n"}).value();
  value.entries.reserve(entry_count);
  for (std::size_t ordinal = 0U; ordinal < entry_count; ++ordinal) {
    const Term term = 1U + static_cast<Term>((ordinal * 3U) / entry_count);
    value.entries.push_back({.index = ordinal + 1U,
                             .term = term,
                             .type = kRaftMetadataCommandEntryType,
                             .payload = payload});
  }
  value.entries.back().term = value.raft_snapshot.last_included_term;
  return value;
}

[[nodiscard]] bool validate_workload(benchmark::State& state,
                                     const MetadataApplicationSnapshot& expected,
                                     std::vector<std::byte>& encoded) {
  auto encoded_result = encode_metadata_application_snapshot_v1(expected);
  if (!encoded_result.has_value()) {
    state.SkipWithError(encoded_result.error().to_string());
    return false;
  }
  encoded = std::move(*encoded_result);
  auto decoded = decode_metadata_application_snapshot_v1(encoded);
  if (!decoded.has_value()) {
    state.SkipWithError(decoded.error().to_string());
    return false;
  }
  if (*decoded != expected) {
    state.SkipWithError("metadata snapshot workload did not round-trip exactly");
    return false;
  }
  return true;
}

void report_shape(benchmark::State& state, const std::size_t entry_count,
                  const std::size_t payload_size, const std::size_t encoded_size) {
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(entry_count));
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(encoded_size));
  state.counters["entries"] = static_cast<double>(entry_count);
  state.counters["payload_bytes_per_entry"] = static_cast<double>(payload_size);
  state.counters["snapshot_bytes"] = static_cast<double>(encoded_size);
}

void metadata_snapshot_encode(benchmark::State& state) {
  const std::size_t entry_count = static_cast<std::size_t>(state.range(0));
  const MetadataApplicationSnapshot expected = snapshot(entry_count);
  std::vector<std::byte> example;
  if (!validate_workload(state, expected, example))
    return;
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = encode_metadata_application_snapshot_v1(expected);
    if (!encoded.has_value() || encoded->size() != example.size()) {
      const std::string error = encoded.has_value() ? "metadata snapshot encode changed size"
                                                    : encoded.error().to_string();
      state.SkipWithError(error);
      return;
    }
    benchmark::DoNotOptimize(encoded->data());
  }
  report_shape(state, entry_count, expected.entries.front().payload.size(), example.size());
  state.SetLabel("local measurement only; canonical metadata application snapshot encode");
}

void metadata_snapshot_decode(benchmark::State& state) {
  const std::size_t entry_count = static_cast<std::size_t>(state.range(0));
  const MetadataApplicationSnapshot expected = snapshot(entry_count);
  std::vector<std::byte> encoded;
  if (!validate_workload(state, expected, encoded))
    return;
  for ([[maybe_unused]] auto iteration : state) {
    auto decoded = decode_metadata_application_snapshot_v1(encoded);
    if (!decoded.has_value() || decoded->entries.size() != entry_count) {
      const std::string error = decoded.has_value() ? "metadata snapshot decode changed entry count"
                                                    : decoded.error().to_string();
      state.SkipWithError(error);
      return;
    }
    benchmark::DoNotOptimize(decoded->entries.data());
  }
  report_shape(state, entry_count, expected.entries.front().payload.size(), encoded.size());
  state.SetLabel("local measurement only; checked owned metadata application snapshot decode");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(metadata_snapshot_encode)->Arg(1'024)->Arg(16'384)->Arg(65'536);
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(metadata_snapshot_decode)->Arg(1'024)->Arg(16'384)->Arg(65'536);

} // namespace
} // namespace chronos::raft
