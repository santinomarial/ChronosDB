#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

class TemporaryDirectory {
public:
  [[nodiscard]] static std::optional<TemporaryDirectory> create() {
    std::error_code error;
    const std::filesystem::path root = std::filesystem::temp_directory_path(error);
    if (error)
      return std::nullopt;
    std::string pattern = (root / "chronos-metadata-storage-benchmark-XXXXXX").string();
    if (::mkdtemp(pattern.data()) == nullptr)
      return std::nullopt;
    return TemporaryDirectory{std::filesystem::path{pattern}};
  }

  TemporaryDirectory(const TemporaryDirectory&) = delete;
  TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;
  TemporaryDirectory(TemporaryDirectory&& other) noexcept : path_(std::exchange(other.path_, {})) {}
  TemporaryDirectory& operator=(TemporaryDirectory&& other) noexcept {
    if (this != &other) {
      remove();
      path_ = std::exchange(other.path_, {});
    }
    return *this;
  }
  ~TemporaryDirectory() {
    remove();
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  explicit TemporaryDirectory(std::filesystem::path path) : path_(std::move(path)) {}

  void remove() noexcept {
    if (!path_.empty()) {
      std::error_code ignored;
      static_cast<void>(std::filesystem::remove_all(path_, ignored));
      path_.clear();
    }
  }

  std::filesystem::path path_;
};

[[nodiscard]] GroupId group() {
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{7U};
  return GroupId{group_bytes};
}

[[nodiscard]] MetadataApplicationSnapshot snapshot(const std::size_t entry_count) {
  SnapshotMetadata metadata{.last_included_index = entry_count,
                            .last_included_term = 3U,
                            .manifest_generation = entry_count,
                            .part_set_checksum = {},
                            .configuration_index = entry_count,
                            .voters = {1U, 2U, 3U, 4U, 5U, 6U, 7U, 8U, 9U}};
  metadata.part_set_checksum.fill(std::byte{0x5aU});
  MetadataApplicationSnapshot value{
      .group_id = group(), .raft_snapshot = std::move(metadata), .entries = {}};
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

[[nodiscard]] MetadataSnapshotStorageConfig storage_config(const TemporaryDirectory& directory) {
  return {.directory_path = directory.path().string(), .group_id = group()};
}

[[nodiscard]] bool validate_workload(benchmark::State& state,
                                     const MetadataApplicationSnapshot& expected,
                                     std::size_t& encoded_size) {
  const auto encoded = encode_metadata_application_snapshot_v1(expected);
  if (!encoded.has_value()) {
    state.SkipWithError(encoded.error().to_string());
    return false;
  }
  const auto decoded = decode_metadata_application_snapshot_v1(*encoded);
  if (!decoded.has_value() || *decoded != expected) {
    state.SkipWithError(decoded.has_value() ? "metadata snapshot workload changed"
                                            : decoded.error().to_string());
    return false;
  }
  encoded_size = encoded->size();
  return true;
}

void report_shape(benchmark::State& state, const MetadataApplicationSnapshot& expected,
                  const std::size_t encoded_size) {
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(expected.entries.size()));
  state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) *
                          static_cast<std::int64_t>(encoded_size));
  state.counters["entries"] = static_cast<double>(expected.entries.size());
  state.counters["payload_bytes_per_entry"] =
      static_cast<double>(expected.entries.front().payload.size());
  state.counters["snapshot_bytes"] = static_cast<double>(encoded_size);
}

void metadata_snapshot_durable_install(benchmark::State& state) {
  const MetadataApplicationSnapshot expected = snapshot(static_cast<std::size_t>(state.range(0)));
  std::size_t encoded_size{};
  if (!validate_workload(state, expected, encoded_size))
    return;

  for ([[maybe_unused]] auto iteration : state) {
    state.PauseTiming();
    {
      auto directory = TemporaryDirectory::create();
      if (!directory.has_value()) {
        state.ResumeTiming();
        state.SkipWithError("could not create metadata snapshot benchmark directory");
        return;
      }
      auto storage = MetadataSnapshotStorage::create(storage_config(*directory));
      if (!storage.has_value()) {
        const std::string error = storage.error().to_string();
        state.ResumeTiming();
        state.SkipWithError(error);
        return;
      }
      state.ResumeTiming();

      const auto installed = storage->install(expected);

      state.PauseTiming();
      if (!installed.has_value() || installed->already_present) {
        const std::string error = installed.has_value() ? "fresh snapshot was already present"
                                                        : installed.error().to_string();
        state.ResumeTiming();
        state.SkipWithError(error);
        return;
      }
    }
    state.ResumeTiming();
  }
  report_shape(state, expected, encoded_size);
  state.SetLabel(
      "local measurement only; exact install, readback, file sync, rename, directory sync");
}

void metadata_snapshot_restart_recovery(benchmark::State& state) {
  const MetadataApplicationSnapshot expected = snapshot(static_cast<std::size_t>(state.range(0)));
  std::size_t encoded_size{};
  if (!validate_workload(state, expected, encoded_size))
    return;
  auto directory = TemporaryDirectory::create();
  if (!directory.has_value()) {
    state.SkipWithError("could not create metadata snapshot benchmark directory");
    return;
  }
  {
    auto storage = MetadataSnapshotStorage::create(storage_config(*directory));
    if (!storage.has_value()) {
      state.SkipWithError(storage.error().to_string());
      return;
    }
    const auto installed = storage->install(expected);
    if (!installed.has_value()) {
      state.SkipWithError(installed.error().to_string());
      return;
    }
  }

  for ([[maybe_unused]] auto iteration : state) {
    {
      auto storage = MetadataSnapshotStorage::open_existing(storage_config(*directory));
      if (!storage.has_value()) {
        state.SkipWithError(storage.error().to_string());
        return;
      }
      const auto latest = storage->load_latest();
      state.PauseTiming();
      if (!latest.has_value()) {
        const std::string error = latest.error().to_string();
        state.ResumeTiming();
        state.SkipWithError(error);
        return;
      }
      const std::optional<LoadedMetadataSnapshot>& selected = *latest;
      if (!selected.has_value()) {
        state.ResumeTiming();
        state.SkipWithError("recovered metadata snapshot is missing");
        return;
      }
      const LoadedMetadataSnapshot& loaded = selected.value();
      if (loaded.snapshot.entries.size() != expected.entries.size() ||
          loaded.bytes.size() != encoded_size) {
        state.ResumeTiming();
        state.SkipWithError("recovered metadata snapshot changed");
        return;
      }
      benchmark::DoNotOptimize(loaded.snapshot.entries.data());
    }
    state.ResumeTiming();
  }
  report_shape(state, expected, encoded_size);
  state.SetLabel("local measurement only; lock, discovery, checked owned load after owner restart");
}

// Google Benchmark intentionally registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(metadata_snapshot_durable_install)->Arg(1'024)->Arg(16'384)->Arg(65'536)->UseRealTime();
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(metadata_snapshot_restart_recovery)->Arg(1'024)->Arg(16'384)->Arg(65'536)->UseRealTime();

} // namespace
} // namespace chronos::raft
