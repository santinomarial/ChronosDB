#include "chronos/manifest/compaction_coordinator.hpp"
#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/publication.hpp"
#include "chronos/manifest/storage.hpp"
#include "io/posix_syscalls.hpp"
#include "manifest/manifest_flush_crash_fixture.hpp"
#include "manifest/manifest_flush_crash_protocol.hpp"
#include "manifest/storage_internal.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::manifest::test {
namespace {

struct ChildConfig {
  std::string directory;
  std::string pause_after;
  bool compaction{false};
  bool part_reclamation{false};
};

[[nodiscard]] ChildConfig parse_arguments(const int count, char** const values) {
  ChildConfig config;
  for (int index = 1; index + 1 < count; index += 2) {
    const std::string_view key{values[index]};
    if (key == "--directory") {
      config.directory = values[index + 1];
    } else if (key == "--pause-after") {
      config.pause_after = values[index + 1];
    } else if (key == "--operation") {
      config.compaction = std::string_view{values[index + 1]} == "compaction";
      config.part_reclamation = std::string_view{values[index + 1]} == "part_reclamation";
    }
  }
  return config;
}

void write_bytes(const std::filesystem::path& path, const common::ByteView bytes) {
  std::ofstream output{path, std::ios::binary};
  // std::ofstream has no std::byte overload.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output.write(reinterpret_cast<const char*>(bytes.data()),
               static_cast<std::streamsize>(bytes.size()));
  output.flush();
}

class ObservingSyscalls final : public io::detail::PosixSyscalls {
public:
  ObservingSyscalls(io::detail::PosixSyscalls& delegate, std::string pause_after)
      : delegate_(delegate), pause_after_(std::move(pause_after)) {}

  int open_directory(const char* path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }
  int open_at(const io::detail::OpenAtRequest& request) override {
    return delegate_.open_at(request);
  }
  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }
  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    const ssize_t result = delegate_.pread(request);
    if (result >= 0 && static_cast<std::size_t>(result) == request.size) {
      if (writes_ == 1U && !part_readback_observed_) {
        part_readback_observed_ = true;
        observe(kAfterPartReadback);
      } else if (writes_ == 2U && !manifest_readback_observed_) {
        manifest_readback_observed_ = true;
        observe(kAfterManifestReadback);
      }
    }
    return result;
  }
  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    const ssize_t result = delegate_.pwrite(request);
    if (result >= 0 && static_cast<std::size_t>(result) == request.size) {
      ++writes_;
      observe(writes_ == 1U ? kAfterPartWrite : kAfterManifestWrite);
    }
    return result;
  }
  int fstat(const int descriptor, struct stat* metadata) override {
    return delegate_.fstat(descriptor, metadata);
  }
  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }
  int fdatasync(const int descriptor) override {
    return delegate_.fdatasync(descriptor);
  }
  int fsync(const int descriptor) override {
    const int result = delegate_.fsync(descriptor);
    if (result == 0) {
      if (reclamation_started_) {
        observe(kAfterPartReclamationDirectorySync);
        return result;
      }
      ++syncs_;
      switch (syncs_) {
      case 1U:
        observe(kAfterPartFileSync);
        break;
      case 2U:
        observe(kAfterPartsDirectorySync);
        break;
      case 3U:
        observe(kAfterManifestFileSync);
        break;
      case 4U:
        observe(kAfterManifestDirectorySync);
        break;
      default:
        break;
      }
    }
    return result;
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    const int result = delegate_.rename_no_replace(request);
    if (result == 0) {
      ++renames_;
      observe(renames_ == 1U ? kAfterPartRename : kAfterManifestRename);
    }
    return result;
  }
  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }
  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }
  int unlink_at(const int descriptor, const char* name) override {
    const int result = delegate_.unlink_at(descriptor, name);
    if (result == 0) {
      reclamation_started_ = true;
      observe(kAfterPartReclamationUnlink);
    }
    return result;
  }
  int close(const int descriptor) override {
    return delegate_.close(descriptor);
  }

  void observe_publication() const {
    observe(kAfterPublication);
  }

private:
  void observe(const std::string_view point) const {
    if (point != pause_after_) {
      return;
    }
    std::cout << "FAILPOINT " << point << '\n' << std::flush;
    for (;;) {
      static_cast<void>(::pause());
    }
  }

  io::detail::PosixSyscalls& delegate_;
  std::string pause_after_;
  std::uint64_t writes_{};
  std::uint64_t syncs_{};
  std::uint64_t renames_{};
  bool part_readback_observed_{false};
  bool manifest_readback_observed_{false};
  bool reclamation_started_{false};
};

[[nodiscard]] int run_flush(const ChildConfig& config, const std::filesystem::path& root,
                            ObservingSyscalls& syscalls) {
  const ManifestFlushCrashFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(1U);
  write_bytes(root / kManifestDirectoryName / *manifest_file_name(1U), predecessor.bytes());

  common::Result<ManifestStorage> opened = detail::ManifestStorageTestAccess::open_existing(
      {.database_root = config.directory}, syscalls);
  if (!opened.has_value()) {
    return 5;
  }
  ManifestStorage storage = std::move(*opened);
  const common::Result<InstalledPart> part =
      storage.install_part({.encoded_part = std::cref(fixture.encoded),
                            .descriptor = fixture.descriptor,
                            .wal_id = fixture.wal_id,
                            .schema = std::cref(fixture.schema_value),
                            .nonce = crash_nonce(0xa0U),
                            .validation_limits = {}});
  if (!part.has_value()) {
    return 6;
  }
  const EncodedManifest successor = fixture.manifest(2U);
  const auto bindings = fixture.bindings();
  const common::Result<InstalledManifest> manifest =
      storage.install_manifest({.encoded_manifest = std::cref(successor),
                                .schema_bindings = bindings,
                                .nonce = crash_nonce(0xb0U),
                                .decode_limits = {},
                                .part_validation_limits = {},
                                .compaction_equivalence_limits = {}});
  return manifest.has_value() ? 0 : 7;
}

[[nodiscard]] int run_compaction(const ChildConfig& config, const std::filesystem::path& root,
                                 ObservingSyscalls& syscalls, const bool reclaim) {
  const ManifestFlushCrashFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(2U);
  write_bytes(root / kPartsDirectoryName / part_file_name(fixture.part_id),
              fixture.encoded.bytes());
  write_bytes(root / kManifestDirectoryName / *manifest_file_name(1U),
              fixture.manifest(1U).bytes());
  write_bytes(root / kManifestDirectoryName / *manifest_file_name(2U), predecessor.bytes());

  common::Result<ManifestStorage> opened = detail::ManifestStorageTestAccess::open_existing(
      {.database_root = config.directory}, syscalls);
  if (!opened.has_value()) {
    return 8;
  }
  ManifestStorage storage = std::move(*opened);
  const auto bindings = fixture.bindings();
  common::Result<LoadedManifestGeneration> loaded =
      storage.load_selected_manifest({.expected_database_id = fixture.database_id,
                                      .expected_wal_id = fixture.wal_id,
                                      .schema_bindings = bindings,
                                      .decode_limits = {},
                                      .part_validation_limits = {}});
  if (!loaded.has_value()) {
    return 9;
  }
  auto selected = std::make_shared<const LoadedManifestGeneration>(std::move(*loaded));
  common::Result<DatabaseStoragePublisher> publisher =
      DatabaseStoragePublisher::create(selected, {});
  if (!publisher.has_value()) {
    return 10;
  }
  common::Result<AppendOnlyCompactionCoordinator> coordinator =
      AppendOnlyCompactionCoordinator::create(storage, *publisher);
  if (!coordinator.has_value()) {
    return 11;
  }
  const std::array inputs{fixture.part_id};
  const common::Result<AppendOnlyCompactionCompletion> completed =
      coordinator->compact({.tablet_id = fixture.tablet_id,
                            .input_part_ids = inputs,
                            .output_part_id = crash_id<cseg::PartId>(9U),
                            .part_nonce = crash_nonce(0xc0U),
                            .manifest_nonce = crash_nonce(0xd0U),
                            .compression = cseg::PageCompression::kZstd,
                            .schema_bindings = bindings,
                            .manifest_decode_limits = {},
                            .part_validation_limits = {},
                            .compaction_limits = {}});
  if (!completed.has_value()) {
    return 12;
  }
  syscalls.observe_publication();
  if (completed->manifest_generation != 3U) {
    return 13;
  }
  if (!reclaim) {
    return 0;
  }
  common::Result<std::vector<RetiredPartSet>> retirements = publisher->drain_retired_part_sets();
  if (!retirements.has_value() || retirements->size() != 1U || retirements->front().is_pinned()) {
    return 14;
  }
  common::Result<LoadedManifestGeneration> current =
      storage.load_selected_manifest({.expected_database_id = fixture.database_id,
                                      .expected_wal_id = fixture.wal_id,
                                      .schema_bindings = bindings,
                                      .decode_limits = {},
                                      .part_validation_limits = {}});
  if (!current.has_value()) {
    return 15;
  }
  const common::Result<PartReclamationReport> reclaimed =
      storage.reclaim_retired_parts({.selected_manifest = std::cref(*current),
                                     .retirement = std::cref(retirements->front()),
                                     .decode_limits = {}});
  return reclaimed.has_value() && reclaimed->removed_parts == 1U ? 0 : 16;
}

[[nodiscard]] int run(const ChildConfig& config) {
  if (config.directory.empty() || config.pause_after.empty()) {
    return 2;
  }
  const std::filesystem::path root{config.directory};
  std::error_code error;
  if (!std::filesystem::create_directory(root / kPartsDirectoryName, error) || error) {
    return 3;
  }
  if (!std::filesystem::create_directory(root / kManifestDirectoryName, error) || error) {
    return 4;
  }
  write_bytes(root / kManifestDirectoryName / std::string{kManifestLockFileName}, {});
  ObservingSyscalls syscalls{io::detail::system_posix_syscalls(), config.pause_after};
  return config.compaction || config.part_reclamation
             ? run_compaction(config, root, syscalls, config.part_reclamation)
             : run_flush(config, root, syscalls);
}

} // namespace
} // namespace chronos::manifest::test

int main(const int argc, char** const argv) {
  try {
    return chronos::manifest::test::run(chronos::manifest::test::parse_arguments(argc, argv));
  } catch (const std::exception&) {
    return 70;
  } catch (...) {
    return 71;
  }
}
