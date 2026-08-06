#include "chronos/manifest/naming.hpp"
#include "chronos/manifest/storage.hpp"
#include "io/posix_syscalls.hpp"
#include "manifest/manifest_flush_crash_fixture.hpp"
#include "manifest/manifest_flush_crash_protocol.hpp"
#include "manifest/storage_internal.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
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
};

[[nodiscard]] ChildConfig parse_arguments(const int count, char** const values) {
  ChildConfig config;
  for (int index = 1; index + 1 < count; index += 2) {
    const std::string_view key{values[index]};
    if (key == "--directory") {
      config.directory = values[index + 1];
    } else if (key == "--pause-after") {
      config.pause_after = values[index + 1];
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
    return delegate_.pread(request);
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
    return delegate_.unlink_at(descriptor, name);
  }
  int close(const int descriptor) override {
    return delegate_.close(descriptor);
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
};

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
  const ManifestFlushCrashFixture fixture;
  const EncodedManifest predecessor = fixture.manifest(1U);
  write_bytes(root / kManifestDirectoryName / *manifest_file_name(1U), predecessor.bytes());

  ObservingSyscalls syscalls{io::detail::system_posix_syscalls(), config.pause_after};
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
                                .part_validation_limits = {}});
  return manifest.has_value() ? 0 : 7;
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
