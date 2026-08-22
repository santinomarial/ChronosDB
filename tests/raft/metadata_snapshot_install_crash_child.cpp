#include "io/posix_syscalls.hpp"
#include "raft/metadata_snapshot_install_crash_fixture.hpp"
#include "raft/metadata_snapshot_install_crash_protocol.hpp"
#include "raft/metadata_snapshot_storage_internal.hpp"

#include <cstddef>
#include <exception>
#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft::test {
namespace {

struct ChildConfig {
  std::filesystem::path directory;
  std::string pause_after;
};

[[nodiscard]] ChildConfig parse_arguments(const int count, char** const values) {
  ChildConfig config;
  for (int index = 1; index + 1 < count; index += 2) {
    const std::string_view key{values[index]};
    if (key == "--directory")
      config.directory = values[index + 1];
    else if (key == "--pause-after")
      config.pause_after = values[index + 1];
  }
  return config;
}

class ObservingSyscalls final : public io::detail::PosixSyscalls {
public:
  ObservingSyscalls(io::detail::PosixSyscalls& delegate, std::string pause_after)
      : delegate_(delegate), pause_after_(std::move(pause_after)) {}

  void begin() noexcept {
    active_ = true;
  }

  int open_directory(const char* path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }

  int open_at(const io::detail::OpenAtRequest& request) override {
    const int result = delegate_.open_at(request);
    if (active_ && result >= 0)
      observe(kAfterMetadataTemporaryCreate);
    return result;
  }

  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }

  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    const ssize_t result = delegate_.pread(request);
    if (active_ && result >= 0 && static_cast<std::size_t>(result) == request.size)
      observe(kAfterMetadataReadback);
    return result;
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    const ssize_t result = delegate_.pwrite(request);
    if (active_ && result >= 0 && static_cast<std::size_t>(result) == request.size)
      observe(kAfterMetadataWrite);
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
    if (!active_ || result != 0)
      return result;
    struct stat metadata {};
    if (delegate_.fstat(descriptor, &metadata) == 0) {
      if (S_ISREG(metadata.st_mode))
        observe(kAfterMetadataFileSync);
      else if (S_ISDIR(metadata.st_mode))
        observe(kAfterMetadataDirectorySync);
    }
    return result;
  }

  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    const int result = delegate_.rename_no_replace(request);
    if (active_ && result == 0)
      observe(kAfterMetadataRename);
    return result;
  }

  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }

  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }

  int unlink_at(const int directory_descriptor, const char* name) override {
    return delegate_.unlink_at(directory_descriptor, name);
  }

  int close(const int descriptor) override {
    const int result = delegate_.close(descriptor);
    if (active_ && result == 0)
      observe(kAfterMetadataTemporaryClose);
    return result;
  }

  void observe_success_release() const {
    observe(kAfterMetadataSuccessRelease);
  }

private:
  void observe(const std::string_view point) const {
    if (point != pause_after_)
      return;
    std::cout << "FAILPOINT " << point << '\n' << std::flush;
    for (;;)
      static_cast<void>(::pause());
  }

  io::detail::PosixSyscalls& delegate_;
  std::string pause_after_;
  bool active_{false};
};

[[nodiscard]] int run(const ChildConfig& config) {
  if (config.directory.empty() || config.pause_after.empty())
    return 2;
  ObservingSyscalls syscalls{io::detail::system_posix_syscalls(), config.pause_after};
  auto storage = detail::MetadataSnapshotStorageTestAccess::create(
      metadata_crash_storage_config(config.directory), syscalls);
  if (!storage.has_value())
    return 3;
  syscalls.begin();
  auto installed = storage->install(metadata_crash_snapshot());
  if (!installed.has_value())
    return 4;
  syscalls.observe_success_release();
  return 5;
}

} // namespace
} // namespace chronos::raft::test

int main(const int argc, char** const argv) {
  try {
    return chronos::raft::test::run(chronos::raft::test::parse_arguments(argc, argv));
  } catch (const std::exception&) {
    return 70;
  } catch (...) {
    return 71;
  }
}
