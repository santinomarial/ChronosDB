#include "chronos/ingest/tablet_movement_raft_snapshot_completion.hpp"
#include "ingest/raft_tablet_snapshot_storage_internal.hpp"
#include "ingest/tablet_snapshot_install_crash_fixture.hpp"
#include "ingest/tablet_snapshot_install_crash_protocol.hpp"
#include "io/posix_syscalls.hpp"
#include "raft/durable_runtime_internal.hpp"

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::ingest::test {
namespace {

struct ChildConfig {
  std::filesystem::path directory;
  std::string pause_after;
  std::uint64_t pause_occurrence{1U};
};

[[nodiscard]] ChildConfig parse_arguments(const int count, char** const values) {
  ChildConfig config;
  for (int index = 1; index + 1 < count; index += 2) {
    const std::string_view key{values[index]};
    if (key == "--directory")
      config.directory = values[index + 1];
    else if (key == "--pause-after")
      config.pause_after = values[index + 1];
    else if (key == "--pause-occurrence") {
      const std::string_view value{values[index + 1]};
      const auto parsed =
          std::from_chars(value.data(), value.data() + value.size(), config.pause_occurrence);
      if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size())
        config.pause_occurrence = 0U;
    }
  }
  return config;
}

class DelegatingSyscalls : public io::detail::PosixSyscalls {
public:
  explicit DelegatingSyscalls(io::detail::PosixSyscalls& delegate) : delegate_(delegate) {}

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
    return delegate_.pwrite(request);
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
    return delegate_.fsync(descriptor);
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    return delegate_.rename_no_replace(request);
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
    return delegate_.close(descriptor);
  }

protected:
  io::detail::PosixSyscalls& delegate_;
};

class ApplicationObservingSyscalls final : public DelegatingSyscalls {
public:
  ApplicationObservingSyscalls(io::detail::PosixSyscalls& delegate, std::string pause_after,
                               const std::uint64_t pause_occurrence)
      : DelegatingSyscalls(delegate), pause_after_(std::move(pause_after)),
        pause_occurrence_(pause_occurrence) {}

  void begin() noexcept {
    active_ = true;
  }

  int open_at(const io::detail::OpenAtRequest& request) override {
    const int result = delegate_.open_at(request);
    if (active_ && result >= 0)
      observe(kAfterApplicationTemporaryCreate);
    return result;
  }
  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    const ssize_t result = delegate_.pwrite(request);
    if (active_ && result >= 0 && static_cast<std::size_t>(result) == request.size)
      observe(kAfterApplicationWrite);
    return result;
  }
  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    const ssize_t result = delegate_.pread(request);
    if (active_ && result >= 0 && static_cast<std::size_t>(result) == request.size)
      observe(kAfterApplicationReadback);
    return result;
  }
  int fsync(const int descriptor) override {
    const int result = delegate_.fsync(descriptor);
    if (!active_ || result != 0)
      return result;
    struct stat metadata {};
    if (delegate_.fstat(descriptor, &metadata) == 0) {
      if (S_ISREG(metadata.st_mode))
        observe(kAfterApplicationFileSync);
      else if (S_ISDIR(metadata.st_mode)) {
        observe(kAfterApplicationDirectorySync);
        observe(kAfterApplicationAuthoritativeReclamationDirectorySync);
        observe(kAfterApplicationOrphanReclamationDirectorySync);
      }
    }
    return result;
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    const int result = delegate_.rename_no_replace(request);
    if (active_ && result == 0)
      observe(kAfterApplicationRename);
    return result;
  }
  int close(const int descriptor) override {
    const int result = delegate_.close(descriptor);
    if (active_ && result == 0)
      observe(kAfterApplicationTemporaryClose);
    return result;
  }

  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    const int result = delegate_.list_directory_entries(descriptor, entries);
    if (active_ && result == 0) {
      observe(kAfterApplicationAuthoritativeReclamationList);
      observe(kAfterApplicationOrphanReclamationList);
    }
    return result;
  }

  int unlink_at(const int directory_descriptor, const char* name) override {
    const int result = delegate_.unlink_at(directory_descriptor, name);
    if (active_ && result == 0 && std::string_view{name}.ends_with(".rtas")) {
      observe(kAfterApplicationAuthoritativeReclamationUnlink);
      observe(kAfterApplicationOrphanReclamationUnlink);
    }
    return result;
  }

  void observe_reclamation_success(const bool authoritative) {
    observe(authoritative ? kAfterApplicationAuthoritativeReclamationSuccess
                          : kAfterApplicationOrphanReclamationSuccess);
  }

private:
  void observe(const std::string_view point) {
    if (point != pause_after_)
      return;
    ++observed_occurrences_;
    if (observed_occurrences_ != pause_occurrence_)
      return;
    std::cout << "FAILPOINT " << point << '\n' << std::flush;
    for (;;)
      static_cast<void>(::pause());
  }

  std::string pause_after_;
  std::uint64_t pause_occurrence_;
  std::uint64_t observed_occurrences_{};
  bool active_{false};
};

class RaftObservingSyscalls final : public DelegatingSyscalls {
public:
  RaftObservingSyscalls(io::detail::PosixSyscalls& delegate, std::string pause_after)
      : DelegatingSyscalls(delegate), pause_after_(std::move(pause_after)) {}

  void begin() noexcept {
    active_ = true;
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    const ssize_t result = delegate_.pwrite(request);
    if (active_ && result >= 0 && static_cast<std::size_t>(result) == request.size)
      observe(kAfterRaftStateWrite);
    return result;
  }
  int fdatasync(const int descriptor) override {
    const int result = delegate_.fdatasync(descriptor);
    if (active_ && result == 0)
      observe(kAfterRaftStateSync);
    return result;
  }

  void observe_success_release() const {
    observe(kAfterSuccessRelease);
  }

private:
  void observe(const std::string_view point) const {
    if (point != pause_after_)
      return;
    std::cout << "FAILPOINT " << point << '\n' << std::flush;
    for (;;)
      static_cast<void>(::pause());
  }

  std::string pause_after_;
  bool active_{false};
};

[[nodiscard]] bool is_authoritative_reclamation_point(const std::string_view point) {
  return point.starts_with("after_application_authoritative_reclamation_");
}

[[nodiscard]] bool is_orphan_reclamation_point(const std::string_view point) {
  return point.starts_with("after_application_orphan_reclamation_");
}

[[nodiscard]] int run(const ChildConfig& config) {
  if (config.directory.empty() || config.pause_after.empty() || config.pause_occurrence == 0U)
    return 2;
  ApplicationObservingSyscalls application_syscalls{io::detail::system_posix_syscalls(),
                                                    config.pause_after, config.pause_occurrence};
  RaftObservingSyscalls raft_syscalls{io::detail::system_posix_syscalls(), config.pause_after};
  auto storage = detail::RaftTabletSnapshotStorageTestAccess::create(
      crash_snapshot_config(config.directory), application_syscalls);
  if (!storage.has_value())
    return 3;
  const bool authoritative_reclamation = is_authoritative_reclamation_point(config.pause_after);
  const bool orphan_reclamation = is_orphan_reclamation_point(config.pause_after);
  if (authoritative_reclamation || orphan_reclamation) {
    for (const raft::LogIndex index : {9U, 10U, 11U}) {
      auto installed = storage->install(crash_application_snapshot(index));
      if (!installed.has_value())
        return 4;
    }
    application_syscalls.begin();
    auto reclaimed = storage->reclaim_obsolete(
        authoritative_reclamation ? std::optional<raft::LogIndex>{10U} : std::nullopt);
    if (!reclaimed.has_value())
      return 5;
    application_syscalls.observe_reclamation_success(authoritative_reclamation);
    return 6;
  }
  auto runtime = raft::detail::DurableMultiRaftRuntimeTestAccess::create_new(
      4U, crash_log_config(config.directory), crash_groups(), {}, raft_syscalls);
  if (!runtime.has_value())
    return 7;
  const RaftTabletApplicationSnapshot expected = crash_application_snapshot();
  auto bytes = encode_raft_tablet_application_snapshot_v1(expected);
  if (!bytes.has_value())
    return 8;
  auto recovered = crash_recovered_movement(*bytes);
  if (!recovered.has_value())
    return 9;
  auto pending = request_crash_snapshot(*runtime, expected.raft_snapshot);
  if (!pending.has_value())
    return 10;

  application_syscalls.begin();
  raft_syscalls.begin();
  auto completed = complete_recovered_tablet_movement_raft_snapshot(*recovered, crash_table_id(),
                                                                    *storage, *pending, *runtime);
  if (!completed.has_value())
    return 11;
  raft_syscalls.observe_success_release();
  return 12;
}

} // namespace
} // namespace chronos::ingest::test

int main(const int argc, char** const argv) {
  try {
    return chronos::ingest::test::run(chronos::ingest::test::parse_arguments(argc, argv));
  } catch (const std::exception&) {
    return 70;
  } catch (...) {
    return 71;
  }
}
