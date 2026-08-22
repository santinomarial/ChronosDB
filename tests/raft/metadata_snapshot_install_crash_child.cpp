#include "chronos/raft/metadata_runtime.hpp"
#include "io/posix_syscalls.hpp"
#include "raft/durable_runtime_internal.hpp"
#include "raft/metadata_snapshot_install_crash_fixture.hpp"
#include "raft/metadata_snapshot_install_crash_protocol.hpp"
#include "raft/metadata_snapshot_storage_internal.hpp"

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
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::raft::test {
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

enum class ObservedDomain : std::uint8_t { kApplication, kRaft };

class ObservingSyscalls final : public io::detail::PosixSyscalls {
public:
  ObservingSyscalls(io::detail::PosixSyscalls& delegate, std::string pause_after,
                    const std::uint64_t pause_occurrence, const ObservedDomain domain)
      : delegate_(delegate), pause_after_(std::move(pause_after)),
        pause_occurrence_(pause_occurrence), domain_(domain) {}

  void begin() noexcept {
    active_ = true;
  }

  int open_directory(const char* path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }

  int open_at(const io::detail::OpenAtRequest& request) override {
    const int result = delegate_.open_at(request);
    if (active_ && domain_ == ObservedDomain::kApplication && result >= 0) {
      observe(kAfterMetadataTemporaryCreate);
      observe(kAfterMetadataCompactionTemporaryCreate);
    }
    return result;
  }

  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }

  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    const ssize_t result = delegate_.pread(request);
    if (active_ && domain_ == ObservedDomain::kApplication && result >= 0 &&
        static_cast<std::size_t>(result) == request.size) {
      observe(kAfterMetadataReadback);
      observe(kAfterMetadataCompactionReadback);
    }
    return result;
  }

  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    const ssize_t result = delegate_.pwrite(request);
    if (active_ && result >= 0 && static_cast<std::size_t>(result) == request.size) {
      if (domain_ == ObservedDomain::kApplication) {
        observe(kAfterMetadataWrite);
        observe(kAfterMetadataCompactionWrite);
      } else {
        observe(kAfterMetadataCompactionRaftWrite);
      }
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
    const int result = delegate_.fdatasync(descriptor);
    if (active_ && domain_ == ObservedDomain::kRaft && result == 0)
      observe(kAfterMetadataCompactionRaftSync);
    return result;
  }

  int fsync(const int descriptor) override {
    const int result = delegate_.fsync(descriptor);
    if (!active_ || domain_ != ObservedDomain::kApplication || result != 0)
      return result;
    struct stat metadata {};
    if (delegate_.fstat(descriptor, &metadata) == 0) {
      if (S_ISREG(metadata.st_mode)) {
        observe(kAfterMetadataFileSync);
        observe(kAfterMetadataCompactionFileSync);
      } else if (S_ISDIR(metadata.st_mode)) {
        observe(kAfterMetadataDirectorySync);
        observe(kAfterMetadataAuthoritativeReclamationDirectorySync);
        observe(kAfterMetadataOrphanReclamationDirectorySync);
        observe(kAfterMetadataCompactionDirectorySync);
      }
    }
    return result;
  }

  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    const int result = delegate_.rename_no_replace(request);
    if (active_ && domain_ == ObservedDomain::kApplication && result == 0) {
      observe(kAfterMetadataRename);
      observe(kAfterMetadataCompactionRename);
    }
    return result;
  }

  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }

  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    const int result = delegate_.list_directory_entries(descriptor, entries);
    if (active_ && domain_ == ObservedDomain::kApplication && result == 0) {
      observe(kAfterMetadataAuthoritativeReclamationList);
      observe(kAfterMetadataOrphanReclamationList);
    }
    return result;
  }

  int unlink_at(const int directory_descriptor, const char* name) override {
    const int result = delegate_.unlink_at(directory_descriptor, name);
    if (active_ && domain_ == ObservedDomain::kApplication && result == 0 &&
        std::string_view{name}.ends_with(".rmas")) {
      observe(kAfterMetadataAuthoritativeReclamationUnlink);
      observe(kAfterMetadataOrphanReclamationUnlink);
    }
    return result;
  }

  int close(const int descriptor) override {
    const int result = delegate_.close(descriptor);
    if (active_ && domain_ == ObservedDomain::kApplication && result == 0) {
      observe(kAfterMetadataTemporaryClose);
      observe(kAfterMetadataCompactionTemporaryClose);
    }
    return result;
  }

  void observe_success_release() {
    observe(kAfterMetadataSuccessRelease);
  }

  void observe_reclamation_success(const bool authoritative) {
    observe(authoritative ? kAfterMetadataAuthoritativeReclamationSuccess
                          : kAfterMetadataOrphanReclamationSuccess);
  }

  void observe_compaction_success() {
    observe(kAfterMetadataCompactionSuccess);
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

  io::detail::PosixSyscalls& delegate_;
  std::string pause_after_;
  std::uint64_t pause_occurrence_;
  std::uint64_t observed_occurrences_{};
  ObservedDomain domain_;
  bool active_{false};
};

[[nodiscard]] bool is_authoritative_reclamation_point(const std::string_view point) {
  return point.starts_with("after_metadata_authoritative_reclamation_");
}

[[nodiscard]] bool is_orphan_reclamation_point(const std::string_view point) {
  return point.starts_with("after_metadata_orphan_reclamation_");
}

[[nodiscard]] bool is_compaction_point(const std::string_view point) {
  return point.starts_with("after_metadata_compaction_");
}

[[nodiscard]] int run_compaction(const ChildConfig& config) {
  std::error_code error;
  static_cast<void>(std::filesystem::create_directory(config.directory / "raft", error));
  if (!error) {
    static_cast<void>(
        std::filesystem::create_directory(config.directory / "metadata-snapshots", error));
  }
  if (error)
    return 9;
  ObservingSyscalls application_syscalls{io::detail::system_posix_syscalls(), config.pause_after,
                                         config.pause_occurrence, ObservedDomain::kApplication};
  ObservingSyscalls raft_syscalls{io::detail::system_posix_syscalls(), config.pause_after,
                                  config.pause_occurrence, ObservedDomain::kRaft};
  auto runtime = detail::DurableMultiRaftRuntimeTestAccess::create_new(
      1U, metadata_compaction_log_config(config.directory), metadata_crash_groups(), {},
      raft_syscalls);
  if (!runtime.has_value())
    return 10;
  auto election = runtime->execute_batch({{metadata_crash_group_id(), StartElectionOperation{}}});
  if (!election.has_value() || election->size() != 1U || !election->front().status.is_ok())
    return 11;
  auto storage = detail::MetadataSnapshotStorageTestAccess::create(
      metadata_compaction_storage_config(config.directory), application_syscalls);
  if (!storage.has_value())
    return 12;
  auto metadata = DurableMetadataStateMachine::recover(metadata_crash_group_id(), *runtime,
                                                       std::move(*storage));
  if (!metadata.has_value())
    return 13;
  auto proposed =
      runtime->execute_batch({{metadata_crash_group_id(), metadata_compaction_proposal()}});
  if (!proposed.has_value() || proposed->size() != 1U || !proposed->front().status.is_ok())
    return 14;
  auto applied = metadata->apply_committed();
  if (!applied.has_value() || applied->last_applied_index != 1U)
    return 15;
  application_syscalls.begin();
  raft_syscalls.begin();
  auto compacted = metadata->compact_applied_prefix(1U);
  if (!compacted.has_value())
    return 16;
  raft_syscalls.observe_compaction_success();
  return 17;
}

[[nodiscard]] int run(const ChildConfig& config) {
  if (config.directory.empty() || config.pause_after.empty() || config.pause_occurrence == 0U)
    return 2;
  if (is_compaction_point(config.pause_after))
    return run_compaction(config);
  ObservingSyscalls syscalls{io::detail::system_posix_syscalls(), config.pause_after,
                             config.pause_occurrence, ObservedDomain::kApplication};
  auto storage = detail::MetadataSnapshotStorageTestAccess::create(
      metadata_crash_storage_config(config.directory), syscalls);
  if (!storage.has_value())
    return 3;
  const bool authoritative_reclamation = is_authoritative_reclamation_point(config.pause_after);
  const bool orphan_reclamation = is_orphan_reclamation_point(config.pause_after);
  if (authoritative_reclamation || orphan_reclamation) {
    for (const LogIndex index : {7U, 8U, 9U}) {
      auto installed = storage->install(metadata_crash_snapshot(index));
      if (!installed.has_value())
        return 4;
    }
    syscalls.begin();
    auto reclaimed = storage->reclaim_obsolete(
        authoritative_reclamation ? std::optional<LogIndex>{8U} : std::nullopt);
    if (!reclaimed.has_value())
      return 5;
    syscalls.observe_reclamation_success(authoritative_reclamation);
    return 6;
  }
  syscalls.begin();
  auto installed = storage->install(metadata_crash_snapshot());
  if (!installed.has_value())
    return 7;
  syscalls.observe_success_release();
  return 8;
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
