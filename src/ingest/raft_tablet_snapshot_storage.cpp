#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"

#include "chronos/ingest/columnar_append_format.hpp"
#include "chronos/io/posix_io.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

constexpr std::string_view kLockFileName = "LOCK";
constexpr std::string_view kPrefix = "snapshot-";
constexpr std::string_view kSuffix = ".rtas";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::size_t kIndexDigits = 20U;

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return common::Status{status.code(), std::move(message)};
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

[[nodiscard]] common::Result<raft::LogIndex> parse_name(const std::string_view name,
                                                        const bool temporary) {
  const std::size_t expected =
      kPrefix.size() + kIndexDigits + kSuffix.size() + (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(kPrefix) ||
      !name.substr(kPrefix.size() + kIndexDigits).starts_with(kSuffix) ||
      (temporary && !name.ends_with(kTemporarySuffix))) {
    return common::make_unexpected(invalid("Raft tablet snapshot file name is noncanonical"));
  }
  raft::LogIndex index{};
  const std::string_view digits = name.substr(kPrefix.size(), kIndexDigits);
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), index);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() || index == 0U) {
    return common::make_unexpected(invalid("Raft tablet snapshot index name is invalid"));
  }
  const auto canonical = raft_tablet_snapshot_file_name(index);
  if (!canonical.has_value() || !name.starts_with(*canonical)) {
    return common::make_unexpected(invalid("Raft tablet snapshot index name is not canonical"));
  }
  return index;
}

} // namespace

class RaftTabletSnapshotStorage::Impl {
public:
  Impl(RaftTabletSnapshotStorageConfig config, io::PosixDirectory directory,
       io::PosixAdvisoryLock lock) noexcept
      : config_(std::move(config)), directory_(std::move(directory)), lock_(std::move(lock)) {}

  [[nodiscard]] common::Status fail(common::Status status, const bool poison = false) {
    if (poison && poison_.is_ok()) {
      poison_ = status;
    }
    return status;
  }

  [[nodiscard]] common::Status check_usable() const {
    if (poison_.is_ok()) {
      return common::Status::ok();
    }
    return unavailable("Raft tablet snapshot storage is poisoned: " + poison_.message());
  }

  [[nodiscard]] common::Status cleanup_temporaries() {
    auto entries = directory_.list_entries();
    if (!entries.has_value()) {
      return entries.error();
    }
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.starts_with(kPrefix) || !entry.name.ends_with(kTemporarySuffix)) {
        continue;
      }
      if (!parse_name(entry.name, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile) {
        return corruption("recognized Raft tablet snapshot temporary is noncanonical");
      }
      common::Status status = directory_.remove_file(entry.name);
      if (!status.is_ok()) {
        return status;
      }
      removed = true;
    }
    return removed ? directory_.sync() : common::Status::ok();
  }

  [[nodiscard]] common::Result<LoadedRaftTabletSnapshot>
  load_file(const std::string& file_name, const raft::LogIndex expected_index) const {
    common::Status usable = check_usable();
    if (!usable.is_ok()) {
      return common::make_unexpected(std::move(usable));
    }
    auto file = directory_.open_regular_file(file_name, io::FileOpenMode::kReadOnly);
    if (!file.has_value()) {
      return common::make_unexpected(file.error());
    }
    auto size = file->size();
    if (!size.has_value()) {
      return common::make_unexpected(size.error());
    }
    if (*size > config_.codec_limits.maximum_snapshot_bytes ||
        *size > std::numeric_limits<std::size_t>::max()) {
      return common::make_unexpected(exhausted("installed Raft tablet snapshot exceeds limit"));
    }
    try {
      std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value()) {
        return common::make_unexpected(read.error());
      }
      if (*read != bytes.size()) {
        return common::make_unexpected(
            corruption("installed Raft tablet snapshot ended before its exact size"));
      }
      auto decoded = decode_raft_tablet_application_snapshot_v1(bytes, config_.codec_limits);
      if (!decoded.has_value()) {
        return common::make_unexpected(decoded.error());
      }
      if (decoded->group_id != config_.group_id ||
          decoded->raft_snapshot.last_included_index != expected_index) {
        return common::make_unexpected(
            corruption("installed Raft tablet snapshot disagrees with its owner or name"));
      }
      return LoadedRaftTabletSnapshot{
          .file_name = file_name, .snapshot = std::move(*decoded), .bytes = std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("Raft tablet snapshot read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("Raft tablet snapshot read exceeded limits"));
    }
  }

  RaftTabletSnapshotStorageConfig config_;
  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  common::Status poison_;
};

common::Result<std::string>
raft_tablet_snapshot_file_name(const raft::LogIndex last_included_index) {
  if (last_included_index == 0U) {
    return common::make_unexpected(invalid("Raft tablet snapshot index must be nonzero"));
  }
  std::array<char, kIndexDigits> digits{};
  const auto converted =
      std::to_chars(digits.data(), digits.data() + digits.size(), last_included_index);
  if (converted.ec != std::errc{}) {
    return common::make_unexpected(invalid("Raft tablet snapshot index is not representable"));
  }
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kPrefix};
  result.append(kIndexDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kSuffix);
  return result;
}

common::Result<RaftTabletSnapshotStorage>
RaftTabletSnapshotStorage::open(RaftTabletSnapshotStorageConfig config, const bool create_lock) {
  if (config.directory_path.empty() || config.group_id.is_nil() || config.file_permissions == 0U ||
      (config.file_permissions & ~0777U) != 0U ||
      config.codec_limits.maximum_snapshot_bytes <
          kRaftTabletSnapshotHeaderSize + sizeof(raft::NodeId) + kRaftTabletSnapshotTrailerSize ||
      config.codec_limits.maximum_snapshot_bytes > kMaximumRaftTabletSnapshotSize ||
      config.codec_limits.maximum_entries == 0U ||
      config.codec_limits.maximum_entry_payload_bytes == 0U ||
      config.codec_limits.maximum_entry_payload_bytes >
          columnar_append_v1::kMaximumApplicationPayloadLength ||
      config.codec_limits.maximum_voters == 0U || config.codec_limits.maximum_voters > 1024U) {
    return common::make_unexpected(invalid("Raft tablet snapshot storage config is invalid"));
  }
  auto directory = io::PosixDirectory::open(config.directory_path);
  if (!directory.has_value()) {
    return common::make_unexpected(directory.error());
  }
  auto lock = create_lock
                  ? directory->acquire_exclusive_lock(kLockFileName, config.file_permissions)
                  : directory->acquire_existing_exclusive_lock(kLockFileName);
  if (!lock.has_value()) {
    return common::make_unexpected(lock.error());
  }
  auto impl = std::make_unique<RaftTabletSnapshotStorage::Impl>(
      std::move(config), std::move(*directory), std::move(*lock));
  common::Status cleanup = impl->cleanup_temporaries();
  if (!cleanup.is_ok()) {
    return common::make_unexpected(std::move(cleanup));
  }
  return RaftTabletSnapshotStorage{std::move(impl)};
}

RaftTabletSnapshotStorage::RaftTabletSnapshotStorage(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
RaftTabletSnapshotStorage::~RaftTabletSnapshotStorage() = default;
RaftTabletSnapshotStorage::RaftTabletSnapshotStorage(RaftTabletSnapshotStorage&&) noexcept =
    default;
RaftTabletSnapshotStorage&
RaftTabletSnapshotStorage::operator=(RaftTabletSnapshotStorage&&) noexcept = default;

common::Result<RaftTabletSnapshotStorage>
RaftTabletSnapshotStorage::create(RaftTabletSnapshotStorageConfig config) {
  try {
    return open(std::move(config), true);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot storage allocation failed"));
  }
}

common::Result<RaftTabletSnapshotStorage>
RaftTabletSnapshotStorage::open_existing(RaftTabletSnapshotStorageConfig config) {
  try {
    return open(std::move(config), false);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot storage allocation failed"));
  }
}

common::Result<InstalledRaftTabletSnapshot>
RaftTabletSnapshotStorage::install(const RaftTabletApplicationSnapshot& snapshot) {
  if (impl_ == nullptr) {
    return common::make_unexpected(invalid("Raft tablet snapshot storage was moved from"));
  }
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok()) {
    return common::make_unexpected(std::move(usable));
  }
  if (snapshot.group_id != impl_->config_.group_id) {
    return common::make_unexpected(invalid("Raft tablet snapshot belongs to another group"));
  }
  auto encoded = encode_raft_tablet_application_snapshot_v1(snapshot, impl_->config_.codec_limits);
  if (!encoded.has_value()) {
    return common::make_unexpected(encoded.error());
  }
  auto final_name = raft_tablet_snapshot_file_name(snapshot.raft_snapshot.last_included_index);
  if (!final_name.has_value()) {
    return common::make_unexpected(final_name.error());
  }
  auto existing = impl_->load_file(*final_name, snapshot.raft_snapshot.last_included_index);
  if (existing.has_value()) {
    if (existing->bytes != *encoded) {
      return common::make_unexpected(
          corruption("Raft tablet snapshot index already has different durable bytes"));
    }
    return InstalledRaftTabletSnapshot{.last_included_index =
                                           snapshot.raft_snapshot.last_included_index,
                                       .file_name = *final_name,
                                       .already_present = true};
  }
  if (existing.error().code() != common::StatusCode::kNotFound) {
    return common::make_unexpected(existing.error());
  }

  const std::string temp_name = temporary_name(*final_name);
  common::Status cleanup = impl_->directory_.remove_file(temp_name);
  if (cleanup.is_ok()) {
    cleanup = impl_->directory_.sync();
  } else if (cleanup.code() == common::StatusCode::kNotFound) {
    cleanup = common::Status::ok();
  }
  if (!cleanup.is_ok()) {
    return common::make_unexpected(with_context("clean prior snapshot temporary", cleanup));
  }
  auto temporary =
      impl_->directory_.create_exclusive_regular_file(temp_name, impl_->config_.file_permissions);
  if (!temporary.has_value()) {
    return common::make_unexpected(temporary.error());
  }
  common::Status operation = temporary->write_all_at(0U, *encoded);
  if (!operation.is_ok()) {
    return common::make_unexpected(with_context("write Raft tablet snapshot temporary", operation));
  }
  auto readback_size = temporary->size();
  if (!readback_size.has_value() || *readback_size != encoded->size()) {
    return common::make_unexpected(readback_size.has_value()
                                       ? corruption("Raft tablet snapshot temporary size changed")
                                       : readback_size.error());
  }
  try {
    std::vector<std::byte> readback(encoded->size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size()) {
      return common::make_unexpected(read.has_value()
                                         ? corruption("Raft tablet snapshot readback is incomplete")
                                         : read.error());
    }
    auto decoded =
        decode_raft_tablet_application_snapshot_v1(readback, impl_->config_.codec_limits);
    if (!decoded.has_value() || *decoded != snapshot) {
      return common::make_unexpected(decoded.has_value()
                                         ? corruption("Raft tablet snapshot readback changed")
                                         : decoded.error());
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot readback allocation failed"));
  }
  operation = temporary->sync_all();
  if (!operation.is_ok()) {
    return common::make_unexpected(with_context("synchronize Raft tablet snapshot", operation));
  }
  operation = temporary->close();
  if (!operation.is_ok()) {
    return common::make_unexpected(with_context("close Raft tablet snapshot temporary", operation));
  }
  operation = impl_->directory_.rename_no_replace({.old_name = temp_name, .new_name = *final_name});
  if (!operation.is_ok()) {
    return common::make_unexpected(with_context("install Raft tablet snapshot", operation));
  }
  operation = impl_->directory_.sync();
  if (!operation.is_ok()) {
    return common::make_unexpected(
        impl_->fail(with_context("synchronize Raft tablet snapshot directory", operation), true));
  }
  return InstalledRaftTabletSnapshot{.last_included_index =
                                         snapshot.raft_snapshot.last_included_index,
                                     .file_name = *final_name,
                                     .already_present = false};
}

common::Result<LoadedRaftTabletSnapshot>
RaftTabletSnapshotStorage::load(const raft::LogIndex last_included_index) const {
  if (impl_ == nullptr) {
    return common::make_unexpected(invalid("Raft tablet snapshot storage was moved from"));
  }
  auto name = raft_tablet_snapshot_file_name(last_included_index);
  if (!name.has_value()) {
    return common::make_unexpected(name.error());
  }
  return impl_->load_file(*name, last_included_index);
}

common::Result<std::optional<LoadedRaftTabletSnapshot>>
RaftTabletSnapshotStorage::load_latest() const {
  if (impl_ == nullptr) {
    return common::make_unexpected(invalid("Raft tablet snapshot storage was moved from"));
  }
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok()) {
    return common::make_unexpected(std::move(usable));
  }
  auto entries = impl_->directory_.list_entries();
  if (!entries.has_value()) {
    return common::make_unexpected(entries.error());
  }
  std::optional<raft::LogIndex> latest;
  for (const io::DirectoryEntry& entry : *entries) {
    if (entry.name == kLockFileName || !entry.name.starts_with(kPrefix)) {
      continue;
    }
    auto parsed = parse_name(entry.name, false);
    if (!parsed.has_value() || entry.type != io::DirectoryEntryType::kRegularFile) {
      return common::make_unexpected(corruption("Raft tablet snapshot directory is noncanonical"));
    }
    if (!latest.has_value() || *parsed > *latest) {
      latest = *parsed;
    }
  }
  if (!latest.has_value()) {
    return std::optional<LoadedRaftTabletSnapshot>{};
  }
  auto loaded = load(*latest);
  if (!loaded.has_value()) {
    return common::make_unexpected(loaded.error());
  }
  return std::optional<LoadedRaftTabletSnapshot>{std::move(*loaded)};
}

common::Result<RaftTabletSnapshotReclamationReport> RaftTabletSnapshotStorage::reclaim_obsolete(
    const std::optional<raft::LogIndex> authoritative_index) {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("Raft tablet snapshot storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (authoritative_index.has_value()) {
    auto authoritative = load(*authoritative_index);
    if (!authoritative.has_value())
      return common::make_unexpected(authoritative.error());
  }
  auto entries = impl_->directory_.list_entries();
  if (!entries.has_value())
    return common::make_unexpected(entries.error());
  std::vector<std::string> obsolete;
  try {
    for (const io::DirectoryEntry& entry : *entries) {
      if (entry.name == kLockFileName || !entry.name.starts_with(kPrefix))
        continue;
      auto parsed = parse_name(entry.name, false);
      if (!parsed.has_value() || entry.type != io::DirectoryEntryType::kRegularFile) {
        return common::make_unexpected(
            corruption("Raft tablet snapshot directory is noncanonical"));
      }
      if (!authoritative_index.has_value() || *parsed != *authoritative_index)
        obsolete.push_back(entry.name);
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot reclamation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Raft tablet snapshot reclamation exceeded limits"));
  }
  for (const std::string& file_name : obsolete) {
    common::Status removed = impl_->directory_.remove_file(file_name);
    if (!removed.is_ok())
      return common::make_unexpected(with_context("remove obsolete Raft tablet snapshot", removed));
  }
  if (!obsolete.empty()) {
    common::Status synchronized = impl_->directory_.sync();
    if (!synchronized.is_ok()) {
      return common::make_unexpected(
          with_context("synchronize Raft tablet snapshot reclamation", synchronized));
    }
  }
  return RaftTabletSnapshotReclamationReport{authoritative_index, obsolete.size()};
}

bool RaftTabletSnapshotStorage::is_usable() const noexcept {
  return impl_ != nullptr && impl_->poison_.is_ok();
}

common::Status RaftTabletSnapshotStorage::poison_status() const {
  return impl_ == nullptr ? invalid("Raft tablet snapshot storage was moved from") : impl_->poison_;
}

} // namespace chronos::ingest
