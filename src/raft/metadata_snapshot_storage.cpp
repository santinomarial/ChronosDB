#include "chronos/raft/metadata_snapshot_storage.hpp"

#include "chronos/io/posix_io.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "io/posix_syscalls.hpp"

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

namespace chronos::raft {
namespace {

constexpr std::string_view kLockFileName = "LOCK";
constexpr std::string_view kPrefix = "metadata-snapshot-";
constexpr std::string_view kSuffix = ".rmas";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::size_t kIndexDigits = 20U;

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}
[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}
[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return {status.code(), std::move(message)};
}
[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

void saturating_add(std::uint64_t& target, const std::uint64_t increment) noexcept {
  const std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max();
  target = target > maximum - increment ? maximum : target + increment;
}

[[nodiscard]] bool valid_limits(const MetadataSnapshotCodecLimits& limits) noexcept {
  return limits.maximum_snapshot_bytes >=
             kMetadataSnapshotHeaderSize + sizeof(NodeId) + kMetadataSnapshotTrailerSize &&
         limits.maximum_snapshot_bytes <= kMaximumMetadataSnapshotSize &&
         limits.maximum_entries > 0U && limits.maximum_entry_payload_bytes > 0U &&
         limits.maximum_entry_payload_bytes <= kMaximumSchemaDefinitionSize &&
         limits.maximum_voters > 0U && limits.maximum_voters <= 1024U;
}

[[nodiscard]] common::Result<LogIndex> parse_name(const std::string_view name,
                                                  const bool temporary) {
  const std::size_t expected =
      kPrefix.size() + kIndexDigits + kSuffix.size() + (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(kPrefix) ||
      !name.substr(kPrefix.size() + kIndexDigits).starts_with(kSuffix) ||
      (temporary && !name.ends_with(kTemporarySuffix))) {
    return common::make_unexpected(invalid("metadata snapshot file name is noncanonical"));
  }
  LogIndex index{};
  const std::string_view digits = name.substr(kPrefix.size(), kIndexDigits);
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), index);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() || index == 0U)
    return common::make_unexpected(invalid("metadata snapshot index name is invalid"));
  auto canonical = metadata_snapshot_file_name(index);
  if (!canonical.has_value() || !name.starts_with(*canonical))
    return common::make_unexpected(invalid("metadata snapshot index name is not canonical"));
  return index;
}

} // namespace

class MetadataSnapshotStorage::Impl {
public:
  Impl(MetadataSnapshotStorageConfig configured, io::PosixDirectory owned_directory,
       io::PosixAdvisoryLock owned_lock) noexcept
      : config(std::move(configured)), directory(std::move(owned_directory)),
        lock(std::move(owned_lock)) {}

  [[nodiscard]] common::Status fail(common::Status status, const bool poison_owner = false) {
    if (poison_owner && poison.is_ok())
      poison = status;
    return status;
  }
  [[nodiscard]] common::Status check_usable() const {
    return poison.is_ok()
               ? common::Status::ok()
               : unavailable("metadata snapshot storage is poisoned: " + poison.message());
  }
  [[nodiscard]] common::Status cleanup_temporaries() {
    auto entries = directory.list_entries();
    if (!entries.has_value())
      return entries.error();
    std::uint64_t removed{};
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.starts_with(kPrefix) || !entry.name.ends_with(kTemporarySuffix))
        continue;
      if (!parse_name(entry.name, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile) {
        return corruption("recognized metadata snapshot temporary is noncanonical");
      }
      common::Status status = directory.remove_file(entry.name);
      if (!status.is_ok())
        return status;
      saturating_add(removed, 1U);
    }
    if (removed == 0U)
      return common::Status::ok();
    common::Status synchronized = directory.sync();
    if (!synchronized.is_ok())
      return synchronized;
    saturating_add(cleanup_metrics.temporary_files_removed, removed);
    saturating_add(cleanup_metrics.temporary_directory_syncs, 1U);
    return common::Status::ok();
  }
  [[nodiscard]] common::Status fail_reclamation(common::Status status) {
    saturating_add(cleanup_metrics.reclamation_failures, 1U);
    return status;
  }
  [[nodiscard]] common::Result<LoadedMetadataSnapshot>
  load_file(const std::string& file_name, const LogIndex expected_index) const {
    common::Status usable = check_usable();
    if (!usable.is_ok())
      return common::make_unexpected(std::move(usable));
    auto file = directory.open_regular_file(file_name, io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto size = file->size();
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size > config.codec_limits.maximum_snapshot_bytes ||
        *size > std::numeric_limits<std::size_t>::max()) {
      return common::make_unexpected(exhausted("installed metadata snapshot exceeds limit"));
    }
    try {
      std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value())
        return common::make_unexpected(read.error());
      if (*read != bytes.size())
        return common::make_unexpected(corruption("installed metadata snapshot is truncated"));
      auto decoded = decode_metadata_application_snapshot_v1(bytes, config.codec_limits);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (decoded->group_id != config.group_id ||
          decoded->raft_snapshot.last_included_index != expected_index) {
        return common::make_unexpected(
            corruption("installed metadata snapshot disagrees with its owner or name"));
      }
      return LoadedMetadataSnapshot{file_name, std::move(*decoded), std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("metadata snapshot read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("metadata snapshot read exceeded limits"));
    }
  }

  MetadataSnapshotStorageConfig config;
  io::PosixDirectory directory;
  io::PosixAdvisoryLock lock;
  common::Status poison;
  MetadataSnapshotCleanupMetrics cleanup_metrics;
};

common::Result<std::string> metadata_snapshot_file_name(const LogIndex last_included_index) {
  if (last_included_index == 0U)
    return common::make_unexpected(invalid("metadata snapshot index must be nonzero"));
  std::array<char, kIndexDigits> digits{};
  const auto converted =
      std::to_chars(digits.data(), digits.data() + digits.size(), last_included_index);
  if (converted.ec != std::errc{})
    return common::make_unexpected(invalid("metadata snapshot index is not representable"));
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kPrefix};
  result.append(kIndexDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kSuffix);
  return result;
}

common::Result<MetadataSnapshotStorage>
MetadataSnapshotStorage::open_with(MetadataSnapshotStorageConfig config, const bool create_lock,
                                   io::detail::PosixSyscalls& syscalls) {
  if (config.directory_path.empty() || config.group_id.is_nil() || config.file_permissions == 0U ||
      (config.file_permissions & ~0777U) != 0U || !valid_limits(config.codec_limits)) {
    return common::make_unexpected(invalid("metadata snapshot storage config is invalid"));
  }
  auto directory = io::detail::PosixHandleFactory::open_directory(config.directory_path, syscalls);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto lock = create_lock
                  ? directory->acquire_exclusive_lock(kLockFileName, config.file_permissions)
                  : directory->acquire_existing_exclusive_lock(kLockFileName);
  if (!lock.has_value())
    return common::make_unexpected(lock.error());
  auto implementation =
      std::make_unique<Impl>(std::move(config), std::move(*directory), std::move(*lock));
  common::Status cleanup = implementation->cleanup_temporaries();
  if (!cleanup.is_ok())
    return common::make_unexpected(std::move(cleanup));
  return MetadataSnapshotStorage{std::move(implementation)};
}

MetadataSnapshotStorage::MetadataSnapshotStorage(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
MetadataSnapshotStorage::~MetadataSnapshotStorage() = default;
MetadataSnapshotStorage::MetadataSnapshotStorage(MetadataSnapshotStorage&&) noexcept = default;
MetadataSnapshotStorage&
MetadataSnapshotStorage::operator=(MetadataSnapshotStorage&&) noexcept = default;

common::Result<MetadataSnapshotStorage>
MetadataSnapshotStorage::create(MetadataSnapshotStorageConfig config) {
  try {
    return open_with(std::move(config), true, io::detail::system_posix_syscalls());
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata snapshot storage allocation failed"));
  }
}
common::Result<MetadataSnapshotStorage>
MetadataSnapshotStorage::open_existing(MetadataSnapshotStorageConfig config) {
  try {
    return open_with(std::move(config), false, io::detail::system_posix_syscalls());
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata snapshot storage allocation failed"));
  }
}

common::Result<InstalledMetadataSnapshot>
MetadataSnapshotStorage::install(const MetadataApplicationSnapshot& snapshot) {
  if (!implementation_)
    return common::make_unexpected(invalid("metadata snapshot storage was moved from"));
  Impl& impl = *implementation_;
  common::Status usable = impl.check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (snapshot.group_id != impl.config.group_id)
    return common::make_unexpected(invalid("metadata snapshot belongs to another group"));
  auto encoded = encode_metadata_application_snapshot_v1(snapshot, impl.config.codec_limits);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  auto final_name = metadata_snapshot_file_name(snapshot.raft_snapshot.last_included_index);
  if (!final_name.has_value())
    return common::make_unexpected(final_name.error());
  auto existing = impl.load_file(*final_name, snapshot.raft_snapshot.last_included_index);
  if (existing.has_value()) {
    if (existing->bytes != *encoded)
      return common::make_unexpected(
          corruption("metadata snapshot index already has different durable bytes"));
    return InstalledMetadataSnapshot{snapshot.raft_snapshot.last_included_index, *final_name, true};
  }
  if (existing.error().code() != common::StatusCode::kNotFound)
    return common::make_unexpected(existing.error());

  const std::string temp_name = temporary_name(*final_name);
  common::Status operation = impl.directory.remove_file(temp_name);
  bool removed_temporary = false;
  if (operation.is_ok()) {
    removed_temporary = true;
    operation = impl.directory.sync();
  } else if (operation.code() == common::StatusCode::kNotFound) {
    operation = common::Status::ok();
  }
  if (!operation.is_ok())
    return common::make_unexpected(
        with_context("clean prior metadata snapshot temporary", operation));
  if (removed_temporary) {
    saturating_add(impl.cleanup_metrics.temporary_files_removed, 1U);
    saturating_add(impl.cleanup_metrics.temporary_directory_syncs, 1U);
  }
  auto temporary =
      impl.directory.create_exclusive_regular_file(temp_name, impl.config.file_permissions);
  if (!temporary.has_value())
    return common::make_unexpected(temporary.error());
  operation = temporary->write_all_at(0U, *encoded);
  if (!operation.is_ok())
    return common::make_unexpected(with_context("write metadata snapshot temporary", operation));
  auto readback_size = temporary->size();
  if (!readback_size.has_value() || *readback_size != encoded->size()) {
    return common::make_unexpected(readback_size.has_value()
                                       ? corruption("metadata snapshot temporary size changed")
                                       : readback_size.error());
  }
  try {
    std::vector<std::byte> readback(encoded->size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size()) {
      return common::make_unexpected(
          read.has_value() ? corruption("metadata snapshot readback is incomplete") : read.error());
    }
    auto decoded = decode_metadata_application_snapshot_v1(readback, impl.config.codec_limits);
    if (!decoded.has_value() || *decoded != snapshot) {
      return common::make_unexpected(
          decoded.has_value() ? corruption("metadata snapshot readback changed") : decoded.error());
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("metadata snapshot readback allocation failed"));
  }
  operation = temporary->sync_all();
  if (!operation.is_ok())
    return common::make_unexpected(with_context("synchronize metadata snapshot", operation));
  operation = temporary->close();
  if (!operation.is_ok())
    return common::make_unexpected(with_context("close metadata snapshot temporary", operation));
  operation = impl.directory.rename_no_replace({temp_name, *final_name});
  if (!operation.is_ok())
    return common::make_unexpected(with_context("install metadata snapshot", operation));
  operation = impl.directory.sync();
  if (!operation.is_ok()) {
    return common::make_unexpected(
        impl.fail(with_context("synchronize metadata snapshot directory", operation), true));
  }
  return InstalledMetadataSnapshot{snapshot.raft_snapshot.last_included_index, *final_name, false};
}

common::Result<LoadedMetadataSnapshot>
MetadataSnapshotStorage::load(const LogIndex last_included_index) const {
  if (!implementation_)
    return common::make_unexpected(invalid("metadata snapshot storage was moved from"));
  auto name = metadata_snapshot_file_name(last_included_index);
  if (!name.has_value())
    return common::make_unexpected(name.error());
  return implementation_->load_file(*name, last_included_index);
}

common::Result<std::optional<LoadedMetadataSnapshot>> MetadataSnapshotStorage::load_latest() const {
  if (!implementation_)
    return common::make_unexpected(invalid("metadata snapshot storage was moved from"));
  common::Status usable = implementation_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  auto entries = implementation_->directory.list_entries();
  if (!entries.has_value())
    return common::make_unexpected(entries.error());
  std::optional<LogIndex> latest;
  for (const io::DirectoryEntry& entry : *entries) {
    if (entry.name == kLockFileName || !entry.name.starts_with(kPrefix))
      continue;
    auto parsed = parse_name(entry.name, false);
    if (!parsed.has_value() || entry.type != io::DirectoryEntryType::kRegularFile)
      return common::make_unexpected(corruption("metadata snapshot directory is noncanonical"));
    if (!latest.has_value() || *parsed > *latest)
      latest = *parsed;
  }
  if (!latest.has_value())
    return std::optional<LoadedMetadataSnapshot>{};
  auto loaded = load(*latest);
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  return std::optional<LoadedMetadataSnapshot>{std::move(*loaded)};
}

common::Result<MetadataSnapshotReclamationReport>
MetadataSnapshotStorage::reclaim_obsolete(const std::optional<LogIndex> authoritative_index) {
  if (!implementation_)
    return common::make_unexpected(invalid("metadata snapshot storage was moved from"));
  Impl& impl = *implementation_;
  saturating_add(impl.cleanup_metrics.reclamation_attempts, 1U);
  common::Status usable = impl.check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(impl.fail_reclamation(std::move(usable)));
  if (authoritative_index.has_value()) {
    auto authoritative = load(*authoritative_index);
    if (!authoritative.has_value())
      return common::make_unexpected(impl.fail_reclamation(authoritative.error()));
  }
  auto entries = impl.directory.list_entries();
  if (!entries.has_value())
    return common::make_unexpected(impl.fail_reclamation(entries.error()));
  std::vector<std::string> obsolete;
  try {
    for (const io::DirectoryEntry& entry : *entries) {
      if (entry.name == kLockFileName || !entry.name.starts_with(kPrefix))
        continue;
      auto parsed = parse_name(entry.name, false);
      if (!parsed.has_value() || entry.type != io::DirectoryEntryType::kRegularFile)
        return common::make_unexpected(
            impl.fail_reclamation(corruption("metadata snapshot directory is noncanonical")));
      if (!authoritative_index.has_value() || *parsed != *authoritative_index)
        obsolete.push_back(entry.name);
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        impl.fail_reclamation(exhausted("metadata snapshot reclamation allocation failed")));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        impl.fail_reclamation(exhausted("metadata snapshot reclamation exceeded limits")));
  }
  for (const std::string& file_name : obsolete) {
    common::Status removed = impl.directory.remove_file(file_name);
    if (!removed.is_ok())
      return common::make_unexpected(
          impl.fail_reclamation(with_context("remove obsolete metadata snapshot", removed)));
  }
  if (!obsolete.empty()) {
    common::Status synchronized = impl.directory.sync();
    if (!synchronized.is_ok())
      return common::make_unexpected(impl.fail_reclamation(
          with_context("synchronize metadata snapshot reclamation", synchronized)));
    saturating_add(impl.cleanup_metrics.reclaimed_files,
                   static_cast<std::uint64_t>(obsolete.size()));
    saturating_add(impl.cleanup_metrics.reclamation_directory_syncs, 1U);
  }
  return MetadataSnapshotReclamationReport{authoritative_index, obsolete.size()};
}

bool MetadataSnapshotStorage::is_usable() const noexcept {
  return implementation_ && implementation_->poison.is_ok();
}
common::Status MetadataSnapshotStorage::poison_status() const {
  return implementation_ ? implementation_->poison
                         : invalid("metadata snapshot storage was moved from");
}

MetadataSnapshotCleanupMetrics MetadataSnapshotStorage::cleanup_metrics() const noexcept {
  return implementation_ ? implementation_->cleanup_metrics : MetadataSnapshotCleanupMetrics{};
}

} // namespace chronos::raft
