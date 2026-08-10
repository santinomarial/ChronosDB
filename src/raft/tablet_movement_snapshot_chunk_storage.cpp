#include "chronos/raft/tablet_movement_snapshot_chunk_storage.hpp"

#include "chronos/common/crc32c.hpp"
#include "chronos/io/posix_io.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

constexpr std::string_view kLockFileName = "LOCK";
constexpr std::string_view kChunkPrefix = "chunk-";
constexpr std::string_view kChunkSuffix = ".mchk";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::size_t kOffsetDigits = 20U;

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

[[nodiscard]] bool valid_limits(const TabletMovementSnapshotChunkCodecLimits& limits) {
  return limits.maximum_snapshot_bytes > 0U && limits.maximum_chunk_bytes > 0U &&
         limits.maximum_chunk_bytes <= limits.maximum_snapshot_bytes &&
         limits.maximum_encoded_bytes >= kTabletMovementSnapshotChunkHeaderSize + 1U +
                                             kTabletMovementSnapshotChunkTrailerSize &&
         limits.maximum_encoded_bytes <= kMaximumTabletMovementSnapshotChunkSize &&
         limits.maximum_chunk_bytes <= limits.maximum_encoded_bytes -
                                           kTabletMovementSnapshotChunkHeaderSize -
                                           kTabletMovementSnapshotChunkTrailerSize;
}

[[nodiscard]] bool valid_session(const TabletMovementSnapshotSession& session,
                                 const TabletMovementSnapshotChunkCodecLimits& limits) {
  return !session.tablet_id.uuid().is_nil() && session.placement_epoch != 0U &&
         session.source_node != 0U && session.target_node != 0U &&
         session.source_node != session.target_node && session.snapshot.manifest_generation != 0U &&
         session.snapshot.applied_index != 0U && session.snapshot.applied_term != 0U &&
         session.snapshot.total_bytes != 0U &&
         session.snapshot.total_bytes <= limits.maximum_snapshot_bytes;
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

[[nodiscard]] common::Result<std::uint64_t> parse_name(const std::string_view name,
                                                       const bool temporary) {
  const std::size_t expected = kChunkPrefix.size() + kOffsetDigits + kChunkSuffix.size() +
                               (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(kChunkPrefix) ||
      !name.substr(kChunkPrefix.size() + kOffsetDigits).starts_with(kChunkSuffix) ||
      (temporary && !name.ends_with(kTemporarySuffix))) {
    return common::make_unexpected(invalid("movement snapshot chunk name is noncanonical"));
  }
  const std::string_view digits = name.substr(kChunkPrefix.size(), kOffsetDigits);
  std::uint64_t offset{};
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), offset);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size())
    return common::make_unexpected(invalid("movement snapshot chunk offset is invalid"));
  auto canonical = tablet_movement_snapshot_chunk_file_name(offset);
  if (!canonical.has_value() || !name.starts_with(*canonical))
    return common::make_unexpected(invalid("movement snapshot chunk name is noncanonical"));
  return offset;
}

struct ChunkFile {
  std::uint64_t offset{};
  std::size_t payload_bytes{};
  std::string file_name;
};

} // namespace

class TabletMovementSnapshotChunkStorage::Impl {
public:
  Impl(TabletMovementSnapshotChunkStorageConfig config, io::PosixDirectory directory,
       io::PosixAdvisoryLock lock) noexcept
      : config_(std::move(config)), directory_(std::move(directory)), lock_(std::move(lock)) {}

  [[nodiscard]] common::Status fail(common::Status status, const bool poison = false) {
    if (poison && poison_.is_ok())
      poison_ = status;
    return status;
  }

  [[nodiscard]] common::Status check_usable() const {
    return poison_.is_ok()
               ? common::Status::ok()
               : unavailable("movement snapshot chunk storage is poisoned: " + poison_.message());
  }

  [[nodiscard]] common::Status cleanup_temporaries() const {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return entries.error();
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.starts_with(kChunkPrefix) || !entry.name.ends_with(kTemporarySuffix))
        continue;
      if (!parse_name(entry.name, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile) {
        return corruption("recognized movement snapshot chunk temporary is noncanonical");
      }
      common::Status status = directory_.remove_file(entry.name);
      if (!status.is_ok())
        return status;
      removed = true;
    }
    return removed ? directory_.sync() : common::Status::ok();
  }

  [[nodiscard]] common::Result<LoadedTabletMovementSnapshotChunk>
  load_file(const std::string& file_name, const std::uint64_t expected_offset) const {
    common::Status usable = check_usable();
    if (!usable.is_ok())
      return common::make_unexpected(std::move(usable));
    auto file = directory_.open_regular_file(file_name, io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto size = file->size();
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size > config_.codec_limits.maximum_encoded_bytes ||
        *size > std::numeric_limits<std::size_t>::max()) {
      return common::make_unexpected(exhausted("installed movement snapshot chunk exceeds limit"));
    }
    try {
      std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value())
        return common::make_unexpected(read.error());
      if (*read != bytes.size()) {
        return common::make_unexpected(
            corruption("installed movement snapshot chunk ended before its exact size"));
      }
      auto decoded = decode_tablet_movement_snapshot_chunk_v1(bytes, config_.codec_limits);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (decoded->session != config_.session || decoded->offset != expected_offset) {
        return common::make_unexpected(
            corruption("installed movement snapshot chunk disagrees with owner or name"));
      }
      return LoadedTabletMovementSnapshotChunk{file_name, std::move(*decoded), std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("movement snapshot chunk read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("movement snapshot chunk read exceeds limits"));
    }
  }

  [[nodiscard]] common::Status recover_progress() {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return entries.error();
    std::vector<std::pair<std::uint64_t, std::string>> candidates;
    for (const io::DirectoryEntry& entry : *entries) {
      if (entry.name == kLockFileName || !entry.name.starts_with(kChunkPrefix))
        continue;
      if (entry.name.ends_with(kTemporarySuffix)) {
        if (!parse_name(entry.name, true).has_value() ||
            entry.type != io::DirectoryEntryType::kRegularFile) {
          return corruption("movement snapshot chunk directory is noncanonical");
        }
        continue;
      }
      auto offset = parse_name(entry.name, false);
      if (!offset.has_value() || entry.type != io::DirectoryEntryType::kRegularFile)
        return corruption("movement snapshot chunk directory is noncanonical");
      if (candidates.size() == config_.maximum_chunks)
        return exhausted("movement snapshot chunk count exceeds configured limit");
      candidates.emplace_back(*offset, entry.name);
    }
    std::ranges::sort(candidates);
    chunks_.clear();
    received_bytes_ = 0U;
    for (const auto& [offset, file_name] : candidates) {
      if (offset != received_bytes_)
        return corruption("movement snapshot chunks are not a contiguous prefix from zero");
      auto loaded = load_file(file_name, offset);
      if (!loaded.has_value())
        return loaded.error();
      chunks_.push_back({offset, loaded->chunk.bytes.size(), file_name});
      received_bytes_ += loaded->chunk.bytes.size();
    }
    return common::Status::ok();
  }

  TabletMovementSnapshotChunkStorageConfig config_;
  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  std::vector<ChunkFile> chunks_;
  std::uint64_t received_bytes_{};
  common::Status poison_;
};

common::Result<std::string> tablet_movement_snapshot_chunk_file_name(const std::uint64_t offset) {
  std::array<char, kOffsetDigits> digits{};
  const auto converted = std::to_chars(digits.data(), digits.data() + digits.size(), offset);
  if (converted.ec != std::errc{})
    return common::make_unexpected(invalid("movement snapshot chunk offset is not representable"));
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kChunkPrefix};
  result.append(kOffsetDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kChunkSuffix);
  return result;
}

common::Result<TabletMovementSnapshotChunkStorage>
TabletMovementSnapshotChunkStorage::open(TabletMovementSnapshotChunkStorageConfig config,
                                         const bool create_lock) {
  if (config.directory_path.empty() || !valid_limits(config.codec_limits) ||
      !valid_session(config.session, config.codec_limits) || config.maximum_chunks == 0U ||
      config.maximum_chunks > kMaximumTabletMovementSnapshotChunkFiles ||
      config.file_permissions == 0U || (config.file_permissions & ~0777U) != 0U) {
    return common::make_unexpected(invalid("movement snapshot chunk storage config is invalid"));
  }
  auto directory = io::PosixDirectory::open(config.directory_path);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto lock = create_lock
                  ? directory->acquire_exclusive_lock(kLockFileName, config.file_permissions)
                  : directory->acquire_existing_exclusive_lock(kLockFileName);
  if (!lock.has_value())
    return common::make_unexpected(lock.error());
  auto impl = std::make_unique<Impl>(std::move(config), std::move(*directory), std::move(*lock));
  common::Status status = impl->cleanup_temporaries();
  if (status.is_ok())
    status = impl->recover_progress();
  if (!status.is_ok())
    return common::make_unexpected(std::move(status));
  return TabletMovementSnapshotChunkStorage{std::move(impl)};
}

TabletMovementSnapshotChunkStorage::TabletMovementSnapshotChunkStorage(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
TabletMovementSnapshotChunkStorage::~TabletMovementSnapshotChunkStorage() = default;
TabletMovementSnapshotChunkStorage::TabletMovementSnapshotChunkStorage(
    TabletMovementSnapshotChunkStorage&&) noexcept = default;
TabletMovementSnapshotChunkStorage& TabletMovementSnapshotChunkStorage::operator=(
    TabletMovementSnapshotChunkStorage&&) noexcept = default;

common::Result<TabletMovementSnapshotChunkStorage>
TabletMovementSnapshotChunkStorage::create(TabletMovementSnapshotChunkStorageConfig config) {
  try {
    return open(std::move(config), true);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("movement snapshot chunk storage allocation failed"));
  }
}

common::Result<TabletMovementSnapshotChunkStorage>
TabletMovementSnapshotChunkStorage::open_existing(TabletMovementSnapshotChunkStorageConfig config) {
  try {
    return open(std::move(config), false);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("movement snapshot chunk storage allocation failed"));
  }
}

common::Result<InstalledTabletMovementSnapshotChunk>
TabletMovementSnapshotChunkStorage::install(const TabletMovementSnapshotChunk& chunk) {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("movement snapshot chunk storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (chunk.session != impl_->config_.session)
    return common::make_unexpected(invalid("movement snapshot chunk belongs to another session"));
  auto encoded = encode_tablet_movement_snapshot_chunk_v1(chunk, impl_->config_.codec_limits);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  auto final_name = tablet_movement_snapshot_chunk_file_name(chunk.offset);
  if (!final_name.has_value())
    return common::make_unexpected(final_name.error());
  auto existing = impl_->load_file(*final_name, chunk.offset);
  if (existing.has_value()) {
    if (existing->encoded_bytes != *encoded) {
      return common::make_unexpected(
          corruption("movement snapshot chunk offset already has different durable bytes"));
    }
    return InstalledTabletMovementSnapshotChunk{chunk.offset, chunk.bytes.size(), *final_name,
                                                true};
  }
  if (existing.error().code() != common::StatusCode::kNotFound)
    return common::make_unexpected(existing.error());
  if (chunk.offset != impl_->received_bytes_)
    return common::make_unexpected(unavailable("movement snapshot chunk is not the next offset"));
  if (impl_->chunks_.size() == impl_->config_.maximum_chunks)
    return common::make_unexpected(exhausted("movement snapshot chunk count limit reached"));

  const std::string temp_name = temporary_name(*final_name);
  common::Status cleanup = impl_->directory_.remove_file(temp_name);
  if (cleanup.is_ok())
    cleanup = impl_->directory_.sync();
  else if (cleanup.code() == common::StatusCode::kNotFound)
    cleanup = common::Status::ok();
  if (!cleanup.is_ok())
    return common::make_unexpected(with_context("clean prior movement chunk temporary", cleanup));
  auto temporary =
      impl_->directory_.create_exclusive_regular_file(temp_name, impl_->config_.file_permissions);
  if (!temporary.has_value())
    return common::make_unexpected(temporary.error());
  common::Status operation = temporary->write_all_at(0U, *encoded);
  if (!operation.is_ok())
    return common::make_unexpected(with_context("write movement chunk temporary", operation));
  auto readback_size = temporary->size();
  if (!readback_size.has_value() || *readback_size != encoded->size()) {
    return common::make_unexpected(readback_size.has_value()
                                       ? corruption("movement chunk temporary size changed")
                                       : readback_size.error());
  }
  try {
    std::vector<std::byte> readback(encoded->size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size()) {
      return common::make_unexpected(
          read.has_value() ? corruption("movement chunk readback is incomplete") : read.error());
    }
    auto decoded = decode_tablet_movement_snapshot_chunk_v1(readback, impl_->config_.codec_limits);
    if (!decoded.has_value() || *decoded != chunk) {
      return common::make_unexpected(
          decoded.has_value() ? corruption("movement chunk readback changed") : decoded.error());
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("movement chunk readback allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("movement chunk readback exceeds limits"));
  }
  operation = temporary->sync_all();
  if (!operation.is_ok())
    return common::make_unexpected(with_context("synchronize movement chunk", operation));
  operation = temporary->close();
  if (!operation.is_ok())
    return common::make_unexpected(with_context("close movement chunk temporary", operation));
  operation = impl_->directory_.rename_no_replace({temp_name, *final_name});
  if (!operation.is_ok())
    return common::make_unexpected(with_context("install movement snapshot chunk", operation));
  operation = impl_->directory_.sync();
  if (!operation.is_ok()) {
    return common::make_unexpected(
        impl_->fail(with_context("synchronize movement chunk directory", operation), true));
  }
  impl_->chunks_.push_back({chunk.offset, chunk.bytes.size(), *final_name});
  impl_->received_bytes_ += chunk.bytes.size();
  return InstalledTabletMovementSnapshotChunk{chunk.offset, chunk.bytes.size(), *final_name, false};
}

common::Result<LoadedTabletMovementSnapshotChunk>
TabletMovementSnapshotChunkStorage::load_chunk(const std::uint64_t offset) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("movement snapshot chunk storage was moved from"));
  auto name = tablet_movement_snapshot_chunk_file_name(offset);
  if (!name.has_value())
    return common::make_unexpected(name.error());
  return impl_->load_file(*name, offset);
}

common::Result<std::uint64_t> TabletMovementSnapshotChunkStorage::received_bytes() const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("movement snapshot chunk storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  return impl_->received_bytes_;
}

common::Result<std::vector<std::byte>>
TabletMovementSnapshotChunkStorage::load_received_prefix() const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("movement snapshot chunk storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  try {
    std::vector<std::byte> prefix;
    prefix.reserve(static_cast<std::size_t>(impl_->received_bytes_));
    std::uint64_t expected_offset = 0U;
    for (const ChunkFile& descriptor : impl_->chunks_) {
      if (descriptor.offset != expected_offset)
        return common::make_unexpected(corruption("movement chunk progress is not contiguous"));
      auto loaded = impl_->load_file(descriptor.file_name, descriptor.offset);
      if (!loaded.has_value())
        return common::make_unexpected(loaded.error());
      if (loaded->chunk.bytes.size() != descriptor.payload_bytes)
        return common::make_unexpected(
            corruption("movement chunk progress changed after recovery"));
      prefix.insert(prefix.end(), loaded->chunk.bytes.begin(), loaded->chunk.bytes.end());
      expected_offset += loaded->chunk.bytes.size();
    }
    if (expected_offset != impl_->received_bytes_)
      return common::make_unexpected(corruption("movement chunk progress length changed"));
    return prefix;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("movement snapshot prefix allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("movement snapshot prefix exceeds limits"));
  }
}

common::Result<std::vector<std::byte>> TabletMovementSnapshotChunkStorage::finalize() const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("movement snapshot chunk storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (impl_->received_bytes_ != impl_->config_.session.snapshot.total_bytes)
    return common::make_unexpected(unavailable("movement snapshot prefix is incomplete"));
  auto prefix = load_received_prefix();
  if (!prefix.has_value())
    return common::make_unexpected(prefix.error());
  if (common::crc32c(*prefix) != impl_->config_.session.snapshot.content_crc32c)
    return common::make_unexpected(corruption("movement snapshot content checksum mismatch"));
  return prefix;
}

bool TabletMovementSnapshotChunkStorage::is_usable() const noexcept {
  return impl_ != nullptr && impl_->poison_.is_ok();
}

common::Status TabletMovementSnapshotChunkStorage::poison_status() const {
  return impl_ == nullptr ? invalid("movement snapshot chunk storage was moved from")
                          : impl_->poison_;
}

} // namespace chronos::raft
