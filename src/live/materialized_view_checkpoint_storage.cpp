#include "chronos/live/materialized_view_checkpoint_storage.hpp"

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

namespace chronos::live {
namespace {

constexpr std::string_view kLockFileName = "LOCK";
constexpr std::string_view kLegacyPrefix = "checkpoint-";
constexpr std::string_view kLegacySuffix = ".mvcp";
constexpr std::string_view kGenerationPrefix = "generation-";
constexpr std::string_view kGenerationSuffix = ".mvcg";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::size_t kCoordinateDigits = 20U;

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

[[nodiscard]] bool valid_identity(const MaterializedViewCheckpointIdentity& identity) noexcept {
  return !identity.database_id.is_nil() && !identity.view_id.is_nil() &&
         !identity.table_id.uuid().is_nil() && !identity.schema_id.uuid().is_nil() &&
         identity.schema_version.value() != 0U;
}

[[nodiscard]] bool valid_limits(const MaterializedViewCheckpointCodecLimits limits) noexcept {
  return limits.maximum_checkpoint_bytes >= kBoundMaterializedViewCheckpointHeaderSize +
                                                kMaterializedViewCheckpointHeaderSize +
                                                kMaterializedViewCheckpointTrailerSize +
                                                kBoundMaterializedViewCheckpointTrailerSize &&
         limits.maximum_checkpoint_bytes <= kMaximumMaterializedViewCheckpointSize &&
         limits.maximum_rows > 0U && limits.maximum_windows > 0U &&
         limits.maximum_window_contributions > 0U;
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

[[nodiscard]] common::Result<std::uint64_t> parse_name(const std::string_view name,
                                                       const std::string_view prefix,
                                                       const std::string_view suffix,
                                                       const bool temporary) try {
  const std::size_t expected = prefix.size() + kCoordinateDigits + suffix.size() +
                               (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(prefix) ||
      !name.substr(prefix.size() + kCoordinateDigits).starts_with(suffix) ||
      (temporary && !name.ends_with(kTemporarySuffix))) {
    return common::make_unexpected(invalid("materialized-view checkpoint name is noncanonical"));
  }
  std::uint64_t coordinate{};
  const std::string_view digits = name.substr(prefix.size(), kCoordinateDigits);
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), coordinate);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size()) {
    return common::make_unexpected(invalid("materialized-view checkpoint sequence is invalid"));
  }
  std::array<char, kCoordinateDigits> encoded{};
  const auto converted = std::to_chars(encoded.data(), encoded.data() + encoded.size(), coordinate);
  if (converted.ec != std::errc{}) {
    return common::make_unexpected(invalid("checkpoint coordinate is not representable"));
  }
  const std::size_t written = static_cast<std::size_t>(converted.ptr - encoded.data());
  std::string canonical{prefix};
  canonical.append(kCoordinateDigits - written, '0');
  canonical.append(encoded.data(), written);
  canonical.append(suffix);
  if (!name.starts_with(canonical)) {
    return common::make_unexpected(
        invalid("materialized-view checkpoint sequence name is noncanonical"));
  }
  return coordinate;
} catch (const std::bad_alloc&) {
  return common::make_unexpected(exhausted("materialized-view checkpoint name allocation failed"));
} catch (const std::length_error&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint name exceeds allocation limits"));
}

} // namespace

class MaterializedViewCheckpointStorage::Impl {
public:
  Impl(MaterializedViewCheckpointStorageConfig config, io::PosixDirectory directory,
       io::PosixAdvisoryLock lock) noexcept
      : config_(std::move(config)), directory_(std::move(directory)), lock_(std::move(lock)) {}

  [[nodiscard]] common::Status fail(common::Status status, const bool poison = false) {
    if (poison && poison_.is_ok()) {
      poison_ = status;
    }
    return status;
  }

  [[nodiscard]] common::Status check_usable() const {
    return poison_.is_ok() ? common::Status::ok()
                           : unavailable("materialized-view checkpoint storage is poisoned: " +
                                         poison_.message());
  }

  [[nodiscard]] common::Status cleanup_temporaries() const {
    auto entries = directory_.list_entries();
    if (!entries.has_value()) {
      return entries.error();
    }
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.ends_with(kTemporarySuffix) ||
          (!entry.name.starts_with(kLegacyPrefix) && !entry.name.starts_with(kGenerationPrefix))) {
        continue;
      }
      const bool generation = entry.name.starts_with(kGenerationPrefix);
      auto parsed = parse_name(entry.name, generation ? kGenerationPrefix : kLegacyPrefix,
                               generation ? kGenerationSuffix : kLegacySuffix, true);
      if (!parsed.has_value()) {
        if (parsed.error().code() == common::StatusCode::kResourceExhausted) {
          return parsed.error();
        }
        return corruption("recognized materialized-view temporary is noncanonical");
      }
      if (entry.type != io::DirectoryEntryType::kRegularFile) {
        return corruption("recognized materialized-view temporary is noncanonical");
      }
      common::Status status = directory_.remove_file(entry.name);
      if (!status.is_ok()) {
        return status;
      }
      removed = true;
    }
    return removed ? directory_.sync() : common::Status::ok();
  }

  [[nodiscard]] common::Result<LoadedMaterializedViewCheckpoint>
  load_file(const std::string& file_name, const std::optional<std::uint64_t> expected_sequence,
            const std::uint64_t expected_generation) const {
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
    if (*size > config_.codec_limits.maximum_checkpoint_bytes ||
        *size > std::numeric_limits<std::size_t>::max()) {
      return common::make_unexpected(
          exhausted("installed materialized-view checkpoint exceeds limit"));
    }
    try {
      std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value()) {
        return common::make_unexpected(read.error());
      }
      if (*read != bytes.size()) {
        return common::make_unexpected(
            corruption("installed materialized-view checkpoint ended before its exact size"));
      }
      auto decoded = decode_bound_materialized_view_checkpoint_v1(bytes, config_.codec_limits);
      if (!decoded.has_value()) {
        return common::make_unexpected(decoded.error());
      }
      if (decoded->identity != config_.identity ||
          decoded->checkpoint_generation != expected_generation ||
          (expected_sequence.has_value() &&
           decoded->state.position.record_sequence != *expected_sequence)) {
        return common::make_unexpected(
            corruption("installed materialized-view checkpoint disagrees with owner or name"));
      }
      return LoadedMaterializedViewCheckpoint{
          .file_name = file_name, .checkpoint = std::move(*decoded), .bytes = std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("materialized-view checkpoint read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(
          exhausted("materialized-view checkpoint read exceeded limits"));
    }
  }

  MaterializedViewCheckpointStorageConfig config_;
  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  common::Status poison_;
};

common::Result<std::string>
materialized_view_checkpoint_file_name(const std::uint64_t record_sequence) try {
  std::array<char, kCoordinateDigits> digits{};
  const auto converted =
      std::to_chars(digits.data(), digits.data() + digits.size(), record_sequence);
  if (converted.ec != std::errc{}) {
    return common::make_unexpected(invalid("checkpoint sequence is not representable"));
  }
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kLegacyPrefix};
  result.append(kCoordinateDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kLegacySuffix);
  return result;
} catch (const std::bad_alloc&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint filename allocation failed"));
} catch (const std::length_error&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint filename exceeds allocation limits"));
}

common::Result<std::string>
materialized_view_checkpoint_generation_file_name(const std::uint64_t checkpoint_generation) try {
  if (checkpoint_generation == 0U) {
    return common::make_unexpected(invalid("checkpoint generation must be nonzero"));
  }
  std::array<char, kCoordinateDigits> digits{};
  const auto converted =
      std::to_chars(digits.data(), digits.data() + digits.size(), checkpoint_generation);
  if (converted.ec != std::errc{}) {
    return common::make_unexpected(invalid("checkpoint generation is not representable"));
  }
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kGenerationPrefix};
  result.append(kCoordinateDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kGenerationSuffix);
  return result;
} catch (const std::bad_alloc&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint generation filename allocation failed"));
} catch (const std::length_error&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint generation filename exceeds allocation limits"));
}

common::Result<MaterializedViewCheckpointStorage>
MaterializedViewCheckpointStorage::open(MaterializedViewCheckpointStorageConfig config,
                                        const bool create_lock) {
  if (config.directory_path.empty() || !valid_identity(config.identity) ||
      !valid_limits(config.codec_limits) || config.file_permissions == 0U ||
      (config.file_permissions & ~0777U) != 0U) {
    return common::make_unexpected(
        invalid("materialized-view checkpoint storage config is invalid"));
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
  auto impl = std::make_unique<MaterializedViewCheckpointStorage::Impl>(
      std::move(config), std::move(*directory), std::move(*lock));
  common::Status cleanup = impl->cleanup_temporaries();
  if (!cleanup.is_ok()) {
    return common::make_unexpected(std::move(cleanup));
  }
  return MaterializedViewCheckpointStorage{std::move(impl)};
}

MaterializedViewCheckpointStorage::MaterializedViewCheckpointStorage(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
MaterializedViewCheckpointStorage::~MaterializedViewCheckpointStorage() = default;
MaterializedViewCheckpointStorage::MaterializedViewCheckpointStorage(
    MaterializedViewCheckpointStorage&&) noexcept = default;
MaterializedViewCheckpointStorage& MaterializedViewCheckpointStorage::operator=(
    MaterializedViewCheckpointStorage&&) noexcept = default;

common::Result<MaterializedViewCheckpointStorage>
MaterializedViewCheckpointStorage::create(MaterializedViewCheckpointStorageConfig config) {
  try {
    return open(std::move(config), true);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint storage allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint storage allocation exceeds limits"));
  }
}

common::Result<MaterializedViewCheckpointStorage>
MaterializedViewCheckpointStorage::open_existing(MaterializedViewCheckpointStorageConfig config) {
  try {
    return open(std::move(config), false);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint storage allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("materialized-view checkpoint storage allocation exceeds limits"));
  }
}

common::Result<InstalledMaterializedViewCheckpoint>
MaterializedViewCheckpointStorage::install(const BoundMaterializedViewCheckpoint& checkpoint) try {
  if (impl_ == nullptr) {
    return common::make_unexpected(invalid("materialized-view checkpoint storage was moved from"));
  }
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok()) {
    return common::make_unexpected(std::move(usable));
  }
  if (checkpoint.identity != impl_->config_.identity) {
    return common::make_unexpected(invalid("materialized-view checkpoint belongs to another view"));
  }
  auto encoded =
      encode_bound_materialized_view_checkpoint_v1(checkpoint, impl_->config_.codec_limits);
  if (!encoded.has_value()) {
    return common::make_unexpected(encoded.error());
  }
  const std::uint64_t sequence = checkpoint.state.position.record_sequence;
  auto final_name =
      checkpoint.checkpoint_generation == 0U
          ? materialized_view_checkpoint_file_name(sequence)
          : materialized_view_checkpoint_generation_file_name(checkpoint.checkpoint_generation);
  if (!final_name.has_value()) {
    return common::make_unexpected(final_name.error());
  }
  auto existing = impl_->load_file(*final_name,
                                   checkpoint.checkpoint_generation == 0U
                                       ? std::optional<std::uint64_t>{sequence}
                                       : std::nullopt,
                                   checkpoint.checkpoint_generation);
  if (existing.has_value()) {
    if (existing->bytes != *encoded) {
      return common::make_unexpected(
          corruption("checkpoint sequence already has different durable bytes"));
    }
    return InstalledMaterializedViewCheckpoint{.checkpoint_generation =
                                                   checkpoint.checkpoint_generation,
                                               .record_sequence = sequence,
                                               .file_name = *final_name,
                                               .already_present = true};
  }
  if (existing.error().code() != common::StatusCode::kNotFound) {
    return common::make_unexpected(existing.error());
  }
  auto latest = load_latest();
  if (!latest.has_value()) {
    return common::make_unexpected(latest.error());
  }
  const std::uint64_t previous_generation =
      latest
          ->transform([](const LoadedMaterializedViewCheckpoint& loaded) noexcept {
            return loaded.checkpoint.checkpoint_generation;
          })
          .value_or(0U);
  if ((checkpoint.checkpoint_generation == 0U && previous_generation != 0U) ||
      (checkpoint.checkpoint_generation != 0U &&
       previous_generation > checkpoint.checkpoint_generation)) {
    return common::make_unexpected(
        invalid("materialized-view checkpoint generation would move durable state backward"));
  }

  const std::string temp_name = temporary_name(*final_name);
  common::Status cleanup = impl_->directory_.remove_file(temp_name);
  if (cleanup.is_ok()) {
    cleanup = impl_->directory_.sync();
  } else if (cleanup.code() == common::StatusCode::kNotFound) {
    cleanup = common::Status::ok();
  }
  if (!cleanup.is_ok()) {
    return common::make_unexpected(with_context("clean prior checkpoint temporary", cleanup));
  }
  auto temporary =
      impl_->directory_.create_exclusive_regular_file(temp_name, impl_->config_.file_permissions);
  if (!temporary.has_value()) {
    return common::make_unexpected(temporary.error());
  }
  common::Status operation = temporary->write_all_at(0U, *encoded);
  if (!operation.is_ok()) {
    return common::make_unexpected(with_context("write checkpoint temporary", operation));
  }
  auto readback_size = temporary->size();
  if (!readback_size.has_value() || *readback_size != encoded->size()) {
    return common::make_unexpected(readback_size.has_value()
                                       ? corruption("checkpoint temporary size changed")
                                       : readback_size.error());
  }
  try {
    std::vector<std::byte> readback(encoded->size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size()) {
      return common::make_unexpected(
          read.has_value() ? corruption("checkpoint readback is incomplete") : read.error());
    }
    auto decoded =
        decode_bound_materialized_view_checkpoint_v1(readback, impl_->config_.codec_limits);
    if (!decoded.has_value() || *decoded != checkpoint) {
      return common::make_unexpected(decoded.has_value() ? corruption("checkpoint readback changed")
                                                         : decoded.error());
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("checkpoint readback allocation failed"));
  }
  operation = temporary->sync_all();
  if (!operation.is_ok()) {
    return common::make_unexpected(with_context("synchronize checkpoint", operation));
  }
  operation = temporary->close();
  if (!operation.is_ok()) {
    return common::make_unexpected(with_context("close checkpoint temporary", operation));
  }
  operation = impl_->directory_.rename_no_replace({.old_name = temp_name, .new_name = *final_name});
  if (!operation.is_ok()) {
    return common::make_unexpected(with_context("install checkpoint", operation));
  }
  operation = impl_->directory_.sync();
  if (!operation.is_ok()) {
    return common::make_unexpected(
        impl_->fail(with_context("synchronize checkpoint directory", operation), true));
  }
  return InstalledMaterializedViewCheckpoint{.checkpoint_generation =
                                                 checkpoint.checkpoint_generation,
                                             .record_sequence = sequence,
                                             .file_name = *final_name,
                                             .already_present = false};
} catch (const std::bad_alloc&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint install allocation failed"));
} catch (const std::length_error&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint install allocation exceeds limits"));
}

common::Result<LoadedMaterializedViewCheckpoint>
MaterializedViewCheckpointStorage::load(const std::uint64_t record_sequence) const try {
  if (impl_ == nullptr) {
    return common::make_unexpected(invalid("materialized-view checkpoint storage was moved from"));
  }
  auto name = materialized_view_checkpoint_file_name(record_sequence);
  if (!name.has_value()) {
    return common::make_unexpected(name.error());
  }
  return impl_->load_file(*name, record_sequence, 0U);
} catch (const std::bad_alloc&) {
  return common::make_unexpected(exhausted("materialized-view checkpoint load allocation failed"));
} catch (const std::length_error&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint load allocation exceeds limits"));
}

common::Result<LoadedMaterializedViewCheckpoint>
MaterializedViewCheckpointStorage::load_generation(const std::uint64_t checkpoint_generation) const
    try {
  if (impl_ == nullptr) {
    return common::make_unexpected(invalid("materialized-view checkpoint storage was moved from"));
  }
  auto name = materialized_view_checkpoint_generation_file_name(checkpoint_generation);
  if (!name.has_value()) {
    return common::make_unexpected(name.error());
  }
  return impl_->load_file(*name, std::nullopt, checkpoint_generation);
} catch (const std::bad_alloc&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint generation load allocation failed"));
} catch (const std::length_error&) {
  return common::make_unexpected(
      exhausted("materialized-view checkpoint generation load allocation exceeds limits"));
}

common::Result<std::optional<LoadedMaterializedViewCheckpoint>>
MaterializedViewCheckpointStorage::load_latest() const try {
  if (impl_ == nullptr) {
    return common::make_unexpected(invalid("materialized-view checkpoint storage was moved from"));
  }
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok()) {
    return common::make_unexpected(std::move(usable));
  }
  auto entries = impl_->directory_.list_entries();
  if (!entries.has_value()) {
    return common::make_unexpected(entries.error());
  }
  std::optional<std::uint64_t> latest_generation;
  std::optional<std::uint64_t> latest_legacy_sequence;
  for (const io::DirectoryEntry& entry : *entries) {
    if (entry.name == kLockFileName ||
        (!entry.name.starts_with(kLegacyPrefix) && !entry.name.starts_with(kGenerationPrefix))) {
      continue;
    }
    const bool generation = entry.name.starts_with(kGenerationPrefix);
    const bool temporary = entry.name.ends_with(kTemporarySuffix);
    auto parsed = parse_name(entry.name, generation ? kGenerationPrefix : kLegacyPrefix,
                             generation ? kGenerationSuffix : kLegacySuffix, temporary);
    if (!parsed.has_value()) {
      if (parsed.error().code() == common::StatusCode::kResourceExhausted) {
        return common::make_unexpected(parsed.error());
      }
      return common::make_unexpected(corruption("checkpoint directory is noncanonical"));
    }
    if (entry.type != io::DirectoryEntryType::kRegularFile) {
      return common::make_unexpected(corruption("checkpoint directory is noncanonical"));
    }
    if (temporary) {
      continue;
    }
    auto& latest = generation ? latest_generation : latest_legacy_sequence;
    if (!latest.has_value() || *parsed > *latest) {
      latest = *parsed;
    }
  }
  if (latest_generation.has_value()) {
    auto loaded = load_generation(*latest_generation);
    if (!loaded.has_value()) {
      return common::make_unexpected(loaded.error());
    }
    return std::optional<LoadedMaterializedViewCheckpoint>{std::move(*loaded)};
  }
  if (!latest_legacy_sequence.has_value()) {
    return std::optional<LoadedMaterializedViewCheckpoint>{};
  }
  auto loaded = load(*latest_legacy_sequence);
  if (!loaded.has_value()) {
    return common::make_unexpected(loaded.error());
  }
  return std::optional<LoadedMaterializedViewCheckpoint>{std::move(*loaded)};
} catch (const std::bad_alloc&) {
  return common::make_unexpected(
      exhausted("latest materialized-view checkpoint load allocation failed"));
} catch (const std::length_error&) {
  return common::make_unexpected(
      exhausted("latest materialized-view checkpoint load allocation exceeds limits"));
}

bool MaterializedViewCheckpointStorage::is_usable() const noexcept {
  return impl_ != nullptr && impl_->poison_.is_ok();
}

common::Status MaterializedViewCheckpointStorage::poison_status() const {
  return impl_ == nullptr ? invalid("materialized-view checkpoint storage was moved from")
                          : impl_->poison_;
}

} // namespace chronos::live
