#include "chronos/live/multi_tablet_subscription_checkpoint_storage.hpp"

#include "chronos/io/posix_io.hpp"

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
constexpr std::string_view kGenerationPrefix = "generation-";
constexpr std::string_view kGenerationSuffix = ".subc";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::size_t kGenerationDigits = 20U;

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

[[nodiscard]] bool valid_identity(const MultiTabletSubscriptionCheckpointStorageIdentity& identity,
                                  const std::size_t maximum_sources) {
  if (identity.database_id.is_nil() || identity.table_id.uuid().is_nil() ||
      identity.schema_id.uuid().is_nil() || identity.schema_version.value() == 0U ||
      identity.sources.empty() || identity.sources.size() > maximum_sources)
    return false;
  for (std::size_t index = 0U; index < identity.sources.size(); ++index) {
    const auto& source = identity.sources[index];
    if (source.tablet_id.uuid().is_nil() || !source.wal_id.is_valid() ||
        (index != 0U && identity.sources[index - 1U].tablet_id >= source.tablet_id))
      return false;
  }
  return true;
}

[[nodiscard]] bool
valid_codec_limits(const MultiTabletSubscriptionCheckpointCodecLimits& limits) noexcept {
  return limits.maximum_checkpoint_bytes >=
             kBoundMultiTabletSubscriptionCheckpointHeaderSize +
                 kMultiTabletSubscriptionCheckpointHeaderSize +
                 kMultiTabletSubscriptionCheckpointTrailerSize +
                 kBoundMultiTabletSubscriptionCheckpointTrailerSize &&
         limits.maximum_checkpoint_bytes <= kMaximumMultiTabletSubscriptionCheckpointSize &&
         limits.maximum_sources != 0U && limits.maximum_sources <= kMaximumResumeTokenSources &&
         limits.maximum_retained_changes != 0U &&
         limits.maximum_retained_changes <= std::numeric_limits<std::uint32_t>::max() &&
         limits.maximum_result_key_bytes != 0U &&
         limits.maximum_result_key_bytes <= std::numeric_limits<std::uint32_t>::max() &&
         limits.maximum_payload_bytes != 0U &&
         limits.maximum_payload_bytes <= std::numeric_limits<std::uint32_t>::max();
}

[[nodiscard]] bool matches(const MultiTabletSubscriptionCheckpointStorageIdentity& expected,
                           const MultiTabletSubscriptionCheckpoint& actual) {
  if (actual.database_id != expected.database_id || actual.table_id != expected.table_id ||
      actual.plan_fingerprint != expected.plan_fingerprint ||
      actual.schema_id != expected.schema_id || actual.schema_version != expected.schema_version ||
      actual.sources.size() != expected.sources.size())
    return false;
  for (std::size_t index = 0U; index < expected.sources.size(); ++index) {
    if (actual.sources[index].latest_position.tablet_id != expected.sources[index].tablet_id ||
        actual.sources[index].latest_position.wal_id != expected.sources[index].wal_id)
      return false;
  }
  return true;
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

[[nodiscard]] common::Result<std::uint64_t> parse_name(const std::string_view name,
                                                       const bool temporary) {
  const std::size_t expected = kGenerationPrefix.size() + kGenerationDigits +
                               kGenerationSuffix.size() +
                               (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(kGenerationPrefix) ||
      !name.substr(kGenerationPrefix.size() + kGenerationDigits).starts_with(kGenerationSuffix) ||
      (temporary && !name.ends_with(kTemporarySuffix)))
    return common::make_unexpected(invalid("subscription checkpoint name is noncanonical"));
  const std::string_view digits = name.substr(kGenerationPrefix.size(), kGenerationDigits);
  std::uint64_t generation{};
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), generation);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() || generation == 0U)
    return common::make_unexpected(invalid("subscription checkpoint generation is invalid"));
  auto canonical = multi_tablet_subscription_checkpoint_generation_file_name(generation);
  if (!canonical.has_value() || !name.starts_with(*canonical))
    return common::make_unexpected(invalid("subscription checkpoint name is noncanonical"));
  return generation;
}

} // namespace

class MultiTabletSubscriptionCheckpointStorage::Impl {
public:
  Impl(MultiTabletSubscriptionCheckpointStorageConfig config, io::PosixDirectory directory,
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
               : unavailable("subscription checkpoint storage is poisoned: " + poison_.message());
  }

  [[nodiscard]] common::Status cleanup_temporaries() const {
    auto entries = directory_.list_entries();
    if (!entries.has_value())
      return entries.error();
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.starts_with(kGenerationPrefix) || !entry.name.ends_with(kTemporarySuffix))
        continue;
      if (!parse_name(entry.name, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile)
        return corruption("recognized subscription checkpoint temporary is noncanonical");
      common::Status status = directory_.remove_file(entry.name);
      if (!status.is_ok())
        return status;
      removed = true;
    }
    return removed ? directory_.sync() : common::Status::ok();
  }

  [[nodiscard]] common::Result<LoadedMultiTabletSubscriptionCheckpoint>
  load_file(const std::string& file_name, const std::uint64_t expected_generation) const {
    common::Status usable = check_usable();
    if (!usable.is_ok())
      return common::make_unexpected(std::move(usable));
    auto file = directory_.open_regular_file(file_name, io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto size = file->size();
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size > config_.codec_limits.maximum_checkpoint_bytes ||
        *size > std::numeric_limits<std::size_t>::max())
      return common::make_unexpected(exhausted("installed subscription checkpoint exceeds limit"));
    try {
      std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value())
        return common::make_unexpected(read.error());
      if (*read != bytes.size())
        return common::make_unexpected(
            corruption("installed subscription checkpoint ended before its exact size"));
      auto decoded =
          decode_bound_multi_tablet_subscription_checkpoint_v1(bytes, config_.codec_limits);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (decoded->checkpoint_generation != expected_generation ||
          !matches(config_.identity, decoded->state))
        return common::make_unexpected(
            corruption("installed subscription checkpoint disagrees with owner or name"));
      return LoadedMultiTabletSubscriptionCheckpoint{file_name, std::move(*decoded),
                                                     std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("subscription checkpoint read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("subscription checkpoint read exceeds limits"));
    }
  }

  MultiTabletSubscriptionCheckpointStorageConfig config_;
  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  common::Status poison_;
};

common::Result<std::string> multi_tablet_subscription_checkpoint_generation_file_name(
    const std::uint64_t checkpoint_generation) {
  if (checkpoint_generation == 0U)
    return common::make_unexpected(invalid("checkpoint generation must be nonzero"));
  std::array<char, kGenerationDigits> digits{};
  const auto converted =
      std::to_chars(digits.data(), digits.data() + digits.size(), checkpoint_generation);
  if (converted.ec != std::errc{})
    return common::make_unexpected(invalid("checkpoint generation is not representable"));
  const std::size_t written = static_cast<std::size_t>(converted.ptr - digits.data());
  std::string result{kGenerationPrefix};
  result.append(kGenerationDigits - written, '0');
  result.append(digits.data(), written);
  result.append(kGenerationSuffix);
  return result;
}

common::Result<MultiTabletSubscriptionCheckpointStorage>
MultiTabletSubscriptionCheckpointStorage::open(
    MultiTabletSubscriptionCheckpointStorageConfig config, const bool create_lock) {
  if (config.directory_path.empty() || !valid_codec_limits(config.codec_limits) ||
      !valid_identity(config.identity, config.codec_limits.maximum_sources) ||
      config.file_permissions == 0U || (config.file_permissions & ~0777U) != 0U)
    return common::make_unexpected(invalid("subscription checkpoint storage config is invalid"));
  auto directory = io::PosixDirectory::open(config.directory_path);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto lock = create_lock
                  ? directory->acquire_exclusive_lock(kLockFileName, config.file_permissions)
                  : directory->acquire_existing_exclusive_lock(kLockFileName);
  if (!lock.has_value())
    return common::make_unexpected(lock.error());
  auto impl = std::make_unique<Impl>(std::move(config), std::move(*directory), std::move(*lock));
  common::Status cleanup = impl->cleanup_temporaries();
  if (!cleanup.is_ok())
    return common::make_unexpected(std::move(cleanup));
  return MultiTabletSubscriptionCheckpointStorage{std::move(impl)};
}

MultiTabletSubscriptionCheckpointStorage::MultiTabletSubscriptionCheckpointStorage(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
MultiTabletSubscriptionCheckpointStorage::~MultiTabletSubscriptionCheckpointStorage() = default;
MultiTabletSubscriptionCheckpointStorage::MultiTabletSubscriptionCheckpointStorage(
    MultiTabletSubscriptionCheckpointStorage&&) noexcept = default;
MultiTabletSubscriptionCheckpointStorage& MultiTabletSubscriptionCheckpointStorage::operator=(
    MultiTabletSubscriptionCheckpointStorage&&) noexcept = default;

common::Result<MultiTabletSubscriptionCheckpointStorage>
MultiTabletSubscriptionCheckpointStorage::create(
    MultiTabletSubscriptionCheckpointStorageConfig config) {
  try {
    return open(std::move(config), true);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription checkpoint storage allocation failed"));
  }
}

common::Result<MultiTabletSubscriptionCheckpointStorage>
MultiTabletSubscriptionCheckpointStorage::open_existing(
    MultiTabletSubscriptionCheckpointStorageConfig config) {
  try {
    return open(std::move(config), false);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription checkpoint storage allocation failed"));
  }
}

common::Result<InstalledMultiTabletSubscriptionCheckpoint>
MultiTabletSubscriptionCheckpointStorage::install(
    const BoundMultiTabletSubscriptionCheckpoint& checkpoint) {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("subscription checkpoint storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (!matches(impl_->config_.identity, checkpoint.state))
    return common::make_unexpected(invalid("subscription checkpoint belongs to another owner"));
  auto encoded =
      encode_bound_multi_tablet_subscription_checkpoint_v1(checkpoint, impl_->config_.codec_limits);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  auto final_name =
      multi_tablet_subscription_checkpoint_generation_file_name(checkpoint.checkpoint_generation);
  if (!final_name.has_value())
    return common::make_unexpected(final_name.error());
  auto existing = impl_->load_file(*final_name, checkpoint.checkpoint_generation);
  if (existing.has_value()) {
    if (existing->bytes != *encoded)
      return common::make_unexpected(
          corruption("checkpoint generation already has different durable bytes"));
    return InstalledMultiTabletSubscriptionCheckpoint{checkpoint.checkpoint_generation, *final_name,
                                                      true};
  }
  if (existing.error().code() != common::StatusCode::kNotFound)
    return common::make_unexpected(existing.error());
  auto latest = load_latest();
  if (!latest.has_value())
    return common::make_unexpected(latest.error());
  const std::uint64_t previous_generation =
      latest
          ->transform([](const LoadedMultiTabletSubscriptionCheckpoint& loaded) noexcept {
            return loaded.checkpoint.checkpoint_generation;
          })
          .value_or(0U);
  if (previous_generation == std::numeric_limits<std::uint64_t>::max())
    return common::make_unexpected(exhausted("subscription checkpoint generation is exhausted"));
  const std::uint64_t expected = previous_generation + 1U;
  if (checkpoint.checkpoint_generation != expected)
    return common::make_unexpected(invalid("subscription checkpoint is not the next generation"));

  const std::string temp_name = temporary_name(*final_name);
  common::Status cleanup = impl_->directory_.remove_file(temp_name);
  if (cleanup.is_ok())
    cleanup = impl_->directory_.sync();
  else if (cleanup.code() == common::StatusCode::kNotFound)
    cleanup = common::Status::ok();
  if (!cleanup.is_ok())
    return common::make_unexpected(with_context("clean prior checkpoint temporary", cleanup));
  auto temporary =
      impl_->directory_.create_exclusive_regular_file(temp_name, impl_->config_.file_permissions);
  if (!temporary.has_value())
    return common::make_unexpected(temporary.error());
  common::Status operation = temporary->write_all_at(0U, *encoded);
  if (!operation.is_ok())
    return common::make_unexpected(with_context("write checkpoint temporary", operation));
  auto readback_size = temporary->size();
  if (!readback_size.has_value() || *readback_size != encoded->size())
    return common::make_unexpected(readback_size.has_value()
                                       ? corruption("checkpoint temporary size changed")
                                       : readback_size.error());
  try {
    std::vector<std::byte> readback(encoded->size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size())
      return common::make_unexpected(
          read.has_value() ? corruption("checkpoint readback is incomplete") : read.error());
    auto decoded =
        decode_bound_multi_tablet_subscription_checkpoint_v1(readback, impl_->config_.codec_limits);
    if (!decoded.has_value() || *decoded != checkpoint)
      return common::make_unexpected(decoded.has_value() ? corruption("checkpoint readback changed")
                                                         : decoded.error());
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("checkpoint readback allocation failed"));
  }
  operation = temporary->sync_all();
  if (!operation.is_ok())
    return common::make_unexpected(with_context("synchronize checkpoint", operation));
  operation = temporary->close();
  if (!operation.is_ok())
    return common::make_unexpected(with_context("close checkpoint temporary", operation));
  operation = impl_->directory_.rename_no_replace({temp_name, *final_name});
  if (!operation.is_ok())
    return common::make_unexpected(with_context("install checkpoint", operation));
  operation = impl_->directory_.sync();
  if (!operation.is_ok())
    return common::make_unexpected(
        impl_->fail(with_context("synchronize checkpoint directory", operation), true));
  return InstalledMultiTabletSubscriptionCheckpoint{checkpoint.checkpoint_generation, *final_name,
                                                    false};
}

common::Result<LoadedMultiTabletSubscriptionCheckpoint>
MultiTabletSubscriptionCheckpointStorage::load_generation(
    const std::uint64_t checkpoint_generation) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("subscription checkpoint storage was moved from"));
  auto name = multi_tablet_subscription_checkpoint_generation_file_name(checkpoint_generation);
  if (!name.has_value())
    return common::make_unexpected(name.error());
  return impl_->load_file(*name, checkpoint_generation);
}

common::Result<std::optional<LoadedMultiTabletSubscriptionCheckpoint>>
MultiTabletSubscriptionCheckpointStorage::load_latest() const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("subscription checkpoint storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  auto entries = impl_->directory_.list_entries();
  if (!entries.has_value())
    return common::make_unexpected(entries.error());
  std::optional<std::uint64_t> latest;
  std::uint64_t generation_count = 0U;
  for (const io::DirectoryEntry& entry : *entries) {
    if (entry.name == kLockFileName || !entry.name.starts_with(kGenerationPrefix))
      continue;
    if (entry.name.ends_with(kTemporarySuffix)) {
      if (!parse_name(entry.name, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile)
        return common::make_unexpected(
            corruption("subscription checkpoint directory is noncanonical"));
      continue;
    }
    auto parsed = parse_name(entry.name, false);
    if (!parsed.has_value() || entry.type != io::DirectoryEntryType::kRegularFile)
      return common::make_unexpected(
          corruption("subscription checkpoint directory is noncanonical"));
    if (!latest.has_value() || *parsed > *latest)
      latest = *parsed;
    ++generation_count;
  }
  if (!latest.has_value())
    return std::optional<LoadedMultiTabletSubscriptionCheckpoint>{};
  if (*latest != generation_count)
    return common::make_unexpected(
        corruption("subscription checkpoint generations are not contiguous from one"));
  auto loaded = load_generation(*latest);
  if (!loaded.has_value())
    return common::make_unexpected(loaded.error());
  return std::optional<LoadedMultiTabletSubscriptionCheckpoint>{std::move(*loaded)};
}

bool MultiTabletSubscriptionCheckpointStorage::is_usable() const noexcept {
  return impl_ != nullptr && impl_->poison_.is_ok();
}

common::Status MultiTabletSubscriptionCheckpointStorage::poison_status() const {
  return impl_ == nullptr ? invalid("subscription checkpoint storage was moved from")
                          : impl_->poison_;
}

} // namespace chronos::live
