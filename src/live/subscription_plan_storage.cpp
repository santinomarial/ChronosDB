#include "chronos/live/subscription_plan_storage.hpp"

#include "chronos/io/posix_io.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

constexpr std::string_view kLockFileName = "LOCK";
constexpr std::string_view kPlanPrefix = "plan-";
constexpr std::string_view kPlanSuffix = ".subp";
constexpr std::string_view kTemporarySuffix = ".tmp";
constexpr std::size_t kFingerprintHexSize = PlanFingerprint{}.size() * 2U;

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

[[nodiscard]] bool
valid_definition_limits(const SubscriptionPlanDefinitionLimits& limits) noexcept {
  return limits.maximum_definition_bytes >=
             kSubscriptionPlanDefinitionHeaderSize + kSubscriptionPlanDefinitionTrailerSize + 1U &&
         limits.maximum_definition_bytes <= kMaximumSubscriptionPlanDefinitionSize &&
         limits.maximum_sql_bytes != 0U &&
         limits.maximum_sql_bytes <= limits.maximum_definition_bytes -
                                         kSubscriptionPlanDefinitionHeaderSize -
                                         kSubscriptionPlanDefinitionTrailerSize;
}

[[nodiscard]] bool is_lower_hex(const char value) noexcept {
  return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f');
}

[[nodiscard]] bool canonical_name(const std::string_view name, const bool temporary) noexcept {
  const std::size_t expected = kPlanPrefix.size() + kFingerprintHexSize + kPlanSuffix.size() +
                               (temporary ? kTemporarySuffix.size() : 0U);
  if (name.size() != expected || !name.starts_with(kPlanPrefix) ||
      !(temporary ? name.ends_with(kTemporarySuffix) : name.ends_with(kPlanSuffix)))
    return false;
  const std::string_view hex{name.data() + kPlanPrefix.size(), kFingerprintHexSize};
  if (!std::ranges::all_of(hex, is_lower_hex))
    return false;
  const std::size_t suffix_offset = kPlanPrefix.size() + kFingerprintHexSize;
  const std::string_view suffix{name.data() + suffix_offset, name.size() - suffix_offset};
  return suffix.starts_with(kPlanSuffix);
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string result{final_name};
  result.append(kTemporarySuffix);
  return result;
}

} // namespace

class SubscriptionPlanStorage::Impl {
public:
  struct LoadedRaw {
    SubscriptionPlanDefinition definition;
    std::vector<std::byte> bytes;
  };

  Impl(SubscriptionPlanStorageConfig configured, io::PosixDirectory owned_directory,
       io::PosixAdvisoryLock owned_lock) noexcept
      : config(std::move(configured)), directory(std::move(owned_directory)),
        lock(std::move(owned_lock)) {}

  [[nodiscard]] common::Status check_usable() const {
    return poison.is_ok()
               ? common::Status::ok()
               : unavailable("subscription plan storage is poisoned: " + poison.message());
  }

  [[nodiscard]] common::Status fail(common::Status status, const bool should_poison) {
    if (should_poison && poison.is_ok())
      poison = status;
    return status;
  }

  [[nodiscard]] common::Status cleanup_temporaries() const {
    auto entries = directory.list_entries();
    if (!entries.has_value())
      return entries.error();
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.starts_with(kPlanPrefix) || !entry.name.ends_with(kTemporarySuffix))
        continue;
      if (!canonical_name(entry.name, true) || entry.type != io::DirectoryEntryType::kRegularFile)
        return corruption("recognized subscription plan temporary is noncanonical");
      common::Status status = directory.remove_file(entry.name);
      if (!status.is_ok())
        return status;
      removed = true;
    }
    return removed ? directory.sync() : common::Status::ok();
  }

  [[nodiscard]] common::Result<LoadedRaw> load_raw(const PlanFingerprint& fingerprint) const {
    common::Status usable = check_usable();
    if (!usable.is_ok())
      return common::make_unexpected(std::move(usable));
    try {
      const std::string file_name = subscription_plan_file_name(fingerprint);
      auto file = directory.open_regular_file(file_name, io::FileOpenMode::kReadOnly);
      if (!file.has_value())
        return common::make_unexpected(file.error());
      auto size = file->size();
      if (!size.has_value())
        return common::make_unexpected(size.error());
      if (*size > config.definition_limits.maximum_definition_bytes ||
          *size > std::numeric_limits<std::size_t>::max())
        return common::make_unexpected(exhausted("stored subscription plan exceeds its limit"));
      std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value())
        return common::make_unexpected(read.error());
      if (*read != bytes.size())
        return common::make_unexpected(corruption("stored subscription plan is truncated"));
      auto decoded = decode_subscription_plan_definition_v1(bytes, config.definition_limits);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (decoded->database_id != config.database_id || decoded->plan_fingerprint != fingerprint)
        return common::make_unexpected(
            corruption("stored subscription plan disagrees with its owner or filename"));
      return LoadedRaw{std::move(*decoded), std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("subscription plan read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("subscription plan read exceeds limits"));
    }
  }

  SubscriptionPlanStorageConfig config;
  io::PosixDirectory directory;
  io::PosixAdvisoryLock lock;
  common::Status poison;
};

std::string subscription_plan_file_name(const PlanFingerprint& fingerprint) {
  static constexpr std::array<char, 16U> kHex{'0', '1', '2', '3', '4', '5', '6', '7',
                                              '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string name{kPlanPrefix};
  name.reserve(kPlanPrefix.size() + kFingerprintHexSize + kPlanSuffix.size());
  for (const std::byte value : fingerprint) {
    const std::uint8_t byte = std::to_integer<std::uint8_t>(value);
    name.push_back(kHex[byte >> 4U]);
    name.push_back(kHex[byte & 0x0fU]);
  }
  name.append(kPlanSuffix);
  return name;
}

common::Result<SubscriptionPlanStorage>
SubscriptionPlanStorage::open(SubscriptionPlanStorageConfig config, const bool create_lock) {
  if (config.directory_path.empty() || config.database_id.is_nil() ||
      !valid_definition_limits(config.definition_limits) || config.file_permissions == 0U ||
      (config.file_permissions & ~0777U) != 0U)
    return common::make_unexpected(invalid("subscription plan storage config is invalid"));
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
  return SubscriptionPlanStorage{std::move(impl)};
}

SubscriptionPlanStorage::SubscriptionPlanStorage(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
SubscriptionPlanStorage::~SubscriptionPlanStorage() = default;
SubscriptionPlanStorage::SubscriptionPlanStorage(SubscriptionPlanStorage&&) noexcept = default;
SubscriptionPlanStorage&
SubscriptionPlanStorage::operator=(SubscriptionPlanStorage&&) noexcept = default;

common::Result<SubscriptionPlanStorage>
SubscriptionPlanStorage::create(SubscriptionPlanStorageConfig config) {
  try {
    return open(std::move(config), true);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription plan storage allocation failed"));
  }
}

common::Result<SubscriptionPlanStorage>
SubscriptionPlanStorage::open_existing(SubscriptionPlanStorageConfig config) {
  try {
    return open(std::move(config), false);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription plan storage allocation failed"));
  }
}

common::Result<InstalledSubscriptionPlan>
SubscriptionPlanStorage::install(const std::string_view sql,
                                 std::shared_ptr<const query::QueryCatalogSnapshot> catalog) {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("subscription plan storage was moved from"));
  common::Status usable = impl_->check_usable();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  try {
    auto prepared = prepare_subscription_plan(sql, std::move(catalog), impl_->config.plan_limits);
    if (!prepared.has_value())
      return common::make_unexpected(prepared.error().status());
    SubscriptionPlanDefinition definition{impl_->config.database_id,
                                          prepared->schema_ptr()->table_id(),
                                          prepared->schema_ptr()->schema_id(),
                                          prepared->schema_ptr()->version(),
                                          prepared->fingerprint(),
                                          std::string{sql}};
    auto encoded =
        encode_subscription_plan_definition_v1(definition, impl_->config.definition_limits);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    const std::string final_name = subscription_plan_file_name(definition.plan_fingerprint);
    auto existing = impl_->load_raw(definition.plan_fingerprint);
    if (existing.has_value()) {
      if (existing->bytes != *encoded)
        return common::make_unexpected(
            corruption("subscription plan fingerprint already has different durable bytes"));
      return InstalledSubscriptionPlan{definition.plan_fingerprint, final_name, true};
    }
    if (existing.error().code() != common::StatusCode::kNotFound)
      return common::make_unexpected(existing.error());

    const std::string temp_name = temporary_name(final_name);
    common::Status cleanup = impl_->directory.remove_file(temp_name);
    if (cleanup.is_ok())
      cleanup = impl_->directory.sync();
    else if (cleanup.code() == common::StatusCode::kNotFound)
      cleanup = common::Status::ok();
    if (!cleanup.is_ok())
      return common::make_unexpected(with_context("clean prior plan temporary", cleanup));
    auto temporary =
        impl_->directory.create_exclusive_regular_file(temp_name, impl_->config.file_permissions);
    if (!temporary.has_value())
      return common::make_unexpected(temporary.error());
    common::Status operation = temporary->write_all_at(0U, *encoded);
    if (!operation.is_ok())
      return common::make_unexpected(with_context("write subscription plan temporary", operation));
    std::vector<std::byte> readback(encoded->size());
    auto read = temporary->read_at(0U, readback);
    if (!read.has_value() || *read != readback.size())
      return common::make_unexpected(
          read.has_value() ? corruption("subscription plan readback is incomplete") : read.error());
    auto decoded =
        decode_subscription_plan_definition_v1(readback, impl_->config.definition_limits);
    if (!decoded.has_value() || *decoded != definition)
      return common::make_unexpected(
          decoded.has_value() ? corruption("subscription plan readback changed") : decoded.error());
    operation = temporary->sync_all();
    if (!operation.is_ok())
      return common::make_unexpected(with_context("synchronize subscription plan", operation));
    operation = temporary->close();
    if (!operation.is_ok())
      return common::make_unexpected(with_context("close subscription plan temporary", operation));
    operation = impl_->directory.rename_no_replace({temp_name, final_name});
    if (!operation.is_ok())
      return common::make_unexpected(with_context("install subscription plan", operation));
    operation = impl_->directory.sync();
    if (!operation.is_ok())
      return common::make_unexpected(
          impl_->fail(with_context("synchronize subscription plan directory", operation), true));
    return InstalledSubscriptionPlan{definition.plan_fingerprint, final_name, false};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription plan installation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription plan installation exceeds limits"));
  }
}

common::Result<PreparedSubscriptionPlan>
SubscriptionPlanStorage::load(const PlanFingerprint& fingerprint,
                              std::shared_ptr<const query::QueryCatalogSnapshot> catalog) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("subscription plan storage was moved from"));
  try {
    auto raw = impl_->load_raw(fingerprint);
    if (!raw.has_value())
      return common::make_unexpected(raw.error());
    auto prepared = prepare_subscription_plan(raw->definition.sql, std::move(catalog),
                                              impl_->config.plan_limits);
    if (!prepared.has_value())
      return common::make_unexpected(
          with_context("reprepare stored subscription plan", prepared.error().status()));
    if (prepared->fingerprint() != raw->definition.plan_fingerprint ||
        prepared->schema_ptr()->table_id() != raw->definition.table_id ||
        prepared->schema_ptr()->schema_id() != raw->definition.schema_id ||
        prepared->schema_ptr()->version() != raw->definition.schema_version)
      return common::make_unexpected(
          common::Status{common::StatusCode::kNotSupported,
                         "stored subscription plan is incompatible with the current catalog"});
    return std::move(*prepared);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("subscription plan load allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("subscription plan load exceeds limits"));
  }
}

bool SubscriptionPlanStorage::is_usable() const noexcept {
  return impl_ != nullptr && impl_->poison.is_ok();
}

common::Status SubscriptionPlanStorage::poison_status() const {
  return impl_ == nullptr ? invalid("subscription plan storage was moved from") : impl_->poison;
}

} // namespace chronos::live
