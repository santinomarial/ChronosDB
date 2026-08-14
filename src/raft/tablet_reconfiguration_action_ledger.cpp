#include "chronos/raft/tablet_reconfiguration_action_ledger.hpp"

#include "chronos/io/posix_io.hpp"

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

constexpr std::string_view kLock = "LOCK";
constexpr std::string_view kPrefix = "action-";
constexpr std::string_view kSuffix = ".ract";
constexpr std::string_view kTemporary = ".tmp";
constexpr std::size_t kEpochDigits = 20U;
constexpr std::size_t kKindDigits = 3U;

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
[[nodiscard]] common::Status context(const std::string_view prefix, const common::Status& status) {
  std::string message{prefix};
  message.append(": ");
  message.append(status.message());
  return {status.code(), std::move(message)};
}

[[nodiscard]] bool valid_kind(const TabletReconfigurationActionKind kind) {
  return kind == TabletReconfigurationActionKind::kBeginJointMembership ||
         kind == TabletReconfigurationActionKind::kFinalizeJointMembership ||
         kind == TabletReconfigurationActionKind::kPublishPlacement;
}

[[nodiscard]] bool valid_limits(const TabletReconfigurationActionCodecLimits& limits) {
  constexpr std::size_t kFraming =
      kTabletReconfigurationActionHeaderSize + kTabletReconfigurationActionTrailerSize;
  return limits.maximum_action_bytes >= kFraming + 8U + sizeof(NodeId) &&
         limits.maximum_action_bytes <= kMaximumTabletReconfigurationActionSize &&
         limits.maximum_voters > 0U &&
         limits.maximum_voters <= (limits.maximum_action_bytes - kFraming - 8U) / sizeof(NodeId);
}

[[nodiscard]] common::Result<TabletReconfigurationActionId>
parse_name(const std::string_view name, const schema::TabletId tablet_id, const bool temporary) {
  const std::size_t base_size = kPrefix.size() + kEpochDigits + 1U + kKindDigits + kSuffix.size();
  const std::size_t expected = base_size + (temporary ? kTemporary.size() : 0U);
  if (name.size() != expected || !name.starts_with(kPrefix) ||
      !name.substr(base_size - kSuffix.size()).starts_with(kSuffix) ||
      (temporary && !name.ends_with(kTemporary))) {
    return common::make_unexpected(invalid("reconfiguration action name is noncanonical"));
  }
  const std::string_view epoch_bytes = name.substr(kPrefix.size(), kEpochDigits);
  if (name[kPrefix.size() + kEpochDigits] != '-')
    return common::make_unexpected(invalid("reconfiguration action name is noncanonical"));
  const std::string_view kind_bytes = name.substr(kPrefix.size() + kEpochDigits + 1U, kKindDigits);
  std::uint64_t epoch{};
  std::uint32_t kind_value{};
  const auto epoch_parsed =
      std::from_chars(epoch_bytes.data(), epoch_bytes.data() + epoch_bytes.size(), epoch);
  const auto kind_parsed =
      std::from_chars(kind_bytes.data(), kind_bytes.data() + kind_bytes.size(), kind_value);
  const auto kind = static_cast<TabletReconfigurationActionKind>(kind_value);
  if (epoch_parsed.ec != std::errc{} || epoch_parsed.ptr != epoch_bytes.end() || epoch == 0U ||
      kind_parsed.ec != std::errc{} || kind_parsed.ptr != kind_bytes.end() || !valid_kind(kind)) {
    return common::make_unexpected(invalid("reconfiguration action coordinate is invalid"));
  }
  TabletReconfigurationActionId id{tablet_id, epoch, kind};
  auto canonical = tablet_reconfiguration_action_file_name(id);
  if (!canonical.has_value() || !name.starts_with(*canonical))
    return common::make_unexpected(invalid("reconfiguration action name is noncanonical"));
  return id;
}

[[nodiscard]] std::string temporary_name(const std::string_view final_name) {
  std::string name{final_name};
  name.append(kTemporary);
  return name;
}

} // namespace

class TabletReconfigurationActionLedger::Impl {
public:
  Impl(TabletReconfigurationActionLedgerConfig config, io::PosixDirectory directory,
       io::PosixAdvisoryLock lock) noexcept
      : config(std::move(config)), directory(std::move(directory)), lock(std::move(lock)) {}

  [[nodiscard]] common::Status check() const {
    return poison.is_ok()
               ? common::Status::ok()
               : unavailable("reconfiguration action ledger is poisoned: " + poison.message());
  }
  [[nodiscard]] common::Status fail(common::Status status, const bool poison_owner = false) {
    if (poison_owner && poison.is_ok())
      poison = status;
    return status;
  }
  [[nodiscard]] common::Status cleanup() const {
    auto entries = directory.list_entries();
    if (!entries.has_value())
      return entries.error();
    bool removed = false;
    for (const io::DirectoryEntry& entry : *entries) {
      if (!entry.name.starts_with(kPrefix) || !entry.name.ends_with(kTemporary))
        continue;
      if (!parse_name(entry.name, config.tablet_id, true).has_value() ||
          entry.type != io::DirectoryEntryType::kRegularFile) {
        return corruption("recognized reconfiguration action temporary is noncanonical");
      }
      common::Status status = directory.remove_file(entry.name);
      if (!status.is_ok())
        return status;
      removed = true;
    }
    return removed ? directory.sync() : common::Status::ok();
  }
  [[nodiscard]] common::Result<LoadedTabletReconfigurationAction>
  load_file(const std::string& file_name, const TabletReconfigurationActionId& expected) const {
    common::Status usable = check();
    if (!usable.is_ok())
      return common::make_unexpected(std::move(usable));
    auto file = directory.open_regular_file(file_name, io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto size = file->size();
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size > config.codec_limits.maximum_action_bytes ||
        *size > std::numeric_limits<std::size_t>::max()) {
      return common::make_unexpected(exhausted("installed action exceeds configured limit"));
    }
    try {
      std::vector<std::byte> bytes(static_cast<std::size_t>(*size));
      auto read = file->read_at(0U, bytes);
      if (!read.has_value())
        return common::make_unexpected(read.error());
      if (*read != bytes.size())
        return common::make_unexpected(corruption("installed action is truncated"));
      auto decoded = decode_tablet_reconfiguration_action_v1(bytes, config.codec_limits);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (decoded->id != expected || decoded->id.tablet_id != config.tablet_id)
        return common::make_unexpected(corruption("installed action disagrees with owner or name"));
      return LoadedTabletReconfigurationAction{file_name, std::move(*decoded), std::move(bytes)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("action read allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("action read exceeds limits"));
    }
  }

  TabletReconfigurationActionLedgerConfig config;
  io::PosixDirectory directory;
  io::PosixAdvisoryLock lock;
  common::Status poison;
};

common::Result<std::string>
tablet_reconfiguration_action_file_name(const TabletReconfigurationActionId& id) {
  if (id.tablet_id.uuid().is_nil() || id.movement_epoch == 0U || !valid_kind(id.kind))
    return common::make_unexpected(invalid("reconfiguration action identity is invalid"));
  std::array<char, kEpochDigits> epoch{};
  std::array<char, kKindDigits> kind{};
  const auto encoded_epoch =
      std::to_chars(epoch.data(), epoch.data() + epoch.size(), id.movement_epoch);
  const auto encoded_kind =
      std::to_chars(kind.data(), kind.data() + kind.size(), static_cast<std::uint32_t>(id.kind));
  if (encoded_epoch.ec != std::errc{} || encoded_kind.ec != std::errc{})
    return common::make_unexpected(
        invalid("reconfiguration action coordinate is not representable"));
  const std::size_t epoch_size = static_cast<std::size_t>(encoded_epoch.ptr - epoch.data());
  const std::size_t kind_size = static_cast<std::size_t>(encoded_kind.ptr - kind.data());
  std::string name{kPrefix};
  name.append(kEpochDigits - epoch_size, '0');
  name.append(epoch.data(), epoch_size);
  name.push_back('-');
  name.append(kKindDigits - kind_size, '0');
  name.append(kind.data(), kind_size);
  name.append(kSuffix);
  return name;
}

common::Result<TabletReconfigurationActionLedger>
TabletReconfigurationActionLedger::open(TabletReconfigurationActionLedgerConfig config,
                                        const bool create_lock) {
  if (config.directory_path.empty() || config.tablet_id.uuid().is_nil() ||
      !valid_limits(config.codec_limits) || config.file_permissions == 0U ||
      (config.file_permissions & ~0777U) != 0U) {
    return common::make_unexpected(invalid("reconfiguration action ledger config is invalid"));
  }
  auto directory = io::PosixDirectory::open(config.directory_path);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto lock = create_lock ? directory->acquire_exclusive_lock(kLock, config.file_permissions)
                          : directory->acquire_existing_exclusive_lock(kLock);
  if (!lock.has_value())
    return common::make_unexpected(lock.error());
  auto impl = std::make_unique<Impl>(std::move(config), std::move(*directory), std::move(*lock));
  common::Status cleaned = impl->cleanup();
  if (!cleaned.is_ok())
    return common::make_unexpected(std::move(cleaned));
  return TabletReconfigurationActionLedger{std::move(impl)};
}

TabletReconfigurationActionLedger::TabletReconfigurationActionLedger(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
TabletReconfigurationActionLedger::~TabletReconfigurationActionLedger() = default;
TabletReconfigurationActionLedger::TabletReconfigurationActionLedger(
    TabletReconfigurationActionLedger&&) noexcept = default;
TabletReconfigurationActionLedger& TabletReconfigurationActionLedger::operator=(
    TabletReconfigurationActionLedger&&) noexcept = default;

common::Result<TabletReconfigurationActionLedger>
TabletReconfigurationActionLedger::create(TabletReconfigurationActionLedgerConfig config) {
  try {
    return open(std::move(config), true);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("action ledger allocation failed"));
  }
}
common::Result<TabletReconfigurationActionLedger>
TabletReconfigurationActionLedger::open_existing(TabletReconfigurationActionLedgerConfig config) {
  try {
    return open(std::move(config), false);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("action ledger allocation failed"));
  }
}

common::Result<PreparedTabletReconfigurationAction>
TabletReconfigurationActionLedger::prepare(const TabletReconfigurationAction& action) {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("action ledger was moved from"));
  common::Status usable = impl_->check();
  if (!usable.is_ok())
    return common::make_unexpected(std::move(usable));
  if (action.id.tablet_id != impl_->config.tablet_id)
    return common::make_unexpected(invalid("action belongs to another tablet"));
  auto encoded = encode_tablet_reconfiguration_action_v1(action, impl_->config.codec_limits);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  auto name = tablet_reconfiguration_action_file_name(action.id);
  if (!name.has_value())
    return common::make_unexpected(name.error());
  auto existing = impl_->load_file(*name, action.id);
  if (existing.has_value()) {
    if (existing->bytes != *encoded)
      return common::make_unexpected(corruption("action identity has different durable bytes"));
    return PreparedTabletReconfigurationAction{action.id, *name, true};
  }
  if (existing.error().code() != common::StatusCode::kNotFound)
    return common::make_unexpected(existing.error());

  const std::string temp = temporary_name(*name);
  common::Status cleaned = impl_->directory.remove_file(temp);
  if (cleaned.is_ok())
    cleaned = impl_->directory.sync();
  else if (cleaned.code() == common::StatusCode::kNotFound)
    cleaned = common::Status::ok();
  if (!cleaned.is_ok())
    return common::make_unexpected(context("clean prior action temporary", cleaned));
  auto file = impl_->directory.create_exclusive_regular_file(temp, impl_->config.file_permissions);
  if (!file.has_value())
    return common::make_unexpected(file.error());
  common::Status status = file->write_all_at(0U, *encoded);
  if (!status.is_ok())
    return common::make_unexpected(context("write action temporary", status));
  std::vector<std::byte> readback(encoded->size());
  auto read = file->read_at(0U, readback);
  if (!read.has_value() || *read != readback.size() || readback != *encoded)
    return common::make_unexpected(read.has_value() ? corruption("action readback changed")
                                                    : read.error());
  auto decoded = decode_tablet_reconfiguration_action_v1(readback, impl_->config.codec_limits);
  if (!decoded.has_value() || decoded->id != action.id)
    return common::make_unexpected(
        decoded.has_value() ? corruption("action readback identity changed") : decoded.error());
  status = file->sync_all();
  if (!status.is_ok())
    return common::make_unexpected(context("synchronize action", status));
  status = file->close();
  if (!status.is_ok())
    return common::make_unexpected(context("close action temporary", status));
  status = impl_->directory.rename_no_replace({temp, *name});
  if (!status.is_ok())
    return common::make_unexpected(context("install action", status));
  status = impl_->directory.sync();
  if (!status.is_ok())
    return common::make_unexpected(
        impl_->fail(context("synchronize action directory", status), true));
  return PreparedTabletReconfigurationAction{action.id, *name, false};
}

common::Result<LoadedTabletReconfigurationAction>
TabletReconfigurationActionLedger::load(const TabletReconfigurationActionId& id) const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("action ledger was moved from"));
  if (id.tablet_id != impl_->config.tablet_id)
    return common::make_unexpected(invalid("action belongs to another tablet"));
  auto name = tablet_reconfiguration_action_file_name(id);
  if (!name.has_value())
    return common::make_unexpected(name.error());
  return impl_->load_file(*name, id);
}

bool TabletReconfigurationActionLedger::is_usable() const noexcept {
  return impl_ != nullptr && impl_->poison.is_ok();
}
common::Status TabletReconfigurationActionLedger::poison_status() const {
  return impl_ == nullptr ? invalid("action ledger was moved from") : impl_->poison;
}

} // namespace chronos::raft
