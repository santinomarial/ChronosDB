#include "chronos/tiering/tiered_parts.hpp"

#include "chronos/ingest/sha256.hpp"
#include "chronos/manifest/naming.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {
[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}
} // namespace

class TieredPartManager::Impl {
public:
  struct CacheEntry {
    std::vector<std::byte> bytes;
    std::list<cseg::PartId>::iterator recency;
  };
  Impl(ObjectStore& remote, TieringLimits configured) : store(&remote), limits(configured) {}

  void touch(const cseg::PartId& part, CacheEntry& entry) {
    recency.erase(entry.recency);
    recency.push_front(part);
    entry.recency = recency.begin();
  }
  void evict_for(const std::size_t bytes) {
    while (!recency.empty() && (cache.size() >= limits.maximum_cache_entries ||
                                bytes > limits.maximum_cache_bytes - cache_bytes)) {
      const cseg::PartId victim = recency.back();
      recency.pop_back();
      const auto found = cache.find(victim);
      cache_bytes -= found->second.bytes.size();
      cache.erase(found);
    }
  }

  ObjectStore* store;
  TieringLimits limits;
  std::map<cseg::PartId, ColdPartDescriptor> parts;
  std::map<cseg::PartId, CacheEntry> cache;
  std::list<cseg::PartId> recency;
  std::size_t cache_bytes{};
};

TieredPartManager::TieredPartManager(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
TieredPartManager::~TieredPartManager() = default;
TieredPartManager::TieredPartManager(TieredPartManager&&) noexcept = default;
TieredPartManager& TieredPartManager::operator=(TieredPartManager&&) noexcept = default;

common::Result<TieredPartManager> TieredPartManager::create(ObjectStore& store,
                                                            const TieringLimits limits) {
  if (limits.maximum_parts == 0U || limits.maximum_object_bytes == 0U ||
      limits.maximum_cache_bytes == 0U || limits.maximum_cache_entries == 0U) {
    return common::make_unexpected(invalid("tiering limits must be finite and nonzero"));
  }
  return TieredPartManager{std::make_unique<Impl>(store, limits)};
}

common::Result<TieringReceipt> TieredPartManager::upload_and_install(
    ColdPartDescriptor descriptor, const common::ByteView local_cseg,
    const TieredPartAdmission& admission, const ColdManifestInstaller& install_manifest) {
  if (descriptor.object_key.empty() || descriptor.part.file_length != local_cseg.size() ||
      local_cseg.empty() || local_cseg.size() > impl_->limits.maximum_object_bytes ||
      !install_manifest) {
    return common::make_unexpected(invalid("cold part descriptor, bytes, or installer is invalid"));
  }
  try {
    const std::string file_name = manifest::part_file_name(descriptor.part.part_id);
    common::Status valid = manifest::validate_manifest_v1_part_image(
        descriptor.part, admission.wal_id, admission.schema.get(),
        {.file_name = file_name, .bytes = local_cseg}, admission.validation_limits);
    if (!valid.is_ok())
      return common::make_unexpected(std::move(valid));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted, "CSEG admission allocation failed"});
  } catch (const std::length_error&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted, "CSEG admission exceeded limits"});
  }
  auto checksum = ingest::sha256(local_cseg);
  if (!checksum.has_value())
    return common::make_unexpected(checksum.error());
  if (*checksum != descriptor.checksum) {
    return common::make_unexpected(invalid("cold part checksum does not match local CSEG"));
  }
  const auto existing = impl_->parts.find(descriptor.part.part_id);
  if (existing != impl_->parts.end()) {
    if (existing->second != descriptor) {
      return common::make_unexpected(common::Status{common::StatusCode::kAlreadyExists,
                                                    "part identity already names another object"});
    }
    return TieringReceipt{existing->second, true};
  }
  if (impl_->parts.size() >= impl_->limits.maximum_parts) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted, "cold part catalog is full"});
  }
  auto uploaded =
      impl_->store->put_if_absent(descriptor.object_key, local_cseg, descriptor.checksum);
  if (!uploaded.has_value())
    return common::make_unexpected(uploaded.error());
  auto verified = impl_->store->stat(descriptor.object_key);
  if (!verified.has_value())
    return common::make_unexpected(verified.error());
  if (verified->size != local_cseg.size() || verified->checksum != descriptor.checksum) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kCorruption, "uploaded object verification failed"});
  }
  const common::Status installed = install_manifest(descriptor);
  if (!installed.is_ok())
    return common::make_unexpected(installed);
  impl_->parts.emplace(descriptor.part.part_id, descriptor);
  return TieringReceipt{std::move(descriptor), true};
}

common::Result<std::vector<std::byte>>
TieredPartManager::read_range(const cseg::PartId& part_id, const std::size_t offset,
                              const std::size_t length,
                              const std::optional<ingest::Sha256Digest>& expected_range_checksum) {
  const auto part = impl_->parts.find(part_id);
  if (part == impl_->parts.end()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "cold part is not installed"});
  }
  if (part->second.part.file_length > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "cold object is not addressable on this host"});
  }
  const std::size_t size = static_cast<std::size_t>(part->second.part.file_length);
  if (offset > size || length > size - offset) {
    return common::make_unexpected(invalid("cold part range is outside object bounds"));
  }
  const auto cached = impl_->cache.find(part_id);
  if (cached != impl_->cache.end()) {
    impl_->touch(part_id, cached->second);
    return std::vector<std::byte>{
        cached->second.bytes.begin() + static_cast<std::ptrdiff_t>(offset),
        cached->second.bytes.begin() + static_cast<std::ptrdiff_t>(offset + length)};
  }

  if (size <= impl_->limits.maximum_cache_bytes) {
    auto full = impl_->store->get_range(part->second.object_key, 0U, size);
    if (!full.has_value())
      return common::make_unexpected(full.error());
    auto checksum = ingest::sha256(*full);
    if (!checksum.has_value())
      return common::make_unexpected(checksum.error());
    if (*checksum != part->second.checksum) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kCorruption, "cold object checksum mismatch"});
    }
    impl_->evict_for(size);
    impl_->recency.push_front(part_id);
    impl_->cache_bytes += size;
    auto [entry, inserted] =
        impl_->cache.emplace(part_id, Impl::CacheEntry{std::move(*full), impl_->recency.begin()});
    static_cast<void>(inserted);
    return std::vector<std::byte>{entry->second.bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                                  entry->second.bytes.begin() +
                                      static_cast<std::ptrdiff_t>(offset + length)};
  }
  auto range = impl_->store->get_range(part->second.object_key, offset, length);
  if (!range.has_value())
    return common::make_unexpected(range.error());
  if (expected_range_checksum.has_value()) {
    auto checksum = ingest::sha256(*range);
    if (!checksum.has_value())
      return common::make_unexpected(checksum.error());
    if (*checksum != *expected_range_checksum) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kCorruption, "cold object range checksum mismatch"});
    }
  }
  return range;
}

std::optional<ColdPartDescriptor> TieredPartManager::find(const cseg::PartId& part_id) const {
  const auto found = impl_->parts.find(part_id);
  return found == impl_->parts.end() ? std::nullopt
                                     : std::optional<ColdPartDescriptor>{found->second};
}
std::size_t TieredPartManager::cached_bytes() const noexcept {
  return impl_->cache_bytes;
}
std::size_t TieredPartManager::cached_entries() const noexcept {
  return impl_->cache.size();
}

} // namespace chronos::tiering
