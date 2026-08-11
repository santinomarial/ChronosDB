#include "chronos/tiering/tiered_part_loader.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/ingest/sha256.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::tiering {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}
[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}
[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] const manifest::TemporalTabletDescriptor*
find_tablet(const std::span<const manifest::TemporalTabletDescriptor> tablets,
            const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::find(tablets, tablet_id, &manifest::TemporalTabletDescriptor::tablet_id);
  return found == tablets.end() ? nullptr : std::addressof(*found);
}

[[nodiscard]] const manifest::TabletSchemaBinding*
find_binding(const std::span<const manifest::TabletSchemaBinding> bindings,
             const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::find(bindings, tablet_id, &manifest::TabletSchemaBinding::tablet_id);
  return found == bindings.end() ? nullptr : std::addressof(*found);
}

} // namespace

TieredTemporalPartImage::TieredTemporalPartImage(const TieredPartSource source,
                                                 manifest::TemporalPartDescriptor descriptor,
                                                 Storage storage,
                                                 TieredDatabaseStorageSnapshot snapshot) noexcept
    : source_(source), descriptor_(std::move(descriptor)), storage_(std::move(storage)),
      snapshot_(std::move(snapshot)) {}

TieredPartSource TieredTemporalPartImage::source() const noexcept {
  return source_;
}
const manifest::TemporalPartDescriptor& TieredTemporalPartImage::descriptor() const noexcept {
  return descriptor_;
}
common::ByteView TieredTemporalPartImage::bytes() const noexcept {
  if (const auto* local = std::get_if<manifest::LoadedTemporalPartImage>(&storage_))
    return local->bytes();
  return std::get<std::vector<std::byte>>(storage_);
}
const TieredDatabaseStorageSnapshot& TieredTemporalPartImage::snapshot() const noexcept {
  return snapshot_;
}

common::Result<std::vector<std::byte>> load_validated_remote_temporal_part_image(
    const ObjectStore& remote_store, const ColdPartLocationDescriptor& location,
    const manifest::TemporalPartDescriptor& descriptor,
    const manifest::TemporalTabletDescriptor& owner, const schema::TableSchema& schema,
    const manifest::TemporalPartValidationLimits limits) {
  if (location.part_id != descriptor.part_id || location.file_length != descriptor.file_length ||
      location.content_sha256 != descriptor.content_sha256) {
    return common::make_unexpected(corruption("remote CSEG route differs from Manifest v2"));
  }
  if (descriptor.file_length > limits.decode.max_file_length)
    return common::make_unexpected(exhausted("remote CSEG exceeds validation byte limit"));
  if (descriptor.file_length > std::numeric_limits<std::size_t>::max())
    return common::make_unexpected(exhausted("remote CSEG is not addressable on this host"));
  const std::size_t file_length = static_cast<std::size_t>(descriptor.file_length);
  auto metadata = remote_store.stat(location.object_key);
  if (!metadata.has_value())
    return common::make_unexpected(metadata.error());
  if (metadata->key != location.object_key || metadata->size != file_length ||
      metadata->checksum != descriptor.content_sha256) {
    return common::make_unexpected(corruption("remote CSEG metadata differs from Manifest v2"));
  }
  auto remote = remote_store.get_range(location.object_key, 0U, file_length);
  if (!remote.has_value())
    return common::make_unexpected(remote.error());
  if (remote->size() != file_length)
    return common::make_unexpected(corruption("remote CSEG read is incomplete"));
  auto digest = ingest::sha256(*remote);
  if (!digest.has_value())
    return common::make_unexpected(digest.error());
  if (*digest != descriptor.content_sha256)
    return common::make_unexpected(corruption("remote CSEG content digest differs"));
  common::Status validation = manifest::validate_manifest_v2_temporal_part_image(
      descriptor, owner, *remote, schema, limits);
  if (!validation.is_ok())
    return common::make_unexpected(std::move(validation));
  return remote;
}

common::Result<std::vector<TieredTemporalPartImage>> load_tiered_temporal_part_images(
    const TieredDatabaseStorageSnapshot& snapshot, const manifest::ManifestStorage& local_storage,
    const ObjectStore& remote_store, const std::span<const cseg::PartId> part_ids,
    const std::span<const manifest::TabletSchemaBinding> schema_bindings,
    const TieredTemporalPartLoadLimits limits) {
  if (part_ids.empty() || part_ids.size() > limits.maximum_parts ||
      limits.maximum_total_bytes == 0U) {
    return common::make_unexpected(invalid("tiered part load limits or identities are invalid"));
  }
  for (std::size_t index = 1U; index < part_ids.size(); ++index) {
    if (!(part_ids[index - 1U] < part_ids[index]))
      return common::make_unexpected(invalid("tiered part identities are not strictly sorted"));
  }

  try {
    std::vector<const manifest::TemporalPartDescriptor*> descriptors;
    descriptors.reserve(part_ids.size());
    std::uint64_t total_bytes{};
    for (const cseg::PartId& part_id : part_ids) {
      const auto found = std::ranges::find(snapshot.manifest_snapshot().parts(), part_id,
                                           &manifest::TemporalPartDescriptor::part_id);
      if (found == snapshot.manifest_snapshot().parts().end())
        return common::make_unexpected(invalid("tiered part is not selected by the snapshot"));
      const auto next = common::checked_add(total_bytes, found->file_length);
      if (!next.has_value() || *next > limits.maximum_total_bytes)
        return common::make_unexpected(exhausted("tiered part load exceeds byte limit"));
      total_bytes = *next;
      descriptors.push_back(std::addressof(*found));
    }

    std::vector<TieredTemporalPartImage> images;
    images.reserve(part_ids.size());
    for (std::size_t index = 0U; index < part_ids.size(); ++index) {
      const manifest::TemporalPartDescriptor& descriptor = *descriptors[index];
      const std::array one_id{part_ids[index]};
      auto local =
          local_storage.load_temporal_part_images(snapshot.manifest_snapshot().selected_manifest(),
                                                  one_id, schema_bindings, limits.validation);
      if (local.has_value()) {
        if (local->size() != 1U)
          return common::make_unexpected(corruption("local tiered loader returned wrong count"));
        images.push_back(TieredTemporalPartImage{
            TieredPartSource::kLocal, descriptor,
            TieredTemporalPartImage::Storage{std::move(local->front())}, snapshot});
        continue;
      }
      if (local.error().code() != common::StatusCode::kNotFound)
        return common::make_unexpected(local.error());

      const ColdPartLocationDescriptor* location = snapshot.find_cold_location(descriptor.part_id);
      if (location == nullptr || location->file_length != descriptor.file_length ||
          location->content_sha256 != descriptor.content_sha256) {
        return common::make_unexpected(
            corruption("missing local part has no exact pinned cold location"));
      }
      const manifest::TemporalTabletDescriptor* owner =
          find_tablet(snapshot.manifest_snapshot().tablets(), descriptor.tablet_id);
      const manifest::TabletSchemaBinding* binding =
          find_binding(schema_bindings, descriptor.tablet_id);
      const std::shared_ptr<const schema::TableSchema> schema_value =
          binding == nullptr ? nullptr : binding->lineage.get().find(descriptor.schema_id);
      if (owner == nullptr || schema_value == nullptr)
        return common::make_unexpected(invalid("remote CSEG has no exact tablet/schema binding"));
      auto remote = load_validated_remote_temporal_part_image(
          remote_store, *location, descriptor, *owner, *schema_value, limits.validation);
      if (!remote.has_value())
        return common::make_unexpected(remote.error());
      images.push_back(TieredTemporalPartImage{TieredPartSource::kRemote, descriptor,
                                               TieredTemporalPartImage::Storage{std::move(*remote)},
                                               snapshot});
    }
    return images;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tiered part load allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tiered part load exceeds container limits"));
  }
}

} // namespace chronos::tiering
