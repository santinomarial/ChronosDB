#ifndef CHRONOS_TIERING_TIERED_PART_LOADER_HPP_
#define CHRONOS_TIERING_TIERED_PART_LOADER_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/types.hpp"
#include "chronos/manifest/storage.hpp"
#include "chronos/manifest/temporal_part_validation.hpp"
#include "chronos/tiering/object_store.hpp"
#include "chronos/tiering/tiered_publication.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <variant>
#include <vector>

namespace chronos::tiering {

enum class TieredPartSource : std::uint8_t {
  kLocal,
  kRemote,
};

struct TieredTemporalPartLoadLimits {
  std::size_t maximum_parts{1U << 20U};
  std::uint64_t maximum_total_bytes{4ULL * 1024ULL * 1024ULL * 1024ULL};
  manifest::TemporalPartValidationLimits validation{};
};

// Reads one exact immutable object after authoritative per-key metadata checks, recomputes its
// complete SHA-256, and applies the full Manifest-v2 CSEG/schema/source validator.
[[nodiscard]] common::Result<std::vector<std::byte>> load_validated_remote_temporal_part_image(
    const ObjectStore& remote_store, const ColdPartLocationDescriptor& location,
    const manifest::TemporalPartDescriptor& descriptor,
    const manifest::TemporalTabletDescriptor& owner, const schema::TableSchema& schema,
    manifest::TemporalPartValidationLimits limits = {});

// Move-only exact CSEG image retaining the aggregate tiered snapshot that selected its local or
// remote source. Returned byte views and descriptors remain valid for this owner's lifetime.
class TieredTemporalPartImage {
public:
  TieredTemporalPartImage() = delete;
  TieredTemporalPartImage(const TieredTemporalPartImage&) = delete;
  TieredTemporalPartImage& operator=(const TieredTemporalPartImage&) = delete;
  TieredTemporalPartImage(TieredTemporalPartImage&&) noexcept = default;
  TieredTemporalPartImage& operator=(TieredTemporalPartImage&&) noexcept = default;

  [[nodiscard]] TieredPartSource source() const noexcept;
  [[nodiscard]] const manifest::TemporalPartDescriptor& descriptor() const noexcept;
  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] const TieredDatabaseStorageSnapshot& snapshot() const noexcept;

private:
  using Storage = std::variant<manifest::LoadedTemporalPartImage, std::vector<std::byte>>;

  TieredTemporalPartImage(TieredPartSource source, manifest::TemporalPartDescriptor descriptor,
                          Storage storage, TieredDatabaseStorageSnapshot snapshot) noexcept;

  TieredPartSource source_;
  manifest::TemporalPartDescriptor descriptor_;
  Storage storage_;
  TieredDatabaseStorageSnapshot snapshot_;

  friend common::Result<std::vector<TieredTemporalPartImage>> load_tiered_temporal_part_images(
      const TieredDatabaseStorageSnapshot&, const manifest::ManifestStorage&, const ObjectStore&,
      std::span<const cseg::PartId>, std::span<const manifest::TabletSchemaBinding>,
      TieredTemporalPartLoadLimits);
};

// Loads strictly sorted selected identities. A missing local final may use its pinned cold route;
// any other local error fails closed. Remote bytes require exact stat metadata, full-object
// SHA-256, and complete Manifest-v2/CSEG/schema/source validation before return.
[[nodiscard]] common::Result<std::vector<TieredTemporalPartImage>> load_tiered_temporal_part_images(
    const TieredDatabaseStorageSnapshot& snapshot, const manifest::ManifestStorage& local_storage,
    const ObjectStore& remote_store, std::span<const cseg::PartId> part_ids,
    std::span<const manifest::TabletSchemaBinding> schema_bindings,
    TieredTemporalPartLoadLimits limits = {});

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_TIERED_PART_LOADER_HPP_
