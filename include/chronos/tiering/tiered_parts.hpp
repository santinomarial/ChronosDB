#ifndef CHRONOS_TIERING_TIERED_PARTS_HPP_
#define CHRONOS_TIERING_TIERED_PARTS_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/manifest/part_validation.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/tiering/object_store.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace chronos::tiering {

struct ColdPartDescriptor {
  manifest::PartDescriptor part;
  std::string object_key;
  ingest::Sha256Digest checksum;

  friend bool operator==(const ColdPartDescriptor&, const ColdPartDescriptor&) = default;
};

struct TieringLimits {
  std::size_t maximum_parts{1U << 20U};
  std::size_t maximum_object_bytes{std::size_t{4U} * 1024U * 1024U * 1024U};
  std::size_t maximum_cache_bytes{std::size_t{256U} * 1024U * 1024U};
  std::size_t maximum_cache_entries{1024U};
};

struct TieringReceipt {
  ColdPartDescriptor installed;
  bool local_source_may_be_released{};
};

struct TieredPartAdmission {
  wal::WalId wal_id;
  std::reference_wrapper<const schema::TableSchema> schema;
  manifest::ReferencedPartValidationLimits validation_limits;
};

using ColdManifestInstaller = std::function<common::Status(const ColdPartDescriptor& descriptor)>;

// Cold-tier coordinator. Exact schema/source-bound CSEG validation, upload, and remote verification
// happen before the caller's atomic manifest installer. Upload/catalog mutation remains
// single-owner and must not overlap reads. After installation quiesces, read_range, find, and cache
// metrics may be called concurrently; destruction and moves require ordinary external lifetime
// exclusion. The local source becomes releasable only after the installer callback succeeds.
class TieredPartManager {
public:
  TieredPartManager() = delete;
  ~TieredPartManager();
  TieredPartManager(const TieredPartManager&) = delete;
  TieredPartManager& operator=(const TieredPartManager&) = delete;
  TieredPartManager(TieredPartManager&&) noexcept;
  TieredPartManager& operator=(TieredPartManager&&) noexcept;

  [[nodiscard]] static common::Result<TieredPartManager> create(ObjectStore& store,
                                                                TieringLimits limits = {});

  [[nodiscard]] common::Result<TieringReceipt>
  upload_and_install(ColdPartDescriptor descriptor, common::ByteView local_cseg,
                     const TieredPartAdmission& admission,
                     const ColdManifestInstaller& install_manifest);

  // Restores an empty manager from strictly sorted descriptors already selected by durable
  // Manifest/cold-manifest authority. Every remote object is exact-metadata preflighted before the
  // catalog is published. The volatile cache always starts empty and repopulates on verified reads.
  [[nodiscard]] common::Status
  restore_catalog(std::span<const ColdPartDescriptor> authoritative_parts);

  // Large objects use range access. Supplying expected_range_checksum makes the range independently
  // authenticated; otherwise CSEG page validation remains responsible for the returned bytes.
  [[nodiscard]] common::Result<std::vector<std::byte>>
  read_range(const cseg::PartId& part_id, std::size_t offset, std::size_t length,
             const std::optional<ingest::Sha256Digest>& expected_range_checksum = std::nullopt);

  [[nodiscard]] std::optional<ColdPartDescriptor> find(const cseg::PartId& part_id) const;
  [[nodiscard]] std::size_t cached_bytes() const;
  [[nodiscard]] std::size_t cached_entries() const;

private:
  class Impl;
  explicit TieredPartManager(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::tiering

#endif // CHRONOS_TIERING_TIERED_PARTS_HPP_
